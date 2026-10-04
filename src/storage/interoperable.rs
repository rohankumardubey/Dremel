use super::{ColumnarTable, DimensionTable, Table};
use crate::execution::scalar::cmp;
use crate::sql::{Expr, Query};
use crate::types::{Scalar, execution_cancelled};
use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

pub(super) fn load_arrow_ipc(path: &str) -> Result<Table, String> {
    Table::from_columnar(ColumnarTable::read(path)?)
}

pub(super) fn load_parquet(path: &str) -> Result<Table, String> {
    Table::from_columnar(ColumnarTable::read(path)?)
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ParquetScanMetrics {
    pub(crate) total_rows: usize,
    pub(crate) rows_read: usize,
    pub(crate) total_row_groups: usize,
    pub(crate) row_groups_read: usize,
    pub(crate) total_columns: usize,
    pub(crate) columns_read: usize,
    pub(crate) compressed_bytes_read: usize,
    pub(crate) batches_read: usize,
    pub(crate) peak_decoded_batch_bytes: usize,
    pub(crate) streaming_fallback: bool,
}

const EVENT_COLUMNS: [&str; 11] = [
    "event_id",
    "user_id",
    "timestamp",
    "country",
    "device",
    "event_type",
    "duration_ms",
    "bytes",
    "score",
    "success",
    "campaign_id",
];

pub(crate) fn parquet_metadata_table(path: &str) -> Result<Table, String> {
    let file = File::open(path).map_err(|error| format!("cannot open {path}: {error}"))?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| error.to_string())?;
    let mut table = Table::empty();
    table.logical_rows = builder.metadata().file_metadata().num_rows() as usize;
    Ok(table)
}

fn direct_columns(query: &Query) -> BTreeSet<String> {
    let mut columns = BTreeSet::new();
    let has_subquery = query
        .select
        .iter()
        .any(|item| crate::sql::contains_subquery(&item.expr))
        || query
            .filter
            .as_ref()
            .is_some_and(crate::sql::contains_subquery)
        || query
            .having
            .as_ref()
            .is_some_and(crate::sql::contains_subquery)
        || query
            .joins
            .iter()
            .filter_map(|join| join.on.as_ref())
            .any(crate::sql::contains_subquery);
    if query.union.is_some()
        || !query.ctes.is_empty()
        || has_subquery
        || query
            .select
            .iter()
            .any(|item| matches!(&item.expr, Expr::Star))
    {
        columns.extend(EVENT_COLUMNS.iter().map(|name| (*name).to_string()));
        return columns;
    }
    for name in &query.columns {
        let name = name.rsplit('.').next().unwrap_or(name);
        if EVENT_COLUMNS.contains(&name) {
            columns.insert(name.into());
        }
    }
    columns
}

fn literal(expression: &Expr) -> Option<Scalar> {
    match expression {
        Expr::Int(value) => Some(Scalar::Int(*value)),
        Expr::Float(value) => Some(Scalar::Float(*value)),
        Expr::Bool(value) => Some(Scalar::Bool(*value)),
        Expr::String(value) => Some(Scalar::Str(value.clone())),
        _ => None,
    }
}

fn statistics_bounds(statistics: &Statistics) -> Option<(Scalar, Scalar)> {
    match statistics {
        Statistics::Boolean(values) => Some((
            Scalar::Bool(*values.min_opt()?),
            Scalar::Bool(*values.max_opt()?),
        )),
        Statistics::Int64(values) => Some((
            Scalar::Int(*values.min_opt()?),
            Scalar::Int(*values.max_opt()?),
        )),
        Statistics::Double(values) => Some((
            Scalar::Float(*values.min_opt()?),
            Scalar::Float(*values.max_opt()?),
        )),
        Statistics::ByteArray(values) if values.min_is_exact() && values.max_is_exact() => Some((
            Scalar::Str(std::str::from_utf8(values.min_opt()?.data()).ok()?.into()),
            Scalar::Str(std::str::from_utf8(values.max_opt()?.data()).ok()?.into()),
        )),
        _ => None,
    }
}

fn comparison_may_match(operation: &str, value: &Scalar, min: &Scalar, max: &Scalar) -> bool {
    let min_vs_value = cmp(min, value);
    let max_vs_value = cmp(max, value);
    match operation {
        "=" => {
            matches!(min_vs_value, Some(Ordering::Less | Ordering::Equal))
                && matches!(max_vs_value, Some(Ordering::Greater | Ordering::Equal))
        }
        "!=" => !(min_vs_value == Some(Ordering::Equal) && max_vs_value == Some(Ordering::Equal)),
        "<" => min_vs_value == Some(Ordering::Less),
        "<=" => matches!(min_vs_value, Some(Ordering::Less | Ordering::Equal)),
        ">" => max_vs_value == Some(Ordering::Greater),
        ">=" => matches!(max_vs_value, Some(Ordering::Greater | Ordering::Equal)),
        _ => true,
    }
}

fn column_index(column: &str, indexes: &HashMap<String, usize>) -> Option<usize> {
    indexes
        .get(column.rsplit('.').next().unwrap_or(column))
        .copied()
}

fn row_group_may_match(
    expression: &Expr,
    row_group: &RowGroupMetaData,
    indexes: &HashMap<String, usize>,
) -> bool {
    match expression {
        Expr::Bool(value) => *value,
        Expr::Null => false,
        Expr::Binary(operation, left, right) if operation == "and" => {
            row_group_may_match(left, row_group, indexes)
                && row_group_may_match(right, row_group, indexes)
        }
        Expr::Binary(operation, left, right) if operation == "or" => {
            row_group_may_match(left, row_group, indexes)
                || row_group_may_match(right, row_group, indexes)
        }
        Expr::Binary(operation, left, right) => {
            let (column, value, operation) =
                match (left.as_ref(), literal(right), literal(left), right.as_ref()) {
                    (Expr::Column(column), Some(value), _, _) => {
                        (column.as_str(), value, operation.as_str())
                    }
                    (_, _, Some(value), Expr::Column(column)) => {
                        let flipped = match operation.as_str() {
                            "<" => ">",
                            "<=" => ">=",
                            ">" => "<",
                            ">=" => "<=",
                            value => value,
                        };
                        (column.as_str(), value, flipped)
                    }
                    _ => return true,
                };
            let Some(index) = column_index(column, indexes) else {
                return true;
            };
            let Some(statistics) = row_group.column(index).statistics() else {
                return true;
            };
            let Some((min, max)) = statistics_bounds(statistics) else {
                return true;
            };
            comparison_may_match(operation, &value, &min, &max)
        }
        Expr::Between(value, low, high, negated) if !negated => {
            let (Expr::Column(column), Some(low), Some(high)) =
                (value.as_ref(), literal(low), literal(high))
            else {
                return true;
            };
            let Some(index) = column_index(column, indexes) else {
                return true;
            };
            let Some(statistics) = row_group.column(index).statistics() else {
                return true;
            };
            let Some((min, max)) = statistics_bounds(statistics) else {
                return true;
            };
            comparison_may_match(">=", &low, &min, &max)
                && comparison_may_match("<=", &high, &min, &max)
        }
        Expr::InList(value, candidates, negated) if !negated => {
            let Expr::Column(column) = value.as_ref() else {
                return true;
            };
            let Some(index) = column_index(column, indexes) else {
                return true;
            };
            let Some(statistics) = row_group.column(index).statistics() else {
                return true;
            };
            let Some((min, max)) = statistics_bounds(statistics) else {
                return true;
            };
            candidates.iter().any(|candidate| {
                literal(candidate).is_none_or(|value| comparison_may_match("=", &value, &min, &max))
            })
        }
        Expr::IsNull(value, negated) => {
            let Expr::Column(column) = value.as_ref() else {
                return true;
            };
            let Some(index) = column_index(column, indexes) else {
                return true;
            };
            let Some(statistics) = row_group.column(index).statistics() else {
                return true;
            };
            let Some(nulls) = statistics.null_count_opt() else {
                return true;
            };
            if *negated {
                nulls < row_group.num_rows() as u64
            } else {
                nulls > 0
            }
        }
        _ => true,
    }
}

fn parquet_scan_selection(
    path: &str,
    query: &Query,
) -> Result<(Vec<usize>, Vec<usize>, ParquetScanMetrics), String> {
    let file = File::open(path).map_err(|error| format!("cannot open {path}: {error}"))?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| error.to_string())?;
    let metadata = builder.metadata();
    let indexes: HashMap<_, _> = builder
        .parquet_schema()
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| (column.name().to_string(), index))
        .collect();
    let columns: Vec<_> = direct_columns(query)
        .into_iter()
        .filter_map(|name| indexes.get(&name).copied())
        .collect();
    let row_groups: Vec<_> = metadata
        .row_groups()
        .iter()
        .enumerate()
        .filter(|(_, row_group)| {
            query
                .filter
                .as_ref()
                .is_none_or(|filter| row_group_may_match(filter, row_group, &indexes))
        })
        .map(|(index, _)| index)
        .collect();
    let rows_read = row_groups
        .iter()
        .map(|&index| metadata.row_group(index).num_rows() as usize)
        .sum();
    let compressed_bytes_read = row_groups
        .iter()
        .flat_map(|&row_group| {
            columns.iter().map(move |&column| {
                metadata
                    .row_group(row_group)
                    .column(column)
                    .compressed_size()
            })
        })
        .map(|bytes| bytes.max(0) as usize)
        .sum();
    let metrics = ParquetScanMetrics {
        total_rows: metadata.file_metadata().num_rows() as usize,
        rows_read,
        total_row_groups: metadata.num_row_groups(),
        row_groups_read: row_groups.len(),
        total_columns: metadata.file_metadata().schema_descr().num_columns(),
        columns_read: columns.len(),
        compressed_bytes_read,
        ..ParquetScanMetrics::default()
    };
    Ok((columns, row_groups, metrics))
}

pub(crate) fn parquet_scan_plan(path: &str, query: &Query) -> Result<ParquetScanMetrics, String> {
    parquet_scan_selection(path, query).map(|(_, _, metrics)| metrics)
}

pub(crate) fn load_parquet_direct(
    path: &str,
    query: &Query,
    batch_size: usize,
) -> Result<(Table, ParquetScanMetrics), String> {
    let (columns, row_groups, mut metrics) = parquet_scan_selection(path, query)?;
    let file = File::open(path).map_err(|error| format!("cannot open {path}: {error}"))?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| error.to_string())?;
    let projection = ProjectionMask::leaves(builder.parquet_schema(), columns);
    let reader = builder
        .with_batch_size(batch_size.max(1))
        .with_projection(projection)
        .with_row_groups(row_groups)
        .build()
        .map_err(|error| error.to_string())?;
    let mut table = Table::empty();
    for batch in reader {
        if execution_cancelled() {
            return Err("query cancelled during Parquet scan".into());
        }
        append_projected_batch(&mut table, &batch.map_err(|error| error.to_string())?)?;
        metrics.batches_read += 1;
        metrics.peak_decoded_batch_bytes = table.approximate_bytes();
    }
    Ok((table, metrics))
}

pub(crate) fn stream_parquet_direct<F>(
    path: &str,
    query: &Query,
    batch_size: usize,
    mut consume: F,
) -> Result<(Table, ParquetScanMetrics), String>
where
    F: FnMut(Table) -> Result<(), String>,
{
    let (columns, row_groups, mut metrics) = parquet_scan_selection(path, query)?;
    let file = File::open(path).map_err(|error| format!("cannot open {path}: {error}"))?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| error.to_string())?;
    let projection = ProjectionMask::leaves(builder.parquet_schema(), columns);
    let reader = builder
        .with_batch_size(batch_size.max(1))
        .with_projection(projection)
        .with_row_groups(row_groups)
        .build()
        .map_err(|error| error.to_string())?;
    let mut country = super::Dictionary::default();
    let mut device = super::Dictionary::default();
    let mut event = super::Dictionary::default();
    for batch in reader {
        if execution_cancelled() {
            return Err("query cancelled during Parquet scan".into());
        }
        let mut table = Table::empty();
        table.country_dict = country;
        table.device_dict = device;
        table.event_dict = event;
        append_projected_batch(&mut table, &batch.map_err(|error| error.to_string())?)?;
        country = table.country_dict.clone();
        device = table.device_dict.clone();
        event = table.event_dict.clone();
        metrics.batches_read += 1;
        metrics.peak_decoded_batch_bytes = metrics
            .peak_decoded_batch_bytes
            .max(table.approximate_bytes());
        consume(table)?;
    }
    let mut dictionaries = Table::empty();
    dictionaries.logical_rows = metrics.rows_read;
    dictionaries.country_dict = country;
    dictionaries.device_dict = device;
    dictionaries.event_dict = event;
    Ok((dictionaries, metrics))
}

fn optional_cast(
    batch: &RecordBatch,
    name: &str,
    data_type: &DataType,
) -> Result<Option<ArrayRef>, String> {
    batch
        .column_by_name(name)
        .map(|array| {
            cast(array, data_type).map_err(|error| format!("invalid {name} column: {error}"))
        })
        .transpose()
}

fn append_projected_batch(table: &mut Table, batch: &RecordBatch) -> Result<(), String> {
    table.logical_rows += batch.num_rows();
    let event_id = optional_cast(batch, "event_id", &DataType::Int64)?;
    let user_id = optional_cast(batch, "user_id", &DataType::Int64)?;
    let timestamp = optional_cast(batch, "timestamp", &DataType::Int64)?;
    let duration = optional_cast(batch, "duration_ms", &DataType::Int64)?;
    let bytes = optional_cast(batch, "bytes", &DataType::Int64)?;
    let score = optional_cast(batch, "score", &DataType::Float64)?;
    let success = optional_cast(batch, "success", &DataType::Boolean)?;
    let campaign = optional_cast(batch, "campaign_id", &DataType::Int64)?;
    let country = optional_cast(batch, "country", &DataType::Utf8)?;
    let device = optional_cast(batch, "device", &DataType::Utf8)?;
    let event_type = optional_cast(batch, "event_type", &DataType::Utf8)?;

    let event_id = event_id
        .as_ref()
        .map(|array| downcast::<Int64Array>(array, "event_id"))
        .transpose()?;
    let user_id = user_id
        .as_ref()
        .map(|array| downcast::<Int64Array>(array, "user_id"))
        .transpose()?;
    let timestamp = timestamp
        .as_ref()
        .map(|array| downcast::<Int64Array>(array, "timestamp"))
        .transpose()?;
    let duration = duration
        .as_ref()
        .map(|array| downcast::<Int64Array>(array, "duration_ms"))
        .transpose()?;
    let bytes = bytes
        .as_ref()
        .map(|array| downcast::<Int64Array>(array, "bytes"))
        .transpose()?;
    let score = score
        .as_ref()
        .map(|array| downcast::<Float64Array>(array, "score"))
        .transpose()?;
    let success = success
        .as_ref()
        .map(|array| downcast::<BooleanArray>(array, "success"))
        .transpose()?;
    let campaign = campaign
        .as_ref()
        .map(|array| downcast::<Int64Array>(array, "campaign_id"))
        .transpose()?;
    let country = country
        .as_ref()
        .map(|array| downcast::<StringArray>(array, "country"))
        .transpose()?;
    let device = device
        .as_ref()
        .map(|array| downcast::<StringArray>(array, "device"))
        .transpose()?;
    let event_type = event_type
        .as_ref()
        .map(|array| downcast::<StringArray>(array, "event_type"))
        .transpose()?;

    for row in 0..batch.num_rows() {
        if let Some(array) = event_id {
            if array.is_null(row) {
                return Err(format!("required column event_id is null at row {row}"));
            }
            table.event_id.push(array.value(row));
        }
        if let Some(array) = user_id {
            if array.is_null(row) {
                return Err(format!("required column user_id is null at row {row}"));
            }
            table.user_id.push(array.value(row));
        }
        if let Some(array) = timestamp {
            if array.is_null(row) {
                return Err(format!("required column timestamp is null at row {row}"));
            }
            table.timestamp.push(array.value(row));
        }
        if let Some(array) = country {
            if array.is_null(row) {
                return Err(format!("required column country is null at row {row}"));
            }
            table
                .country
                .push(table.country_dict.insert(array.value(row)));
        }
        if let Some(array) = device {
            if array.is_null(row) {
                return Err(format!("required column device is null at row {row}"));
            }
            table
                .device
                .push(table.device_dict.insert(array.value(row)));
        }
        if let Some(array) = event_type {
            if array.is_null(row) {
                return Err(format!("required column event_type is null at row {row}"));
            }
            table
                .event_type
                .push(table.event_dict.insert(array.value(row)));
        }
        if let Some(array) = duration {
            if array.is_null(row) {
                return Err(format!("required column duration_ms is null at row {row}"));
            }
            table.duration.push(array.value(row));
        }
        if let Some(array) = bytes {
            if array.is_null(row) {
                return Err(format!("required column bytes is null at row {row}"));
            }
            table.bytes.push(array.value(row));
        }
        if let Some(array) = score {
            if array.is_null(row) {
                return Err(format!("required column score is null at row {row}"));
            }
            table.score.push(array.value(row));
        }
        if let Some(array) = success {
            if array.is_null(row) {
                return Err(format!("required column success is null at row {row}"));
            }
            table.success.push(u8::from(array.value(row)));
        }
        if let Some(array) = campaign {
            if array.is_null(row) {
                table.campaign.push(0);
                table.campaign_def.push(0);
            } else {
                table.campaign.push(array.value(row));
                table.campaign_def.push(1);
            }
        }
    }
    Ok(())
}

fn downcast<'a, T: 'static>(array: &'a Arc<dyn Array>, name: &str) -> Result<&'a T, String> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("unsupported {name} array type {}", array.data_type()))
}

pub(super) fn load_users(path: &Path) -> Result<DimensionTable, String> {
    DimensionTable::users(ColumnarTable::read(path)?)
}

pub(super) fn load_campaigns(path: &Path) -> Result<DimensionTable, String> {
    DimensionTable::campaigns(ColumnarTable::read(path)?)
}

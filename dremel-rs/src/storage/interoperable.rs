use super::{CampaignsTable, Table, UsersTable};
use crate::execution::scalar::cmp;
use crate::sql::{Expr, Query};
use crate::types::Scalar;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int64Array, StringArray,
};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::ipc::reader::FileReader;
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
    load_batches(read_batches(Path::new(path))?)
}

pub(super) fn load_parquet(path: &str) -> Result<Table, String> {
    load_batches(read_batches(Path::new(path))?)
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
    let (columns, row_groups, metrics) = parquet_scan_selection(path, query)?;
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
        append_projected_batch(&mut table, &batch.map_err(|error| error.to_string())?)?;
    }
    Ok((table, metrics))
}

fn read_batches(path: &Path) -> Result<Vec<RecordBatch>, String> {
    let file =
        File::open(path).map_err(|error| format!("cannot open {}: {error}", path.display()))?;
    if path.extension().and_then(|value| value.to_str()) == Some("arrow") {
        FileReader::try_new(file, None)
            .map_err(|error| error.to_string())?
            .map(|batch| batch.map_err(|error| error.to_string()))
            .collect()
    } else {
        ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| error.to_string())?
            .with_batch_size(65_536)
            .build()
            .map_err(|error| error.to_string())?
            .map(|batch| batch.map_err(|error| error.to_string()))
            .collect()
    }
}

fn load_batches(batches: Vec<RecordBatch>) -> Result<Table, String> {
    let mut table = Table::empty();
    for batch in batches {
        append_batch(&mut table, &batch)?;
    }
    Ok(table)
}

fn column(batch: &RecordBatch, name: &str) -> Result<ArrayRef, String> {
    batch
        .column_by_name(name)
        .cloned()
        .ok_or_else(|| format!("missing required column {name}"))
}

fn as_int64(batch: &RecordBatch, name: &str) -> Result<ArrayRef, String> {
    cast(&column(batch, name)?, &DataType::Int64)
        .map_err(|error| format!("invalid {name} column: {error}"))
}

fn as_float64(batch: &RecordBatch, name: &str) -> Result<ArrayRef, String> {
    cast(&column(batch, name)?, &DataType::Float64)
        .map_err(|error| format!("invalid {name} column: {error}"))
}

fn as_boolean(batch: &RecordBatch, name: &str) -> Result<ArrayRef, String> {
    cast(&column(batch, name)?, &DataType::Boolean)
        .map_err(|error| format!("invalid {name} column: {error}"))
}

fn as_utf8(batch: &RecordBatch, name: &str) -> Result<ArrayRef, String> {
    cast(&column(batch, name)?, &DataType::Utf8)
        .map_err(|error| format!("invalid {name} column: {error}"))
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

fn append_batch(table: &mut Table, batch: &RecordBatch) -> Result<(), String> {
    let event_id = as_int64(batch, "event_id")?;
    let user_id = as_int64(batch, "user_id")?;
    let timestamp = as_int64(batch, "timestamp")?;
    let duration = as_int64(batch, "duration_ms")?;
    let bytes = as_int64(batch, "bytes")?;
    let score = as_float64(batch, "score")?;
    let success = as_boolean(batch, "success")?;
    let campaign = as_int64(batch, "campaign_id")?;
    let country = as_utf8(batch, "country")?;
    let device = as_utf8(batch, "device")?;
    let event_type = as_utf8(batch, "event_type")?;

    let event_id = downcast::<Int64Array>(&event_id, "event_id")?;
    let user_id = downcast::<Int64Array>(&user_id, "user_id")?;
    let timestamp = downcast::<Int64Array>(&timestamp, "timestamp")?;
    let duration = downcast::<Int64Array>(&duration, "duration_ms")?;
    let bytes = downcast::<Int64Array>(&bytes, "bytes")?;
    let score = downcast::<Float64Array>(&score, "score")?;
    let success = downcast::<BooleanArray>(&success, "success")?;
    let campaign = downcast::<Int64Array>(&campaign, "campaign_id")?;
    let country = downcast::<StringArray>(&country, "country")?;
    let device = downcast::<StringArray>(&device, "device")?;
    let event_type = downcast::<StringArray>(&event_type, "event_type")?;

    for row in 0..batch.num_rows() {
        for (name, array) in [
            ("event_id", event_id as &dyn Array),
            ("user_id", user_id),
            ("timestamp", timestamp),
            ("country", country),
            ("device", device),
            ("event_type", event_type),
            ("duration_ms", duration),
            ("bytes", bytes),
            ("score", score),
            ("success", success),
        ] {
            if array.is_null(row) {
                return Err(format!("required column {name} is null at row {row}"));
            }
        }
        table.event_id.push(event_id.value(row));
        table.user_id.push(user_id.value(row));
        table.timestamp.push(timestamp.value(row));
        table
            .country
            .push(table.country_dict.insert(country.value(row)));
        table
            .device
            .push(table.device_dict.insert(device.value(row)));
        table
            .event_type
            .push(table.event_dict.insert(event_type.value(row)));
        table.duration.push(duration.value(row));
        table.bytes.push(bytes.value(row));
        table.score.push(score.value(row));
        table.success.push(u8::from(success.value(row)));
        if campaign.is_null(row) {
            table.campaign.push(0);
            table.campaign_def.push(0);
        } else {
            table.campaign.push(campaign.value(row));
            table.campaign_def.push(1);
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

pub(super) fn load_users(path: &Path) -> Result<UsersTable, String> {
    let mut users = UsersTable::default();
    for batch in read_batches(path)? {
        let user_id = as_int64(&batch, "user_id")?;
        let segment = as_utf8(&batch, "segment")?;
        let signup_date = as_utf8(&batch, "signup_date")?;
        let region = as_utf8(&batch, "region")?;
        let active = as_boolean(&batch, "active")?;
        let lifetime = column(&batch, "lifetime_value")?;
        let user_id = downcast::<Int64Array>(&user_id, "user_id")?;
        let segment = downcast::<StringArray>(&segment, "segment")?;
        let signup_date = downcast::<StringArray>(&signup_date, "signup_date")?;
        let region = downcast::<StringArray>(&region, "region")?;
        let active = downcast::<BooleanArray>(&active, "active")?;
        let lifetime = downcast::<Decimal128Array>(&lifetime, "lifetime_value")?;
        for row_in_batch in 0..batch.num_rows() {
            for (name, array) in [
                ("user_id", user_id as &dyn Array),
                ("segment", segment),
                ("signup_date", signup_date),
                ("lifetime_value", lifetime),
                ("region", region),
                ("active", active),
            ] {
                if array.is_null(row_in_batch) {
                    return Err(format!("required column {name} contains null"));
                }
            }
            let row = users.user_id.len();
            let id = user_id.value(row_in_batch);
            users.user_id.push(id);
            users.segment.push(segment.value(row_in_batch).into());
            users
                .signup_date
                .push(signup_date.value(row_in_batch).into());
            users.lifetime_value.push(
                i64::try_from(lifetime.value(row_in_batch))
                    .map_err(|_| "lifetime_value exceeds engine decimal range")?,
            );
            users.region.push(region.value(row_in_batch).into());
            users.active.push(active.value(row_in_batch));
            users.index.entry(id).or_default().push(row);
        }
    }
    Ok(users)
}

pub(super) fn load_campaigns(path: &Path) -> Result<CampaignsTable, String> {
    let mut campaigns = CampaignsTable::default();
    for batch in read_batches(path)? {
        let campaign_id = as_int64(&batch, "campaign_id")?;
        let campaign_name = as_utf8(&batch, "campaign_name")?;
        let budget = column(&batch, "budget")?;
        let start_date = as_utf8(&batch, "start_date")?;
        let end_date = as_utf8(&batch, "end_date")?;
        let channel = as_utf8(&batch, "channel")?;
        let campaign_id = downcast::<Int64Array>(&campaign_id, "campaign_id")?;
        let campaign_name = downcast::<StringArray>(&campaign_name, "campaign_name")?;
        let budget = downcast::<Decimal128Array>(&budget, "budget")?;
        let start_date = downcast::<StringArray>(&start_date, "start_date")?;
        let end_date = downcast::<StringArray>(&end_date, "end_date")?;
        let channel = downcast::<StringArray>(&channel, "channel")?;
        for row_in_batch in 0..batch.num_rows() {
            let row = campaigns.campaign_id.len();
            let id = campaign_id.value(row_in_batch);
            campaigns.campaign_id.push(id);
            campaigns
                .campaign_name
                .push(campaign_name.value(row_in_batch).into());
            campaigns.budget.push(
                i64::try_from(budget.value(row_in_batch))
                    .map_err(|_| "budget exceeds engine decimal range")?,
            );
            campaigns
                .start_date
                .push(start_date.value(row_in_batch).into());
            campaigns.end_date.push(end_date.value(row_in_batch).into());
            campaigns.channel.push(channel.value(row_in_batch).into());
            campaigns.index.entry(id).or_default().push(row);
        }
    }
    Ok(campaigns)
}

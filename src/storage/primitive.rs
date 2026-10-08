use super::{ColumnarTable, SqlType, arrow_sql_type};
use crate::execution::scalar::format_date;
use crate::types::{Scalar, account_query_memory, current_query_memory};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float64Array,
    Int64Array, StringArray, TimestampSecondArray,
};
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{DataType, TimeUnit};
use std::collections::HashMap;

pub(crate) struct PrimitiveBatch {
    pub(crate) rows: usize,
    columns: HashMap<String, (SqlType, ArrayRef)>,
}

impl PrimitiveBatch {
    pub(crate) fn from_table(table: &ColumnarTable, names: &[String]) -> Result<Vec<Self>, String> {
        let indexes: HashMap<_, _> = table
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| (field.name().to_ascii_lowercase(), index))
            .collect();
        table.batches().iter().map(|batch| {
            let mut columns = HashMap::new();
            for name in names {
                let name = name.rsplit('.').next().unwrap_or(name);
                if columns.contains_key(name) { continue; }
                let mut array = batch.column(indexes[name]).clone();
                let mut dictionary_bytes = 0;
                if let DataType::Dictionary(_, value_type) = array.data_type() {
                    array = cast_with_options(&array, value_type, &CastOptions { safe: false, ..Default::default() })
                        .map_err(|error| format!("invalid {name} dictionary: {error}"))?;
                    dictionary_bytes = array.get_array_memory_size();
                    account_query_memory(dictionary_bytes, "dictionary decoding")?;
                }
                let kind = arrow_sql_type(array.data_type());
                let target = match kind {
                    SqlType::Null => DataType::Null,
                    SqlType::Bool => DataType::Boolean,
                    SqlType::Int => DataType::Int64,
                    SqlType::Double => DataType::Float64,
                    SqlType::String => DataType::Utf8,
                    SqlType::Decimal => DataType::Decimal128(18, 2),
                    SqlType::Date if matches!(array.data_type(), DataType::Date64) => DataType::Date64,
                    SqlType::Date => DataType::Date32,
                    SqlType::Timestamp => DataType::Timestamp(TimeUnit::Second, None),
                    _ => return Err(format!("unsupported SQL type for column {name}: {}", array.data_type())),
                };
                if let DataType::Timestamp(unit, _) = array.data_type() {
                    let divisor = match unit {
                        TimeUnit::Second => 1,
                        TimeUnit::Millisecond => 1_000,
                        TimeUnit::Microsecond => 1_000_000,
                        TimeUnit::Nanosecond => 1_000_000_000,
                    };
                    let values = cast_with_options(&array, &DataType::Int64, &CastOptions { safe: false, ..Default::default() })
                        .map_err(|error| error.to_string())?;
                    let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
                    if values.iter().flatten().any(|value| value % divisor != 0) {
                        return Err(format!("column {name} contains subsecond timestamps; SQL timestamps currently require whole seconds"));
                    }
                }
                let normalized = if array.data_type() == &target { array.clone() } else {
                    let normalized = cast_with_options(&array, &target, &CastOptions { safe: false, ..Default::default() })
                        .map_err(|error| format!("invalid {name} column: {error}"))?;
                    account_query_memory(normalized.get_array_memory_size(), "column normalization")?;
                    if let Some(memory) = current_query_memory() { memory.release(dictionary_bytes); }
                    normalized
                };
                if kind == SqlType::Decimal {
                    let values = normalized.as_any().downcast_ref::<Decimal128Array>().unwrap();
                    if values.iter().flatten().any(|value| i64::try_from(value).is_err() || value.unsigned_abs() >= 1_000_000_000_000_000_000) {
                        return Err(format!("column {name} exceeds engine decimal range"));
                    }
                }
                if let Some(values) = normalized.as_any().downcast_ref::<Date64Array>()
                    && values.iter().flatten().any(|value| value % 86_400_000 != 0) {
                    return Err(format!("column {name} contains a Date64 value that is not a whole day"));
                }
                columns.insert(name.into(), (kind, normalized));
            }
            Ok(Self { rows: batch.num_rows(), columns })
        }).collect()
    }

    pub(crate) fn scalar(&self, column: &str, row: usize) -> Scalar {
        let column = column.rsplit('.').next().unwrap_or(column);
        let (kind, array) = &self.columns[column];
        if array.is_null(row) {
            return Scalar::Null;
        }
        match kind {
            SqlType::Null => Scalar::Null,
            SqlType::Bool => Scalar::Bool(
                array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(row),
            ),
            SqlType::Int => Scalar::Int(
                array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row),
            ),
            SqlType::Double => Scalar::Float(
                array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row),
            ),
            SqlType::String => Scalar::Str(
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row)
                    .into(),
            ),
            SqlType::Decimal => Scalar::Decimal(
                array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .value(row) as i64,
            ),
            SqlType::Date => {
                let days = if let Some(array) = array.as_any().downcast_ref::<Date32Array>() {
                    i64::from(array.value(row))
                } else {
                    array
                        .as_any()
                        .downcast_ref::<Date64Array>()
                        .unwrap()
                        .value(row)
                        / 86_400_000
                };
                Scalar::Str(format_date(days))
            }
            SqlType::Timestamp => Scalar::Int(
                array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap()
                    .value(row),
            ),
            _ => unreachable!("validated primitive column"),
        }
    }
}

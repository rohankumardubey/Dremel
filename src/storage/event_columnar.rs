use super::ColumnarTable;
use crate::types::Scalar;
use arrow::array::{
    Array, ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int64Array, StringArray,
    UInt32Array,
};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::{DataType, UInt32Type};

pub(super) const COLUMNS: [(&str, DataType); 11] = [
    ("event_id", DataType::Int64),
    ("user_id", DataType::Int64),
    ("timestamp", DataType::Int64),
    ("country", DataType::Utf8),
    ("device", DataType::Utf8),
    ("event_type", DataType::Utf8),
    ("duration_ms", DataType::Int64),
    ("bytes", DataType::Int64),
    ("score", DataType::Float64),
    ("success", DataType::Boolean),
    ("campaign_id", DataType::Int64),
];

/// Typed access to the current SQL columns without discarding the source schema.
/// Multiple eager batches are coalesced once for constant-time row lookup.
pub(crate) struct EventColumnar {
    data: ColumnarTable,
    cast_bytes: usize,
    event_id: Option<Int64Array>,
    user_id: Option<Int64Array>,
    timestamp: Option<Int64Array>,
    country: Option<TextColumn>,
    device: Option<TextColumn>,
    event_type: Option<TextColumn>,
    duration: Option<Int64Array>,
    bytes: Option<Int64Array>,
    score: Option<Float64Array>,
    success: Option<BooleanArray>,
    campaign: Option<Int64Array>,
}

enum TextColumn {
    Plain(StringArray),
    Dictionary {
        keys: UInt32Array,
        values: StringArray,
    },
}

impl TextColumn {
    fn new(array: &ArrayRef) -> Self {
        if let Some(dictionary) = array.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
            Self::Dictionary {
                keys: dictionary.keys().clone(),
                values: typed(dictionary.values()),
            }
        } else {
            Self::Plain(typed(array))
        }
    }

    fn value(&self, row: usize) -> &str {
        match self {
            Self::Plain(values) => values.value(row),
            Self::Dictionary { keys, values } => values.value(keys.value(row) as usize),
        }
    }

    fn key(&self, row: usize) -> Option<u32> {
        match self {
            Self::Plain(_) => None,
            Self::Dictionary { keys, .. } => Some(keys.value(row)),
        }
    }
}

impl EventColumnar {
    pub(crate) fn new(data: ColumnarTable) -> Result<Self, String> {
        Self::build(data, true)
    }

    pub(crate) fn projected(data: ColumnarTable) -> Result<Self, String> {
        Self::build(data, false)
    }

    fn build(data: ColumnarTable, require_all: bool) -> Result<Self, String> {
        let schema = data.schema().clone();
        let batch = if let [batch] = data.batches() {
            batch.clone()
        } else {
            concat_batches(&schema, data.batches()).map_err(|error| error.to_string())?
        };
        let mut columns = Vec::with_capacity(COLUMNS.len());
        let mut cast_bytes = 0usize;
        for (name, data_type) in &COLUMNS {
            let Some(array) = batch.column_by_name(name) else {
                if require_all {
                    return Err(format!("missing required column {name}"));
                }
                columns.push(None);
                continue;
            };
            let dictionary_text = *data_type == DataType::Utf8
                && matches!(
                    array.data_type(),
                    DataType::Dictionary(key, value)
                        if **key == DataType::UInt32 && **value == DataType::Utf8
                );
            let needs_cast = array.data_type() != data_type && !dictionary_text;
            let array = if needs_cast {
                cast(array, data_type).map_err(|error| format!("invalid {name} column: {error}"))?
            } else {
                array.clone()
            };
            if needs_cast {
                cast_bytes = cast_bytes.saturating_add(array.get_array_memory_size());
            }
            if *name != "campaign_id" && array.null_count() != 0 {
                return Err(format!("required column {name} contains null"));
            }
            columns.push(Some(array));
        }
        let data = ColumnarTable::try_new(schema, vec![batch])?;
        Ok(Self {
            data,
            cast_bytes,
            event_id: columns[0].as_ref().map(typed),
            user_id: columns[1].as_ref().map(typed),
            timestamp: columns[2].as_ref().map(typed),
            country: columns[3].as_ref().map(TextColumn::new),
            device: columns[4].as_ref().map(TextColumn::new),
            event_type: columns[5].as_ref().map(TextColumn::new),
            duration: columns[6].as_ref().map(typed),
            bytes: columns[7].as_ref().map(typed),
            score: columns[8].as_ref().map(typed),
            success: columns[9].as_ref().map(typed),
            campaign: columns[10].as_ref().map(typed),
        })
    }

    pub(crate) fn row_count(&self) -> usize {
        self.data.row_count()
    }

    pub(crate) fn approximate_bytes(&self) -> usize {
        self.cast_bytes
            + self
                .data
                .batches()
                .iter()
                .map(|batch| batch.get_array_memory_size())
                .sum::<usize>()
    }

    pub(crate) fn campaign_null_count(&self) -> usize {
        self.campaign.as_ref().map_or(0, Array::null_count)
    }

    pub(crate) fn string(&self, column: &str, row: usize) -> Option<&str> {
        match column {
            "country" => self.country.as_ref().map(|values| values.value(row)),
            "device" => self.device.as_ref().map(|values| values.value(row)),
            "event_type" => self.event_type.as_ref().map(|values| values.value(row)),
            _ => panic!("unknown event string column {column}"),
        }
    }

    pub(crate) fn dictionary_key(&self, column: &str, row: usize) -> Option<u32> {
        match column {
            "country" => self.country.as_ref().and_then(|values| values.key(row)),
            "device" => self.device.as_ref().and_then(|values| values.key(row)),
            "event_type" => self.event_type.as_ref().and_then(|values| values.key(row)),
            _ => None,
        }
    }

    pub(crate) fn scalar(&self, column: &str, row: usize) -> Scalar {
        match column {
            "event_id" => self
                .event_id
                .as_ref()
                .map_or(Scalar::Null, |v| Scalar::Int(v.value(row))),
            "user_id" => self
                .user_id
                .as_ref()
                .map_or(Scalar::Null, |v| Scalar::Int(v.value(row))),
            "timestamp" => self
                .timestamp
                .as_ref()
                .map_or(Scalar::Null, |v| Scalar::Int(v.value(row))),
            "country" => self
                .country
                .as_ref()
                .map_or(Scalar::Null, |v| Scalar::Str(v.value(row).into())),
            "device" => self
                .device
                .as_ref()
                .map_or(Scalar::Null, |v| Scalar::Str(v.value(row).into())),
            "event_type" => self
                .event_type
                .as_ref()
                .map_or(Scalar::Null, |v| Scalar::Str(v.value(row).into())),
            "duration_ms" => self
                .duration
                .as_ref()
                .map_or(Scalar::Null, |v| Scalar::Int(v.value(row))),
            "bytes" => self
                .bytes
                .as_ref()
                .map_or(Scalar::Null, |v| Scalar::Int(v.value(row))),
            "score" => self
                .score
                .as_ref()
                .map_or(Scalar::Null, |v| Scalar::Float(v.value(row))),
            "success" => self
                .success
                .as_ref()
                .map_or(Scalar::Null, |v| Scalar::Bool(v.value(row))),
            "campaign_id" if self.campaign.as_ref().is_some_and(|v| v.is_null(row)) => Scalar::Null,
            "campaign_id" => self
                .campaign
                .as_ref()
                .map_or(Scalar::Null, |v| Scalar::Int(v.value(row))),
            _ => Scalar::Null,
        }
    }
}

fn typed<T: Array + Clone + 'static>(array: &ArrayRef) -> T {
    array.as_any().downcast_ref::<T>().unwrap().clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Table;
    use arrow::datatypes::{Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::fs::File;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

    fn fixture() -> (Arc<Schema>, RecordBatch) {
        let mut fields: Vec<_> = COLUMNS
            .iter()
            .map(|(name, data_type)| Field::new(*name, data_type.clone(), *name == "campaign_id"))
            .collect();
        fields.push(Field::new("extra", DataType::Utf8, true));
        let schema = Arc::new(Schema::new(fields));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
            Arc::new(StringArray::from(vec!["US", "IN", "US"])),
            Arc::new(StringArray::from(vec!["mobile", "desktop", "mobile"])),
            Arc::new(StringArray::from(vec!["click", "view", "click"])),
            Arc::new(Int64Array::from(vec![4, 5, 6])),
            Arc::new(Int64Array::from(vec![40, 50, 60])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
            Arc::new(Int64Array::from(vec![Some(7), None, Some(8)])),
            Arc::new(StringArray::from(vec![
                Some("kept"),
                None,
                Some("also kept"),
            ])),
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        (schema, batch)
    }

    #[test]
    fn reads_typed_rows_from_multiple_batches() {
        let (schema, batch) = fixture();
        let data =
            ColumnarTable::try_new(schema, vec![batch.slice(0, 1), batch.slice(1, 2)]).unwrap();
        let table = Table::from_columnar(data).unwrap();
        assert_eq!(table.len(), 3);
        assert!(table.event_id.is_empty());
        assert!(table.score.is_empty());
        assert_eq!(table.scalar("event_id", 2), Scalar::Int(3));
        assert_eq!(table.scalar("country", 2), Scalar::Str("US".into()));
        assert_eq!(table.scalar("success", 1), Scalar::Bool(false));
        assert_eq!(table.scalar("campaign_id", 1), Scalar::Null);
        assert_eq!(table.campaign_null_count(), 1);
        assert_eq!(table.event_id_bounds(), (1, 3));
        assert_eq!(table.raw_key("country", 0), table.raw_key("country", 2));
        assert_ne!(table.raw_key("country", 0), table.raw_key("country", 1));
        assert_eq!(table.raw_key("success", 0), 1);
        assert_eq!(table.raw_key("success", 1), 0);
        assert_eq!(
            table
                .columnar
                .as_ref()
                .unwrap()
                .data
                .field("extra")
                .unwrap()
                .data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn loads_arrow_and_parquet_without_materializing_numeric_vectors() {
        let (schema, batch) = fixture();
        for extension in ["arrow", "parquet"] {
            let path = std::env::temp_dir().join(format!(
                "dremel-events-{}-{}.{}",
                std::process::id(),
                NEXT_FILE.fetch_add(1, Ordering::Relaxed),
                extension
            ));
            let file = File::create(&path).unwrap();
            if extension == "arrow" {
                let mut writer = FileWriter::try_new(file, &schema).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            } else {
                let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
                writer.write(&batch).unwrap();
                writer.close().unwrap();
            }
            let table = Table::load(path.to_str().unwrap()).unwrap();
            std::fs::remove_file(path).unwrap();
            assert_eq!(table.scalar("bytes", 2), Scalar::Int(60));
            assert_eq!(table.scalar("campaign_id", 1), Scalar::Null);
            assert!(table.bytes.is_empty());
            assert!(table.columnar.as_ref().unwrap().data.field("extra").is_ok());
        }
    }

    #[test]
    fn projected_batches_keep_typed_values_and_stable_dictionary_ids() {
        let (_, batch) = fixture();
        let projected = batch.project(&[0, 3, 10]).unwrap();
        let first = projected.slice(0, 2);
        let second = projected.slice(2, 1);
        let data = ColumnarTable::try_new(projected.schema(), vec![first, second]).unwrap();
        let table = Table::from_projected_columnar(data, Table::empty()).unwrap();
        assert_eq!(table.len(), 3);
        assert_eq!(table.scalar("event_id", 2), Scalar::Int(3));
        assert_eq!(table.scalar("campaign_id", 1), Scalar::Null);
        assert_eq!(table.scalar("country", 0), Scalar::Str("US".into()));
        assert_eq!(table.scalar("device", 0), Scalar::Null);
        assert_eq!(table.raw_key("country", 0), table.raw_key("country", 2));
        assert!(table.event_id.is_empty());
        assert!(table.campaign.is_empty());
        assert_eq!(table.campaign_null_count(), 1);

        let mut next = Table::empty();
        next.country_dict = table.country_dict.clone();
        let data = ColumnarTable::try_new(projected.schema(), vec![projected.slice(0, 1)]).unwrap();
        let next = Table::from_projected_columnar(data, next).unwrap();
        assert_eq!(table.raw_key("country", 0), next.raw_key("country", 0));
    }

    #[test]
    fn projected_columns_still_reject_null_required_values() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "event_id",
            DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        let data = ColumnarTable::try_new(schema, vec![batch]).unwrap();
        assert!(
            Table::from_projected_columnar(data, Table::empty())
                .err()
                .unwrap()
                .contains("required column event_id contains null")
        );
    }

    #[test]
    fn rejects_null_in_required_event_column() {
        let (schema, batch) = fixture();
        let mut fields: Vec<_> = schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        fields[0] = Field::new("event_id", DataType::Int64, true);
        let schema = Arc::new(Schema::new(fields));
        let mut columns = batch.columns().to_vec();
        columns[0] = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let data = ColumnarTable::try_new(schema, vec![batch]).unwrap();
        assert!(Table::from_columnar(data).is_err());
    }
}

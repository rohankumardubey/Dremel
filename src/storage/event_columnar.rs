use super::ColumnarTable;
use crate::types::Scalar;
use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::DataType;

const COLUMNS: [(&str, DataType); 11] = [
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
/// Eager Arrow and Parquet batches are coalesced once for constant-time row lookup.
pub(crate) struct EventColumnar {
    data: ColumnarTable,
    cast_bytes: usize,
    event_id: Int64Array,
    user_id: Int64Array,
    timestamp: Int64Array,
    country: StringArray,
    device: StringArray,
    event_type: StringArray,
    duration: Int64Array,
    bytes: Int64Array,
    score: Float64Array,
    success: BooleanArray,
    campaign: Int64Array,
}

impl EventColumnar {
    pub(crate) fn new(data: ColumnarTable) -> Result<Self, String> {
        let schema = data.schema().clone();
        let batch = concat_batches(&schema, data.batches()).map_err(|error| error.to_string())?;
        let mut columns = Vec::with_capacity(COLUMNS.len());
        let mut cast_bytes = 0usize;
        for (name, data_type) in &COLUMNS {
            let array = batch
                .column_by_name(name)
                .ok_or_else(|| format!("missing required column {name}"))?;
            let needs_cast = array.data_type() != data_type;
            let array = cast(array, data_type)
                .map_err(|error| format!("invalid {name} column: {error}"))?;
            if needs_cast {
                cast_bytes = cast_bytes.saturating_add(array.get_array_memory_size());
            }
            if *name != "campaign_id" && array.null_count() != 0 {
                return Err(format!("required column {name} contains null"));
            }
            columns.push(array);
        }
        let data = ColumnarTable::try_new(schema, vec![batch])?;
        Ok(Self {
            data,
            cast_bytes,
            event_id: typed(&columns[0]),
            user_id: typed(&columns[1]),
            timestamp: typed(&columns[2]),
            country: typed(&columns[3]),
            device: typed(&columns[4]),
            event_type: typed(&columns[5]),
            duration: typed(&columns[6]),
            bytes: typed(&columns[7]),
            score: typed(&columns[8]),
            success: typed(&columns[9]),
            campaign: typed(&columns[10]),
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
        self.campaign.null_count()
    }

    pub(crate) fn string(&self, column: &str, row: usize) -> &str {
        match column {
            "country" => self.country.value(row),
            "device" => self.device.value(row),
            "event_type" => self.event_type.value(row),
            _ => panic!("unknown event string column {column}"),
        }
    }

    pub(crate) fn scalar(&self, column: &str, row: usize) -> Scalar {
        match column {
            "event_id" => Scalar::Int(self.event_id.value(row)),
            "user_id" => Scalar::Int(self.user_id.value(row)),
            "timestamp" => Scalar::Int(self.timestamp.value(row)),
            "country" => Scalar::Str(self.country.value(row).into()),
            "device" => Scalar::Str(self.device.value(row).into()),
            "event_type" => Scalar::Str(self.event_type.value(row).into()),
            "duration_ms" => Scalar::Int(self.duration.value(row)),
            "bytes" => Scalar::Int(self.bytes.value(row)),
            "score" => Scalar::Float(self.score.value(row)),
            "success" => Scalar::Bool(self.success.value(row)),
            "campaign_id" if self.campaign.is_null(row) => Scalar::Null,
            "campaign_id" => Scalar::Int(self.campaign.value(row)),
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

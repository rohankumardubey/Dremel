use arrow::array::ArrayRef;
use arrow::datatypes::{Field, SchemaRef};
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashSet;
use std::fs::File;
use std::path::Path;

/// An Arrow-backed table with a schema independent of the engine's current SQL tables.
///
/// Each batch retains its original typed Arrow arrays, including null bitmaps and
/// nested data. Named-table SQL supports flat primitive fields; nested scans
/// retain containers and parent validity without flattening records.
#[derive(Debug)]
pub struct ColumnarTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    row_count: usize,
}

impl ColumnarTable {
    pub fn try_new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Self, String> {
        let mut names = HashSet::new();
        for field in schema.fields() {
            if !names.insert(field.name()) {
                return Err(format!("duplicate column name {}", field.name()));
            }
        }
        let mut row_count = 0usize;
        for (index, batch) in batches.iter().enumerate() {
            // Parquet can retain file metadata in the table schema while decoded
            // batches omit it. The typed field layout must still match.
            if batch.schema().fields() != schema.fields() {
                return Err(format!("batch {index} fields differ from table schema"));
            }
            row_count = row_count
                .checked_add(batch.num_rows())
                .ok_or("table row count overflow")?;
        }
        Ok(Self {
            schema,
            batches,
            row_count,
        })
    }

    pub fn read(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref();
        let file =
            File::open(path).map_err(|error| format!("cannot open {}: {error}", path.display()))?;
        match path.extension().and_then(|value| value.to_str()) {
            Some("arrow") => {
                let reader = FileReader::try_new(file, None).map_err(|error| error.to_string())?;
                let schema = reader.schema();
                let batches = reader
                    .map(|batch| batch.map_err(|error| error.to_string()))
                    .collect::<Result<Vec<_>, _>>()?;
                Self::try_new(schema, batches)
            }
            Some("parquet") => {
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                    .map_err(|error| error.to_string())?;
                let schema = builder.schema().clone();
                let reader = builder
                    .with_batch_size(65_536)
                    .build()
                    .map_err(|error| error.to_string())?;
                let batches = reader
                    .map(|batch| batch.map_err(|error| error.to_string()))
                    .collect::<Result<Vec<_>, _>>()?;
                Self::try_new(schema, batches)
            }
            _ => Err(format!(
                "unsupported columnar file {}; expected .arrow or .parquet",
                path.display()
            )),
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn approximate_bytes(&self) -> usize {
        self.batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum()
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn field(&self, name: &str) -> Result<&Field, String> {
        let index = self
            .schema
            .index_of(name)
            .map_err(|_| format!("unknown column {name}"))?;
        Ok(self.schema.field(index))
    }

    /// Returns cheap Arc clones of a named typed column across all batches.
    pub fn column_chunks(&self, name: &str) -> Result<Vec<ArrayRef>, String> {
        let index = self
            .schema
            .index_of(name)
            .map_err(|_| format!("unknown column {name}"))?;
        Ok(self
            .batches
            .iter()
            .map(|batch| batch.column(index).clone())
            .collect())
    }

    pub fn into_batches(self) -> Vec<RecordBatch> {
        self.batches
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

    fn fixture() -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("arbitrary_id", DataType::Int64, false),
            Field::new("optional_label", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec![Some("one"), None])),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    fn temp_file(extension: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "dremel-columnar-{}-{}.{}",
            std::process::id(),
            NEXT_FILE.fetch_add(1, Ordering::Relaxed),
            extension
        ))
    }

    #[test]
    fn preserves_typed_columns_and_nulls() {
        let (schema, batch) = fixture();
        let table = ColumnarTable::try_new(schema.clone(), vec![batch.clone(), batch]).unwrap();
        assert_eq!(table.schema().as_ref(), schema.as_ref());
        assert_eq!(table.row_count(), 4);
        assert_eq!(
            table.field("optional_label").unwrap().data_type(),
            &DataType::Utf8
        );
        let chunks = table.column_chunks("optional_label").unwrap();
        assert_eq!(chunks.len(), 2);
        assert!(chunks[0].is_null(1));
        assert!(table.column_chunks("missing").is_err());
    }

    #[test]
    fn rejects_schema_changes_and_duplicate_names() {
        let (schema, batch) = fixture();
        let other = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            true,
        )]));
        assert!(ColumnarTable::try_new(other, vec![batch]).is_err());
        let duplicate = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("id", DataType::Utf8, true),
        ]));
        assert!(ColumnarTable::try_new(duplicate, vec![]).is_err());
        assert_eq!(
            ColumnarTable::try_new(schema, vec![]).unwrap().row_count(),
            0
        );
    }

    #[test]
    fn retains_table_metadata_when_batches_omit_it() {
        let (schema, batch) = fixture();
        let with_metadata = Arc::new(Schema::new_with_metadata(
            schema.fields().clone(),
            [("dremel.table".into(), "arbitrary".into())].into(),
        ));
        let table = ColumnarTable::try_new(with_metadata, vec![batch]).unwrap();
        assert_eq!(table.schema().metadata()["dremel.table"], "arbitrary");
    }

    #[test]
    fn reads_non_builtin_arrow_and_parquet_schemas() {
        let (schema, batch) = fixture();
        let arrow_path = temp_file("arrow");
        let parquet_path = temp_file("parquet");
        {
            let file = File::create(&arrow_path).unwrap();
            let mut writer = FileWriter::try_new(file, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        {
            let file = File::create(&parquet_path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        for path in [&arrow_path, &parquet_path] {
            let table = ColumnarTable::read(path).unwrap();
            assert_eq!(table.schema().as_ref(), schema.as_ref());
            assert_eq!(table.row_count(), 2);
            assert!(table.column_chunks("optional_label").unwrap()[0].is_null(1));
            std::fs::remove_file(path).unwrap();
        }
    }
}

use super::ColumnarTable;
use crate::types::Scalar;
use arrow::array::{Array, ArrayRef, BooleanArray, Decimal128Array, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

const USERS: &[(&str, DataType)] = &[
    ("user_id", DataType::Int64),
    ("segment", DataType::Utf8),
    ("signup_date", DataType::Utf8),
    ("lifetime_value", DataType::Decimal128(18, 2)),
    ("region", DataType::Utf8),
    ("active", DataType::Boolean),
];
const CAMPAIGNS: &[(&str, DataType)] = &[
    ("campaign_id", DataType::Int64),
    ("campaign_name", DataType::Utf8),
    ("budget", DataType::Decimal128(18, 2)),
    ("start_date", DataType::Utf8),
    ("end_date", DataType::Utf8),
    ("channel", DataType::Utf8),
];

/// A dimension retains its Arrow schema and batches. Only the columns needed by
/// today's SQL operators are cast once; other columns remain available in `data`.
pub(crate) struct DimensionTable {
    pub(crate) data: ColumnarTable,
    pub(crate) index: HashMap<i64, Vec<usize>>,
    normalized: HashMap<String, Vec<ArrayRef>>,
    offsets: Vec<usize>,
}

impl DimensionTable {
    pub(crate) fn users(data: ColumnarTable) -> Result<Self, String> {
        Self::new(data, USERS, "user_id")
    }

    pub(crate) fn campaigns(data: ColumnarTable) -> Result<Self, String> {
        Self::new(data, CAMPAIGNS, "campaign_id")
    }

    fn new(data: ColumnarTable, fields: &[(&str, DataType)], key: &str) -> Result<Self, String> {
        let mut normalized = HashMap::new();
        for (name, data_type) in fields {
            data.field(name)?;
            let chunks = data
                .column_chunks(name)?
                .into_iter()
                .map(|array| {
                    let array = if matches!(
                        (data_type, array.data_type()),
                        (DataType::Decimal128(_, 2), DataType::Decimal128(_, 2))
                    ) {
                        array
                    } else {
                        cast(&array, data_type)
                            .map_err(|error| format!("invalid {name} column: {error}"))?
                    };
                    if array.null_count() != 0 {
                        return Err(format!("required column {name} contains null"));
                    }
                    if matches!(data_type, DataType::Decimal128(_, 2)) {
                        let values = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
                        if values
                            .values()
                            .iter()
                            .any(|value| i64::try_from(*value).is_err())
                        {
                            return Err(format!("{name} exceeds engine decimal range"));
                        }
                    }
                    Ok(array)
                })
                .collect::<Result<Vec<_>, String>>()?;
            normalized.insert((*name).to_string(), chunks);
        }
        let mut offsets = Vec::with_capacity(data.batches().len() + 1);
        offsets.push(0);
        for batch in data.batches() {
            offsets.push(offsets.last().unwrap() + batch.num_rows());
        }
        let mut index: HashMap<i64, Vec<usize>> = HashMap::new();
        for (batch_index, array) in normalized[key].iter().enumerate() {
            let ids = array.as_any().downcast_ref::<Int64Array>().unwrap();
            for row in 0..ids.len() {
                index
                    .entry(ids.value(row))
                    .or_default()
                    .push(offsets[batch_index] + row);
            }
        }
        Ok(Self {
            data,
            index,
            normalized,
            offsets,
        })
    }

    pub(crate) fn row_count(&self) -> usize {
        self.data.row_count()
    }

    pub(crate) fn scalar(&self, column: &str, row: usize) -> Scalar {
        let Some(chunks) = self.normalized.get(column) else {
            return Scalar::Null;
        };
        let batch = self
            .offsets
            .partition_point(|offset| *offset <= row)
            .saturating_sub(1);
        let Some(array) = chunks.get(batch) else {
            return Scalar::Null;
        };
        let local = row - self.offsets[batch];
        if local >= array.len() || array.is_null(local) {
            return Scalar::Null;
        }
        match array.data_type() {
            DataType::Int64 => Scalar::Int(
                array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(local),
            ),
            DataType::Utf8 => Scalar::Str(
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(local)
                    .into(),
            ),
            DataType::Boolean => Scalar::Bool(
                array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(local),
            ),
            DataType::Decimal128(_, 2) => i64::try_from(
                array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .value(local),
            )
            .map_or(Scalar::Null, Scalar::Decimal),
            _ => Scalar::Null,
        }
    }

    pub(crate) fn from_users_rows(
        rows: Vec<(i64, String, String, i64, String, bool)>,
    ) -> Result<Self, String> {
        let schema = Arc::new(Schema::new(
            USERS
                .iter()
                .map(|(name, kind)| Field::new(*name, kind.clone(), false))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|row| row.1.as_str()),
                )),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|row| row.2.as_str()),
                )),
                Arc::new(
                    Decimal128Array::from_iter_values(rows.iter().map(|row| i128::from(row.3)))
                        .with_precision_and_scale(18, 2)
                        .map_err(|error| error.to_string())?,
                ),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|row| row.4.as_str()),
                )),
                Arc::new(BooleanArray::from_iter(rows.iter().map(|row| Some(row.5)))),
            ],
        )
        .map_err(|error| error.to_string())?;
        Self::users(ColumnarTable::try_new(schema, vec![batch])?)
    }

    pub(crate) fn from_campaign_rows(
        rows: Vec<(i64, String, i64, String, String, String)>,
    ) -> Result<Self, String> {
        let schema = Arc::new(Schema::new(
            CAMPAIGNS
                .iter()
                .map(|(name, kind)| Field::new(*name, kind.clone(), false))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|row| row.1.as_str()),
                )),
                Arc::new(
                    Decimal128Array::from_iter_values(rows.iter().map(|row| i128::from(row.2)))
                        .with_precision_and_scale(18, 2)
                        .map_err(|error| error.to_string())?,
                ),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|row| row.3.as_str()),
                )),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|row| row.4.as_str()),
                )),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|row| row.5.as_str()),
                )),
            ],
        )
        .map_err(|error| error.to_string())?;
        Self::campaigns(ColumnarTable::try_new(schema, vec![batch])?)
    }
}

impl Default for DimensionTable {
    fn default() -> Self {
        Self {
            data: ColumnarTable::try_new(Arc::new(Schema::empty()), vec![]).unwrap(),
            index: HashMap::new(),
            normalized: HashMap::new(),
            offsets: vec![0],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::fs::File;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn indexes_multiple_batches_and_keeps_extra_parquet_columns() {
        let base = DimensionTable::from_campaign_rows(vec![
            (
                7,
                "seven".into(),
                7000,
                "2024-01-01".into(),
                "2024-02-01".into(),
                "search".into(),
            ),
            (
                7,
                "again".into(),
                9000,
                "2024-01-02".into(),
                "2024-02-02".into(),
                "email".into(),
            ),
        ])
        .unwrap();
        let original = &base.data.batches()[0];
        let mut fields: Vec<_> = original.schema().fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new("extra", DataType::Utf8, false)));
        let schema = Arc::new(Schema::new(fields));
        let make_batch = |index| {
            let mut arrays = original.slice(index, 1).columns().to_vec();
            arrays.push(Arc::new(StringArray::from(vec!["retained"])));
            RecordBatch::try_new(schema.clone(), arrays).unwrap()
        };
        let split = DimensionTable::campaigns(
            ColumnarTable::try_new(schema.clone(), vec![make_batch(0), make_batch(1)]).unwrap(),
        )
        .unwrap();
        assert_eq!(split.data.batches().len(), 2);
        assert_eq!(split.index[&7], vec![0, 1]);
        assert_eq!(split.scalar("budget", 1), Scalar::Decimal(9000));
        let path = std::env::temp_dir().join(format!(
            "dremel-dimension-{}-{}.parquet",
            std::process::id(),
            NEXT_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        {
            let properties = WriterProperties::builder()
                .set_max_row_group_row_count(Some(1))
                .build();
            let mut writer = ArrowWriter::try_new(
                File::create(&path).unwrap(),
                schema.clone(),
                Some(properties),
            )
            .unwrap();
            writer.write(&make_batch(0)).unwrap();
            writer.write(&make_batch(1)).unwrap();
            writer.close().unwrap();
        }
        let loaded = DimensionTable::campaigns(ColumnarTable::read(&path).unwrap()).unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(loaded.row_count(), 2);
        assert_eq!(loaded.index[&7], vec![0, 1]);
        assert_eq!(loaded.scalar("budget", 1), Scalar::Decimal(9000));
        assert_eq!(
            loaded.scalar("campaign_name", 0),
            Scalar::Str("seven".into())
        );
        assert_eq!(
            loaded.data.field("extra").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn rejects_null_and_out_of_range_required_values() {
        let base = DimensionTable::from_users_rows(vec![(
            1,
            "pro".into(),
            "2024-01-01".into(),
            100,
            "apac".into(),
            true,
        )])
        .unwrap();
        let schema = Arc::new(Schema::new(
            base.data
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    if field.name() == "segment" {
                        Arc::new(Field::new("segment", DataType::Utf8, true))
                    } else {
                        field.clone()
                    }
                })
                .collect::<Vec<_>>(),
        ));
        let mut arrays = base.data.batches()[0].columns().to_vec();
        arrays[1] = Arc::new(StringArray::from(vec![None::<&str>]));
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        assert!(
            DimensionTable::users(ColumnarTable::try_new(schema, vec![batch]).unwrap()).is_err()
        );

        let base = DimensionTable::from_campaign_rows(vec![(
            1,
            "one".into(),
            100,
            "2024-01-01".into(),
            "2024-02-01".into(),
            "email".into(),
        )])
        .unwrap();
        let schema = base.data.schema().clone();
        let mut arrays = base.data.batches()[0].columns().to_vec();
        arrays[2] = Arc::new(
            Decimal128Array::from(vec![i128::from(i64::MAX) + 1])
                .with_precision_and_scale(38, 2)
                .unwrap(),
        );
        let wide_schema = Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    if index == 2 {
                        Arc::new(Field::new("budget", DataType::Decimal128(38, 2), false))
                    } else {
                        field.clone()
                    }
                })
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(wide_schema.clone(), arrays).unwrap();
        assert!(
            DimensionTable::campaigns(ColumnarTable::try_new(wide_schema, vec![batch]).unwrap())
                .is_err()
        );
    }
}

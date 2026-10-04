use super::event_columnar::{COLUMNS, EventColumnar};
use super::{
    ColumnarTable, Dictionary, Table, read_bytes, read_dictionary, read_f64s, read_i64s, read_u32,
    read_u64,
};
use arrow::array::{
    ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int64Array, StringArray, UInt32Array,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Schema, UInt32Type};
use arrow::record_batch::RecordBatch;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;

fn dictionary_array(dictionary: &Dictionary, keys: Vec<u32>) -> Result<ArrayRef, String> {
    let values: ArrayRef = Arc::new(StringArray::from(dictionary.values.clone()));
    DictionaryArray::<UInt32Type>::try_new(UInt32Array::from(keys), values)
        .map(|array| Arc::new(array) as ArrayRef)
        .map_err(|error| error.to_string())
}

pub(super) fn load_binary(path: &str) -> Result<Table, String> {
    let mut file = File::open(path).map_err(|error| error.to_string())?;
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)
        .map_err(|error| error.to_string())?;
    if &magic != b"DREMCOL1" {
        return Err("invalid column-store magic".into());
    }
    let version = read_u32(&mut file)?;
    if version != 1 {
        return Err(format!("unsupported column-store version {version}"));
    }
    let rows: usize = read_u64(&mut file)?
        .try_into()
        .map_err(|_| "column-store row count exceeds platform limits")?;

    let event_id = read_i64s(&mut file, rows)?;
    let user_id = read_i64s(&mut file, rows)?;
    let timestamp = read_i64s(&mut file, rows)?;
    let (country_dict, country_keys) = read_dictionary(&mut file, rows)?;
    let (device_dict, device_keys) = read_dictionary(&mut file, rows)?;
    let (event_dict, event_keys) = read_dictionary(&mut file, rows)?;
    let duration = read_i64s(&mut file, rows)?;
    let bytes = read_i64s(&mut file, rows)?;
    let score = read_f64s(&mut file, rows)?;
    let success = read_bytes(&mut file, rows)?;
    let campaign = read_i64s(&mut file, rows)?;
    let campaign_def = read_bytes(&mut file, rows)?;

    let schema = Arc::new(Schema::new(
        COLUMNS
            .iter()
            .map(|(name, data_type)| {
                let data_type = if *data_type == DataType::Utf8 {
                    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8))
                } else {
                    data_type.clone()
                };
                Field::new(*name, data_type, *name == "campaign_id")
            })
            .collect::<Vec<_>>(),
    ));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(event_id)),
        Arc::new(Int64Array::from(user_id)),
        Arc::new(Int64Array::from(timestamp)),
        dictionary_array(&country_dict, country_keys)?,
        dictionary_array(&device_dict, device_keys)?,
        dictionary_array(&event_dict, event_keys)?,
        Arc::new(Int64Array::from(duration)),
        Arc::new(Int64Array::from(bytes)),
        Arc::new(Float64Array::from(score)),
        Arc::new(BooleanArray::from(
            success
                .into_iter()
                .map(|value| value != 0)
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::new(
            campaign.into(),
            Some(NullBuffer::from(
                campaign_def
                    .into_iter()
                    .map(|value| value != 0)
                    .collect::<Vec<_>>(),
            )),
        )),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns).map_err(|error| error.to_string())?;
    let data = ColumnarTable::try_new(schema, vec![batch])?;
    let columnar = EventColumnar::new(data)?;
    let mut table = Table::empty();
    table.logical_rows = rows;
    table.country_dict = country_dict;
    table.device_dict = device_dict;
    table.event_dict = event_dict;
    table.columnar = Some(columnar);
    Ok(table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Scalar;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

    fn fixture(country_keys: [u32; 3]) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dremel-native-columnar-{}-{}.dremel",
            std::process::id(),
            NEXT_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        let mut file = File::create(&path).unwrap();
        file.write_all(b"DREMCOL1").unwrap();
        file.write_all(&1u32.to_le_bytes()).unwrap();
        file.write_all(&3u64.to_le_bytes()).unwrap();
        for values in [[1i64, 2, 3], [10, 20, 30], [100, 200, 300]] {
            for value in values {
                file.write_all(&value.to_le_bytes()).unwrap();
            }
        }
        for (values, keys) in [
            (vec!["US", "IN"], country_keys),
            (vec!["mobile", "desktop"], [0, 1, 0]),
            (vec!["click", "view"], [0, 1, 0]),
        ] {
            file.write_all(&(values.len() as u32).to_le_bytes())
                .unwrap();
            for value in values {
                file.write_all(&(value.len() as u32).to_le_bytes()).unwrap();
                file.write_all(value.as_bytes()).unwrap();
            }
            for key in keys {
                file.write_all(&key.to_le_bytes()).unwrap();
            }
        }
        for values in [[4i64, 5, 6], [40, 50, 60]] {
            for value in values {
                file.write_all(&value.to_le_bytes()).unwrap();
            }
        }
        for value in [1.0f64, 2.0, 3.0] {
            file.write_all(&value.to_le_bytes()).unwrap();
        }
        file.write_all(&[1, 0, 1]).unwrap();
        for value in [7i64, 0, 8] {
            file.write_all(&value.to_le_bytes()).unwrap();
        }
        file.write_all(&[1, 0, 1]).unwrap();
        path
    }

    #[test]
    fn native_events_use_typed_dictionary_columns() {
        let path = fixture([0, 1, 0]);
        let table = Table::load_binary(path.to_str().unwrap()).unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(table.len(), 3);
        assert!(table.event_id.is_empty());
        assert!(table.country.is_empty());
        assert_eq!(table.scalar("event_id", 2), Scalar::Int(3));
        assert_eq!(table.scalar("country", 1), Scalar::Str("IN".into()));
        assert_eq!(table.raw_key("country", 0), table.raw_key("country", 2));
        assert_ne!(table.raw_key("country", 0), table.raw_key("country", 1));
        assert_eq!(table.dict_id("country", "IN"), Some(1));
        assert_eq!(table.scalar("campaign_id", 1), Scalar::Null);
        assert_eq!(table.campaign_null_count(), 1);
        assert_eq!(table.scalar("success", 1), Scalar::Bool(false));
        assert_eq!(
            table
                .columnar
                .as_ref()
                .unwrap()
                .dictionary_key("country", 1),
            Some(1)
        );
    }

    #[test]
    fn rejects_out_of_range_dictionary_key() {
        let path = fixture([0, 2, 0]);
        let result = Table::load_binary(path.to_str().unwrap());
        std::fs::remove_file(path).unwrap();
        assert!(result.is_err());
    }
}

use super::{CampaignsTable, Table, UsersTable};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int64Array, StringArray,
};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

pub(super) fn load_arrow_ipc(path: &str) -> Result<Table, String> {
    load_batches(read_batches(Path::new(path))?)
}

pub(super) fn load_parquet(path: &str) -> Result<Table, String> {
    load_batches(read_batches(Path::new(path))?)
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

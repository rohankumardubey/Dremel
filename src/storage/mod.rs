use crate::sql::*;
use crate::types::Scalar;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::sync::Arc;

mod binding;
mod columnar;
mod dimension;
mod event_columnar;
mod interoperable;
mod native;
mod nested_scan;
mod primitive;
pub(crate) use binding::*;
pub use columnar::ColumnarTable;
pub(crate) use dimension::DimensionTable;
use event_columnar::EventColumnar;
pub(crate) use interoperable::ParquetScanMetrics;
pub use nested_scan::{ParquetScan, ParquetScanOptions, ParquetScanPlan};
pub(crate) use primitive::PrimitiveBatch;

#[derive(Clone, Default)]
pub(crate) struct Dictionary {
    pub(crate) values: Vec<String>,
    pub(crate) ids: std::collections::HashMap<String, u32>,
}
impl Dictionary {
    pub(crate) fn insert(&mut self, s: &str) -> u32 {
        if let Some(v) = self.ids.get(s) {
            *v
        } else {
            let id = self.values.len() as u32;
            self.values.push(s.into());
            self.ids.insert(s.into(), id);
            id
        }
    }
    pub(crate) fn get(&self, id: u32) -> &str {
        &self.values[id as usize]
    }
}
pub(crate) fn read_bytes<R: Read>(r: &mut R, n: usize) -> Result<Vec<u8>, String> {
    let mut v = vec![0; n];
    r.read_exact(&mut v).map_err(|e| e.to_string())?;
    Ok(v)
}
pub(crate) fn read_u32<R: Read>(r: &mut R) -> Result<u32, String> {
    let b = read_bytes(r, 4)?;
    Ok(u32::from_le_bytes(b.try_into().expect("four bytes")))
}
pub(crate) fn read_u64<R: Read>(r: &mut R) -> Result<u64, String> {
    let b = read_bytes(r, 8)?;
    Ok(u64::from_le_bytes(b.try_into().expect("eight bytes")))
}
pub(crate) fn read_i64s<R: Read>(r: &mut R, n: usize) -> Result<Vec<i64>, String> {
    let b = read_bytes(r, n * 8)?;
    Ok(b.chunks_exact(8)
        .map(|x| i64::from_le_bytes(x.try_into().expect("eight bytes")))
        .collect())
}
pub(crate) fn read_f64s<R: Read>(r: &mut R, n: usize) -> Result<Vec<f64>, String> {
    let b = read_bytes(r, n * 8)?;
    Ok(b.chunks_exact(8)
        .map(|x| f64::from_le_bytes(x.try_into().expect("eight bytes")))
        .collect())
}
pub(crate) fn read_u32s<R: Read>(r: &mut R, n: usize) -> Result<Vec<u32>, String> {
    let b = read_bytes(r, n * 4)?;
    Ok(b.chunks_exact(4)
        .map(|x| u32::from_le_bytes(x.try_into().expect("four bytes")))
        .collect())
}
pub(crate) fn read_dictionary<R: Read>(
    r: &mut R,
    rows: usize,
) -> Result<(Dictionary, Vec<u32>), String> {
    let count = read_u32(r)?;
    let mut d = Dictionary::default();
    for _ in 0..count {
        let len = read_u32(r)? as usize;
        let raw = read_bytes(r, len)?;
        let value = String::from_utf8(raw).map_err(|e| e.to_string())?;
        d.insert(&value);
    }
    Ok((d, read_u32s(r, rows)?))
}
pub struct Table {
    columnar: Option<EventColumnar>,
    pub(crate) logical_rows: usize,
    pub(crate) event_id: Vec<i64>,
    pub(crate) user_id: Vec<i64>,
    pub(crate) timestamp: Vec<i64>,
    pub(crate) country: Vec<u32>,
    pub(crate) country_dict: Dictionary,
    pub(crate) device: Vec<u32>,
    pub(crate) device_dict: Dictionary,
    pub(crate) event_type: Vec<u32>,
    pub(crate) event_dict: Dictionary,
    pub(crate) duration: Vec<i64>,
    pub(crate) bytes: Vec<i64>,
    pub(crate) score: Vec<f64>,
    pub(crate) success: Vec<u8>,
    pub(crate) campaign: Vec<i64>,
    pub(crate) campaign_def: Vec<u8>,
}

pub(crate) struct Catalog {
    pub(crate) events: Arc<Table>,
    pub(crate) users: DimensionTable,
    pub(crate) campaigns: DimensionTable,
}

pub(crate) fn parse_decimal_cents(value: &str) -> Result<i64, String> {
    let (negative, value) = value
        .strip_prefix('-')
        .map_or((false, value), |value| (true, value));
    let mut parts = value.split('.');
    let whole: i64 = parts
        .next()
        .ok_or("missing decimal")?
        .parse()
        .map_err(|_| "bad decimal")?;
    let fraction = parts.next().unwrap_or("0");
    if parts.next().is_some() || fraction.len() > 2 || !fraction.chars().all(|c| c.is_ascii_digit())
    {
        return Err("DECIMAL(18,2) requires at most two fractional digits".into());
    }
    let fraction: i64 = format!("{fraction:0<2}")
        .parse()
        .map_err(|_| "bad decimal fraction")?;
    let units = whole
        .checked_mul(100)
        .and_then(|whole| whole.checked_add(fraction))
        .ok_or("decimal overflow")?;
    Ok(if negative { -units } else { units })
}

impl Catalog {
    pub(crate) fn load(events_path: &str, events: Arc<Table>) -> Result<Self, String> {
        let directory = Path::new(events_path)
            .parent()
            .unwrap_or_else(|| Path::new("."));
        if events_path.ends_with(".arrow") || events_path.ends_with(".parquet") {
            let event_name = Path::new(events_path)
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or("invalid events path")?;
            let users_path = directory.join(event_name.replacen("events", "users", 1));
            let campaigns_path = directory.join(event_name.replacen("events", "campaigns", 1));
            return Ok(Self {
                events,
                users: interoperable::load_users(&users_path)?,
                campaigns: interoperable::load_campaigns(&campaigns_path)?,
            });
        }
        let users_path = directory.join("users.csv");
        let campaigns_path = directory.join("campaigns.csv");
        let mut users = Vec::new();
        let user_file = File::open(&users_path)
            .map_err(|error| format!("cannot open {}: {error}", users_path.display()))?;
        for (line_number, line) in BufReader::new(user_file).lines().enumerate() {
            let line = line.map_err(|error| error.to_string())?;
            if line_number == 0 {
                continue;
            }
            let fields: Vec<_> = line.split(',').collect();
            if fields.len() != 6 {
                return Err(format!("bad users row {}", line_number + 1));
            }
            let id = fields[0].parse().map_err(|_| "bad users.user_id")?;
            users.push((
                id,
                fields[1].into(),
                fields[2].into(),
                parse_decimal_cents(fields[3]).map_err(|_| "bad users.lifetime_value")?,
                fields[4].into(),
                fields[5] == "true",
            ));
        }
        let mut campaigns = Vec::new();
        let campaign_file = File::open(&campaigns_path)
            .map_err(|error| format!("cannot open {}: {error}", campaigns_path.display()))?;
        for (line_number, line) in BufReader::new(campaign_file).lines().enumerate() {
            let line = line.map_err(|error| error.to_string())?;
            if line_number == 0 {
                continue;
            }
            let fields: Vec<_> = line.split(',').collect();
            if fields.len() != 6 {
                return Err(format!("bad campaigns row {}", line_number + 1));
            }
            let id = fields[0].parse().map_err(|_| "bad campaigns.campaign_id")?;
            campaigns.push((
                id,
                fields[1].into(),
                parse_decimal_cents(fields[2]).map_err(|_| "bad campaigns.budget")?,
                fields[3].into(),
                fields[4].into(),
                fields[5].into(),
            ));
        }
        Ok(Self {
            events,
            users: DimensionTable::from_users_rows(users)?,
            campaigns: DimensionTable::from_campaign_rows(campaigns)?,
        })
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct RelRow {
    pub(crate) event: Option<usize>,
    pub(crate) user: Option<usize>,
    pub(crate) campaign: Option<usize>,
}

pub(crate) fn relation_scalar(catalog: &Catalog, row: RelRow, table: &str, column: &str) -> Scalar {
    match (table, column) {
        ("events", column) => row
            .event
            .map_or(Scalar::Null, |index| catalog.events.scalar(column, index)),
        ("users", column) => row
            .user
            .map_or(Scalar::Null, |i| catalog.users.scalar(column, i)),
        ("campaigns", column) => row
            .campaign
            .map_or(Scalar::Null, |i| catalog.campaigns.scalar(column, i)),
        _ => Scalar::Null,
    }
}
impl Table {
    pub(crate) fn parquet_metadata(path: &str) -> Result<Self, String> {
        interoperable::parquet_metadata_table(path)
    }
    pub(crate) fn load_parquet_direct(
        path: &str,
        query: &Query,
        batch_size: usize,
    ) -> Result<(Self, ParquetScanMetrics), String> {
        interoperable::load_parquet_direct(path, query, batch_size)
    }
    pub(crate) fn stream_parquet_direct<F>(
        path: &str,
        query: &Query,
        batch_size: usize,
        consume: F,
    ) -> Result<(Self, ParquetScanMetrics), String>
    where
        F: FnMut(Self) -> Result<(), String>,
    {
        interoperable::stream_parquet_direct(path, query, batch_size, consume)
    }
    pub(crate) fn parquet_scan_plan(
        path: &str,
        query: &Query,
    ) -> Result<ParquetScanMetrics, String> {
        interoperable::parquet_scan_plan(path, query)
    }
    pub fn load(path: &str) -> Result<Self, String> {
        if path.ends_with(".dremel") {
            Self::load_binary(path)
        } else if path.ends_with(".arrow") {
            interoperable::load_arrow_ipc(path)
        } else if path.ends_with(".parquet") {
            interoperable::load_parquet(path)
        } else {
            Self::load_csv(path)
        }
    }
    pub(crate) fn empty() -> Self {
        Self {
            columnar: None,
            logical_rows: 0,
            event_id: vec![],
            user_id: vec![],
            timestamp: vec![],
            country: vec![],
            country_dict: Dictionary::default(),
            device: vec![],
            device_dict: Dictionary::default(),
            event_type: vec![],
            event_dict: Dictionary::default(),
            duration: vec![],
            bytes: vec![],
            score: vec![],
            success: vec![],
            campaign: vec![],
            campaign_def: vec![],
        }
    }
    pub(crate) fn from_columnar(data: ColumnarTable) -> Result<Self, String> {
        let columnar = EventColumnar::new(data)?;
        let mut table = Self::empty();
        table.logical_rows = columnar.row_count();
        for row in 0..table.logical_rows {
            table
                .country
                .push(table.country_dict.insert(columnar.string("country", row)));
            table
                .device
                .push(table.device_dict.insert(columnar.string("device", row)));
            table
                .event_type
                .push(table.event_dict.insert(columnar.string("event_type", row)));
        }
        table.columnar = Some(columnar);
        Ok(table)
    }
    pub(crate) fn load_binary(path: &str) -> Result<Self, String> {
        native::load_binary(path)
    }
    pub(crate) fn load_csv(path: &str) -> Result<Self, String> {
        let f = File::open(path).map_err(|e| e.to_string())?;
        let mut lines = BufReader::new(f).lines();
        lines
            .next()
            .ok_or("empty csv")?
            .map_err(|e| e.to_string())?;
        let mut t = Self::empty();
        for line in lines {
            let line = line.map_err(|e| e.to_string())?;
            let p: Vec<&str> = line.split(',').collect();
            if p.len() != 11 {
                return Err(format!("bad CSV row: {line}"));
            }
            t.event_id.push(p[0].parse().map_err(|_| "event_id")?);
            t.user_id.push(p[1].parse().map_err(|_| "user_id")?);
            t.timestamp.push(p[2].parse().map_err(|_| "timestamp")?);
            t.country.push(t.country_dict.insert(p[3]));
            t.device.push(t.device_dict.insert(p[4]));
            t.event_type.push(t.event_dict.insert(p[5]));
            t.duration.push(p[6].parse().map_err(|_| "duration")?);
            t.bytes.push(p[7].parse().map_err(|_| "bytes")?);
            t.score.push(p[8].parse().map_err(|_| "score")?);
            t.success.push(u8::from(p[9] == "true"));
            if p[10].is_empty() {
                t.campaign.push(0);
                t.campaign_def.push(0)
            } else {
                t.campaign.push(p[10].parse().map_err(|_| "campaign")?);
                t.campaign_def.push(1)
            }
        }
        Ok(t)
    }
    pub(crate) fn len(&self) -> usize {
        if self.event_id.is_empty() {
            self.logical_rows
        } else {
            self.event_id.len()
        }
    }
    pub(crate) fn campaign_null_count(&self) -> usize {
        self.columnar.as_ref().map_or_else(
            || {
                self.campaign_def
                    .iter()
                    .filter(|&&level| level == 0)
                    .count()
            },
            EventColumnar::campaign_null_count,
        )
    }
    pub(crate) fn event_id_bounds(&self) -> (i64, i64) {
        if self.len() == 0 {
            (0, 0)
        } else if let Some(columnar) = &self.columnar {
            let first = columnar.scalar("event_id", 0);
            let last = columnar.scalar("event_id", self.len() - 1);
            match (first, last) {
                (Scalar::Int(first), Scalar::Int(last)) => (first, last),
                _ => (0, 0),
            }
        } else {
            (
                *self.event_id.first().unwrap_or(&0),
                *self.event_id.last().unwrap_or(&0),
            )
        }
    }
    pub(crate) fn approximate_bytes(&self) -> usize {
        self.columnar
            .as_ref()
            .map_or(0, EventColumnar::approximate_bytes)
            + (self.event_id.len()
                + self.user_id.len()
                + self.timestamp.len()
                + self.duration.len()
                + self.bytes.len()
                + self.campaign.len())
                * 8
            + (self.country.len() + self.device.len() + self.event_type.len()) * 4
            + self.score.len() * 8
            + self.success.len()
            + self.campaign_def.len()
            + self
                .country_dict
                .values
                .iter()
                .chain(&self.device_dict.values)
                .chain(&self.event_dict.values)
                .map(String::len)
                .sum::<usize>()
    }
    pub(crate) fn group_upper_bound(&self, columns: &[String]) -> usize {
        columns
            .iter()
            .map(|column| match column.rsplit('.').next().unwrap_or(column) {
                "country" if !self.country_dict.values.is_empty() => self.country_dict.values.len(),
                "device" if !self.device_dict.values.is_empty() => self.device_dict.values.len(),
                "event_type" if !self.event_dict.values.is_empty() => self.event_dict.values.len(),
                "success" => 2,
                _ => self.len(),
            })
            .fold(1usize, |a, b| a.saturating_mul(b))
            .min(self.len())
    }
    pub(crate) fn dict_id(&self, col: &str, s: &str) -> Option<u32> {
        match col.rsplit('.').next().unwrap_or(col) {
            "country" => self.country_dict.ids.get(s).copied(),
            "device" => self.device_dict.ids.get(s).copied(),
            "event_type" => self.event_dict.ids.get(s).copied(),
            _ => None,
        }
    }
    pub(crate) fn scalar(&self, c: &str, i: usize) -> Scalar {
        if let Some(columnar) = &self.columnar {
            return columnar.scalar(c.rsplit('.').next().unwrap_or(c), i);
        }
        match c.rsplit('.').next().unwrap_or(c) {
            "event_id" => Scalar::Int(self.event_id[i]),
            "user_id" => Scalar::Int(self.user_id[i]),
            "timestamp" => Scalar::Int(self.timestamp[i]),
            "country" => Scalar::Str(self.country_dict.get(self.country[i]).into()),
            "device" => Scalar::Str(self.device_dict.get(self.device[i]).into()),
            "event_type" => Scalar::Str(self.event_dict.get(self.event_type[i]).into()),
            "duration_ms" => Scalar::Int(self.duration[i]),
            "bytes" => Scalar::Int(self.bytes[i]),
            "score" => Scalar::Float(self.score[i]),
            "success" => Scalar::Bool(self.success[i] != 0),
            "campaign_id" => {
                if self.campaign_def[i] == 0 {
                    Scalar::Null
                } else {
                    Scalar::Int(self.campaign[i])
                }
            }
            _ => Scalar::Null,
        }
    }
    pub(crate) fn raw_key(&self, c: &str, i: usize) -> u64 {
        match c.rsplit('.').next().unwrap_or(c) {
            "country" => self
                .country
                .get(i)
                .copied()
                .or_else(|| self.columnar.as_ref()?.dictionary_key("country", i))
                .expect("country dictionary key") as u64,
            "device" => self
                .device
                .get(i)
                .copied()
                .or_else(|| self.columnar.as_ref()?.dictionary_key("device", i))
                .expect("device dictionary key") as u64,
            "event_type" => self
                .event_type
                .get(i)
                .copied()
                .or_else(|| self.columnar.as_ref()?.dictionary_key("event_type", i))
                .expect("event_type dictionary key") as u64,
            "success" if self.columnar.is_none() => self.success[i] as u64,
            "campaign_id" if self.columnar.is_none() => self.campaign[i] as u64,
            _ => match self.scalar(c, i) {
                Scalar::Int(v) => v as u64,
                Scalar::Bool(v) => u64::from(v),
                _ => 0,
            },
        }
    }
    pub(crate) fn key_scalar(&self, c: &str, k: u64) -> Scalar {
        match c.rsplit('.').next().unwrap_or(c) {
            "country" => Scalar::Str(self.country_dict.get(k as u32).into()),
            "device" => Scalar::Str(self.device_dict.get(k as u32).into()),
            "event_type" => Scalar::Str(self.event_dict.get(k as u32).into()),
            "success" => Scalar::Bool(k != 0),
            _ => Scalar::Int(k as i64),
        }
    }
}

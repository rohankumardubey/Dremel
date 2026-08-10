use crate::sql::*;
use crate::types::Scalar;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::sync::Arc;

mod interoperable;

#[derive(Default)]
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

#[derive(Default)]
pub(crate) struct UsersTable {
    pub(crate) user_id: Vec<i64>,
    pub(crate) segment: Vec<String>,
    pub(crate) signup_date: Vec<String>,
    pub(crate) lifetime_value: Vec<i64>,
    pub(crate) region: Vec<String>,
    pub(crate) active: Vec<bool>,
    pub(crate) index: std::collections::HashMap<i64, Vec<usize>>,
}

#[derive(Default)]
pub(crate) struct CampaignsTable {
    pub(crate) campaign_id: Vec<i64>,
    pub(crate) campaign_name: Vec<String>,
    pub(crate) budget: Vec<i64>,
    pub(crate) start_date: Vec<String>,
    pub(crate) end_date: Vec<String>,
    pub(crate) channel: Vec<String>,
    pub(crate) index: std::collections::HashMap<i64, Vec<usize>>,
}

pub(crate) struct Catalog {
    pub(crate) events: Arc<Table>,
    pub(crate) users: UsersTable,
    pub(crate) campaigns: CampaignsTable,
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
        let mut users = UsersTable::default();
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
            let row = users.user_id.len();
            let id = fields[0].parse().map_err(|_| "bad users.user_id")?;
            users.user_id.push(id);
            users.segment.push(fields[1].into());
            users.signup_date.push(fields[2].into());
            users
                .lifetime_value
                .push(parse_decimal_cents(fields[3]).map_err(|_| "bad users.lifetime_value")?);
            users.region.push(fields[4].into());
            users.active.push(fields[5] == "true");
            users.index.entry(id).or_default().push(row);
        }
        let mut campaigns = CampaignsTable::default();
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
            let row = campaigns.campaign_id.len();
            let id = fields[0].parse().map_err(|_| "bad campaigns.campaign_id")?;
            campaigns.campaign_id.push(id);
            campaigns.campaign_name.push(fields[1].into());
            campaigns
                .budget
                .push(parse_decimal_cents(fields[2]).map_err(|_| "bad campaigns.budget")?);
            campaigns.start_date.push(fields[3].into());
            campaigns.end_date.push(fields[4].into());
            campaigns.channel.push(fields[5].into());
            campaigns.index.entry(id).or_default().push(row);
        }
        Ok(Self {
            events,
            users,
            campaigns,
        })
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct RelRow {
    pub(crate) event: Option<usize>,
    pub(crate) user: Option<usize>,
    pub(crate) campaign: Option<usize>,
}

pub(crate) fn table_columns(table: &str) -> &'static [&'static str] {
    match table {
        "events" => &[
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
        ],
        "users" => &[
            "user_id",
            "segment",
            "signup_date",
            "lifetime_value",
            "region",
            "active",
        ],
        "campaigns" => &[
            "campaign_id",
            "campaign_name",
            "budget",
            "start_date",
            "end_date",
            "channel",
        ],
        _ => &[],
    }
}

pub(crate) fn query_bindings(
    query: &Query,
) -> Result<std::collections::HashMap<String, String>, String> {
    let mut bindings = std::collections::HashMap::new();
    for table in std::iter::once(&query.from).chain(query.joins.iter().map(|join| &join.table)) {
        if table_columns(&table.name).is_empty() {
            return Err(format!("unknown table {}", table.name));
        }
        if bindings
            .insert(table.alias.clone(), table.name.clone())
            .is_some()
        {
            return Err(format!("duplicate table alias {}", table.alias));
        }
        bindings
            .entry(table.name.clone())
            .or_insert_with(|| table.name.clone());
    }
    Ok(bindings)
}

pub(crate) fn resolve_column(
    column: &str,
    bindings: &std::collections::HashMap<String, String>,
) -> Result<(String, String), String> {
    if let Some((qualifier, name)) = column.split_once('.') {
        let table = bindings
            .get(qualifier)
            .ok_or_else(|| format!("unknown table or alias {qualifier}"))?;
        if !table_columns(table).contains(&name) {
            return Err(format!("unknown column {column}"));
        }
        return Ok((table.clone(), name.into()));
    }
    let mut tables: Vec<_> = bindings
        .values()
        .filter(|table| table_columns(table).contains(&column))
        .cloned()
        .collect();
    tables.sort();
    tables.dedup();
    match tables.as_slice() {
        [] => Err(format!("unknown column {column}")),
        [table] => Ok((table.clone(), column.into())),
        _ => Err(format!("ambiguous column {column}")),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SqlType {
    Null,
    Bool,
    Int,
    Decimal,
    Double,
    String,
    Date,
    Timestamp,
    Interval,
    Unknown,
}

pub(crate) fn column_type(table: &str, column: &str) -> SqlType {
    match (table, column) {
        ("events", "event_id" | "user_id" | "duration_ms" | "bytes" | "campaign_id")
        | ("users", "user_id")
        | ("campaigns", "campaign_id") => SqlType::Int,
        ("events", "timestamp") => SqlType::Timestamp,
        ("events", "score") => SqlType::Double,
        ("events", "success") | ("users", "active") => SqlType::Bool,
        ("users", "lifetime_value") | ("campaigns", "budget") => SqlType::Decimal,
        ("users", "signup_date") | ("campaigns", "start_date" | "end_date") => SqlType::Date,
        ("events", "country" | "device" | "event_type")
        | ("users", "segment" | "region")
        | ("campaigns", "campaign_name" | "channel") => SqlType::String,
        _ => SqlType::Unknown,
    }
}

pub(crate) fn numeric_type(data_type: SqlType) -> bool {
    matches!(data_type, SqlType::Int | SqlType::Decimal | SqlType::Double)
}

pub(crate) fn common_type(left: SqlType, right: SqlType) -> Result<SqlType, String> {
    if left == right {
        return Ok(left);
    }
    if left == SqlType::Null || left == SqlType::Unknown {
        return Ok(right);
    }
    if right == SqlType::Null || right == SqlType::Unknown {
        return Ok(left);
    }
    if numeric_type(left) && numeric_type(right) {
        return Ok(if left == SqlType::Double || right == SqlType::Double {
            SqlType::Double
        } else if left == SqlType::Decimal || right == SqlType::Decimal {
            SqlType::Decimal
        } else {
            SqlType::Int
        });
    }
    Err(format!("incompatible SQL types {left:?} and {right:?}"))
}

pub(crate) fn infer_type(
    expression: &Expr,
    bindings: &std::collections::HashMap<String, String>,
) -> Result<SqlType, String> {
    match expression {
        Expr::Null => Ok(SqlType::Null),
        Expr::Column(column) => {
            let (table, column) = resolve_column(column, bindings)?;
            Ok(column_type(&table, &column))
        }
        Expr::Int(_) | Expr::Star => Ok(SqlType::Int),
        Expr::Float(_) => Ok(SqlType::Double),
        Expr::Bool(_) | Expr::DictEq(_, _, _) | Expr::IsNull(_, _) => Ok(SqlType::Bool),
        Expr::String(_) => Ok(SqlType::String),
        Expr::Unary(operator, value) => {
            let value = infer_type(value, bindings)?;
            if operator == "not" {
                if !matches!(value, SqlType::Bool | SqlType::Null | SqlType::Unknown) {
                    return Err("NOT requires BOOLEAN".into());
                }
                Ok(SqlType::Bool)
            } else if numeric_type(value) || matches!(value, SqlType::Null | SqlType::Unknown) {
                Ok(value)
            } else {
                Err("unary minus requires a numeric operand".into())
            }
        }
        Expr::Binary(operator, left, right) => {
            let left = infer_type(left, bindings)?;
            let right = infer_type(right, bindings)?;
            match operator.as_str() {
                "and" | "or" => {
                    if !matches!(left, SqlType::Bool | SqlType::Null | SqlType::Unknown)
                        || !matches!(right, SqlType::Bool | SqlType::Null | SqlType::Unknown)
                    {
                        return Err(format!("{operator} requires BOOLEAN operands"));
                    }
                    Ok(SqlType::Bool)
                }
                "=" | "!=" | "<" | "<=" | ">" | ">=" => {
                    common_type(left, right)?;
                    Ok(SqlType::Bool)
                }
                "+" | "-" if left == SqlType::Date && right == SqlType::Interval => {
                    Ok(SqlType::Date)
                }
                "+" | "-" if left == SqlType::Timestamp && right == SqlType::Interval => {
                    Ok(SqlType::Timestamp)
                }
                "+" | "-" | "*" | "/" if numeric_type(left) && numeric_type(right) => {
                    if operator == "/" {
                        Ok(SqlType::Double)
                    } else {
                        common_type(left, right)
                    }
                }
                "+" | "-" | "*" | "/"
                    if matches!(left, SqlType::Null | SqlType::Unknown)
                        || matches!(right, SqlType::Null | SqlType::Unknown) =>
                {
                    Ok(common_type(left, right).unwrap_or(SqlType::Unknown))
                }
                _ => Err(format!(
                    "{operator} has incompatible operand types {left:?}, {right:?}"
                )),
            }
        }
        Expr::Func(name, argument) => {
            let argument = infer_type(argument, bindings)?;
            match name.as_str() {
                "count" => Ok(SqlType::Int),
                "avg" => Ok(SqlType::Double),
                "sum" | "min" | "max" => Ok(argument),
                "lower" | "upper" | "substring" => Ok(SqlType::String),
                "length" => Ok(SqlType::Int),
                "abs" => numeric_type(argument)
                    .then_some(argument)
                    .ok_or_else(|| "ABS requires a numeric operand".into()),
                "date" => Ok(SqlType::Date),
                "timestamp" => Ok(SqlType::Timestamp),
                "interval" => Ok(SqlType::Interval),
                _ => Ok(SqlType::Unknown),
            }
        }
        Expr::Call(name, arguments) => {
            let types: Vec<_> = arguments
                .iter()
                .map(|argument| infer_type(argument, bindings))
                .collect::<Result<_, _>>()?;
            match name.as_str() {
                "count" => Ok(SqlType::Int),
                "avg" => Ok(SqlType::Double),
                "sum" | "min" | "max" => Ok(types.first().copied().unwrap_or(SqlType::Unknown)),
                "coalesce" => types.into_iter().try_fold(SqlType::Null, common_type),
                "nullif" => {
                    if types.len() == 2 {
                        common_type(types[0], types[1])?;
                    }
                    Ok(types.first().copied().unwrap_or(SqlType::Unknown))
                }
                "concat" | "substring" => Ok(SqlType::String),
                "date" => Ok(SqlType::Date),
                "timestamp" => Ok(SqlType::Timestamp),
                "interval" => Ok(SqlType::Interval),
                "date_trunc" => Ok(types.get(1).copied().unwrap_or(SqlType::Unknown)),
                "extract" | "length" => Ok(SqlType::Int),
                _ => Ok(SqlType::Unknown),
            }
        }
        Expr::Case(branches, fallback) => {
            let mut result = infer_type(fallback, bindings)?;
            for (condition, value) in branches {
                let condition = infer_type(condition, bindings)?;
                if !matches!(condition, SqlType::Bool | SqlType::Null | SqlType::Unknown) {
                    return Err("CASE WHEN requires BOOLEAN".into());
                }
                result = common_type(result, infer_type(value, bindings)?)?;
            }
            Ok(result)
        }
        Expr::Cast(_, data_type) => Ok(if data_type.starts_with("decimal") {
            SqlType::Decimal
        } else {
            match data_type.as_str() {
                "bigint" | "int64" | "integer" => SqlType::Int,
                "double" | "float" | "real" => SqlType::Double,
                "varchar" | "string" | "text" => SqlType::String,
                "boolean" | "bool" => SqlType::Bool,
                "date" => SqlType::Date,
                "timestamp" => SqlType::Timestamp,
                _ => SqlType::Unknown,
            }
        }),
        Expr::InList(value, candidates, _) => {
            let value = infer_type(value, bindings)?;
            for candidate in candidates {
                common_type(value, infer_type(candidate, bindings)?)?;
            }
            Ok(SqlType::Bool)
        }
        Expr::Between(value, low, high, _) => {
            let value = infer_type(value, bindings)?;
            common_type(value, infer_type(low, bindings)?)?;
            common_type(value, infer_type(high, bindings)?)?;
            Ok(SqlType::Bool)
        }
        Expr::Like(value, pattern, _) => {
            common_type(infer_type(value, bindings)?, SqlType::String)?;
            common_type(infer_type(pattern, bindings)?, SqlType::String)?;
            Ok(SqlType::Bool)
        }
        Expr::Window { name, args, .. } => match name.as_str() {
            "row_number" | "rank" | "dense_rank" | "count" => Ok(SqlType::Int),
            "avg" => Ok(SqlType::Double),
            _ => args.first().map_or(Ok(SqlType::Unknown), |argument| {
                infer_type(argument, bindings)
            }),
        },
        Expr::ScalarSubquery(_) => Ok(SqlType::Unknown),
        Expr::InSubquery(_, _, _) | Expr::Exists(_) => Ok(SqlType::Bool),
    }
}

pub(crate) fn bind_expr(
    expression: &Expr,
    bindings: &std::collections::HashMap<String, String>,
) -> Result<(), String> {
    match expression {
        Expr::Column(column) | Expr::DictEq(column, _, _) => {
            resolve_column(column, bindings)?;
        }
        Expr::Unary(_, value) | Expr::Func(_, value) | Expr::IsNull(value, _) => {
            bind_expr(value, bindings)?;
        }
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            bind_expr(left, bindings)?;
            bind_expr(right, bindings)?;
        }
        Expr::Call(_, args) => {
            for arg in args {
                bind_expr(arg, bindings)?;
            }
        }
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                bind_expr(condition, bindings)?;
                bind_expr(value, bindings)?;
            }
            bind_expr(fallback, bindings)?;
        }
        Expr::Cast(value, data_type) => {
            bind_expr(value, bindings)?;
            if ![
                "bigint",
                "int64",
                "integer",
                "double",
                "float",
                "real",
                "varchar",
                "string",
                "text",
                "boolean",
                "bool",
                "date",
                "timestamp",
                "decimal",
            ]
            .contains(&data_type.as_str())
                && !data_type.starts_with("decimal(")
            {
                return Err(format!("unsupported CAST type {data_type}"));
            }
        }
        Expr::InList(value, values, _) => {
            bind_expr(value, bindings)?;
            for item in values {
                bind_expr(item, bindings)?;
            }
        }
        Expr::Between(value, low, high, _) => {
            bind_expr(value, bindings)?;
            bind_expr(low, bindings)?;
            bind_expr(high, bindings)?;
        }
        Expr::Window {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                bind_expr(arg, bindings)?;
            }
            for column in partition_by {
                resolve_column(column, bindings)?;
            }
            for order in order_by {
                resolve_column(&order.key, bindings)?;
            }
        }
        Expr::InSubquery(value, _, _) => bind_expr(value, bindings)?,
        _ => {}
    }
    Ok(())
}

pub(crate) fn bind_query(
    query: &Query,
) -> Result<std::collections::HashMap<String, String>, String> {
    let bindings = query_bindings(query)?;
    for item in &query.select {
        bind_expr(&item.expr, &bindings)?;
        infer_type(&item.expr, &bindings)?;
    }
    if let Some(filter) = &query.filter {
        bind_expr(filter, &bindings)?;
        if !matches!(
            infer_type(filter, &bindings)?,
            SqlType::Bool | SqlType::Null | SqlType::Unknown
        ) {
            return Err("WHERE requires a BOOLEAN expression".into());
        }
    }
    for join in &query.joins {
        if let Some(on) = &join.on {
            bind_expr(on, &bindings)?;
            if !matches!(
                infer_type(on, &bindings)?,
                SqlType::Bool | SqlType::Null | SqlType::Unknown
            ) {
                return Err("JOIN ON requires a BOOLEAN expression".into());
            }
        }
    }
    if let Some(having) = &query.having {
        bind_expr(having, &bindings)?;
        if !matches!(
            infer_type(having, &bindings)?,
            SqlType::Bool | SqlType::Null | SqlType::Unknown
        ) {
            return Err("HAVING requires a BOOLEAN expression".into());
        }
    }
    for group in &query.group_by {
        resolve_column(group, &bindings)?;
    }
    Ok(bindings)
}

pub(crate) fn relation_scalar(catalog: &Catalog, row: RelRow, table: &str, column: &str) -> Scalar {
    match (table, column) {
        ("events", column) => row
            .event
            .map_or(Scalar::Null, |index| catalog.events.scalar(column, index)),
        ("users", "user_id") => row
            .user
            .map_or(Scalar::Null, |i| Scalar::Int(catalog.users.user_id[i])),
        ("users", "segment") => row.user.map_or(Scalar::Null, |i| {
            Scalar::Str(catalog.users.segment[i].clone())
        }),
        ("users", "signup_date") => row.user.map_or(Scalar::Null, |i| {
            Scalar::Str(catalog.users.signup_date[i].clone())
        }),
        ("users", "lifetime_value") => row.user.map_or(Scalar::Null, |i| {
            Scalar::Decimal(catalog.users.lifetime_value[i])
        }),
        ("users", "region") => row.user.map_or(Scalar::Null, |i| {
            Scalar::Str(catalog.users.region[i].clone())
        }),
        ("users", "active") => row
            .user
            .map_or(Scalar::Null, |i| Scalar::Bool(catalog.users.active[i])),
        ("campaigns", "campaign_id") => row.campaign.map_or(Scalar::Null, |i| {
            Scalar::Int(catalog.campaigns.campaign_id[i])
        }),
        ("campaigns", "campaign_name") => row.campaign.map_or(Scalar::Null, |i| {
            Scalar::Str(catalog.campaigns.campaign_name[i].clone())
        }),
        ("campaigns", "budget") => row.campaign.map_or(Scalar::Null, |i| {
            Scalar::Decimal(catalog.campaigns.budget[i])
        }),
        ("campaigns", "start_date") => row.campaign.map_or(Scalar::Null, |i| {
            Scalar::Str(catalog.campaigns.start_date[i].clone())
        }),
        ("campaigns", "end_date") => row.campaign.map_or(Scalar::Null, |i| {
            Scalar::Str(catalog.campaigns.end_date[i].clone())
        }),
        ("campaigns", "channel") => row.campaign.map_or(Scalar::Null, |i| {
            Scalar::Str(catalog.campaigns.channel[i].clone())
        }),
        _ => Scalar::Null,
    }
}
impl Table {
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
    pub(crate) fn load_binary(path: &str) -> Result<Self, String> {
        let mut f = File::open(path).map_err(|e| e.to_string())?;
        let mut magic = [0u8; 8];
        f.read_exact(&mut magic).map_err(|e| e.to_string())?;
        if &magic != b"DREMCOL1" {
            return Err("invalid column-store magic".into());
        }
        let version = read_u32(&mut f)?;
        if version != 1 {
            return Err(format!("unsupported column-store version {version}"));
        }
        let rows = read_u64(&mut f)? as usize;
        let mut t = Self::empty();
        t.event_id = read_i64s(&mut f, rows)?;
        t.user_id = read_i64s(&mut f, rows)?;
        t.timestamp = read_i64s(&mut f, rows)?;
        (t.country_dict, t.country) = read_dictionary(&mut f, rows)?;
        (t.device_dict, t.device) = read_dictionary(&mut f, rows)?;
        (t.event_dict, t.event_type) = read_dictionary(&mut f, rows)?;
        t.duration = read_i64s(&mut f, rows)?;
        t.bytes = read_i64s(&mut f, rows)?;
        t.score = read_f64s(&mut f, rows)?;
        t.success = read_bytes(&mut f, rows)?;
        t.campaign = read_i64s(&mut f, rows)?;
        t.campaign_def = read_bytes(&mut f, rows)?;
        Ok(t)
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
        self.event_id.len()
    }
    pub(crate) fn approximate_bytes(&self) -> usize {
        self.len() * (6 * 8 + 3 * 4 + 8 + 2)
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
                "country" => self.country_dict.values.len(),
                "device" => self.device_dict.values.len(),
                "event_type" => self.event_dict.values.len(),
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
            "country" => self.country[i] as u64,
            "device" => self.device[i] as u64,
            "event_type" => self.event_type[i] as u64,
            "success" => self.success[i] as u64,
            "campaign_id" => self.campaign[i] as u64,
            _ => match self.scalar(c, i) {
                Scalar::Int(v) => v as u64,
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

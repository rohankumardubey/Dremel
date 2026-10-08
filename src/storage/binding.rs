use crate::sql::*;
use arrow::datatypes::{DataType, Schema, TimeUnit};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

#[derive(Debug, Default)]
pub(crate) struct Bindings {
    aliases: HashMap<String, String>,
    columns: HashMap<String, HashMap<String, SqlType>>,
}

impl Deref for Bindings {
    type Target = HashMap<String, String>;
    fn deref(&self) -> &Self::Target {
        &self.aliases
    }
}

impl DerefMut for Bindings {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.aliases
    }
}

impl Bindings {
    fn builtin() -> Self {
        let columns = ["events", "users", "campaigns"]
            .into_iter()
            .map(|table| {
                (
                    table.into(),
                    table_columns(table)
                        .iter()
                        .map(|column| ((*column).into(), column_type(table, column)))
                        .collect(),
                )
            })
            .collect();
        Self {
            aliases: HashMap::new(),
            columns,
        }
    }

    pub(crate) fn from_schema(query: &Query, name: &str, schema: &Schema) -> Result<Self, String> {
        if query.from.name != name {
            return Err(format!(
                "unknown table {}; available table is {name}",
                query.from.name
            ));
        }
        let mut fields = HashMap::new();
        for field in schema.fields() {
            let column = field.name().to_ascii_lowercase();
            if fields
                .insert(column.clone(), arrow_sql_type(field.data_type()))
                .is_some()
            {
                return Err(format!("ambiguous schema column {column}"));
            }
        }
        Ok(Self {
            aliases: [
                (name.into(), name.into()),
                (query.from.alias.clone(), name.into()),
            ]
            .into(),
            columns: [(name.into(), fields)].into(),
        })
    }

    fn has_column(&self, table: &str, column: &str) -> bool {
        self.columns
            .get(table)
            .is_some_and(|fields| fields.contains_key(column))
    }

    pub(crate) fn column_type(&self, table: &str, column: &str) -> SqlType {
        self.columns
            .get(table)
            .and_then(|fields| fields.get(column))
            .copied()
            .unwrap_or(SqlType::Unknown)
    }
}

pub(crate) fn arrow_sql_type(data_type: &DataType) -> SqlType {
    match data_type {
        DataType::Null => SqlType::Null,
        DataType::Boolean => SqlType::Bool,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => SqlType::Int,
        DataType::Float32 | DataType::Float64 => SqlType::Double,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => SqlType::String,
        DataType::Decimal128(_, 2) => SqlType::Decimal,
        DataType::Date32 | DataType::Date64 => SqlType::Date,
        DataType::Timestamp(
            TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond,
            _,
        ) => SqlType::Timestamp,
        DataType::Dictionary(_, value) => arrow_sql_type(value),
        _ => SqlType::Unknown,
    }
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

pub(crate) fn query_bindings(query: &Query) -> Result<Bindings, String> {
    let mut bindings = Bindings::builtin();
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
    bindings: &Bindings,
) -> Result<(String, String), String> {
    if let Some((qualifier, name)) = column.split_once('.') {
        let table = bindings
            .get(qualifier)
            .ok_or_else(|| format!("unknown table or alias {qualifier}"))?;
        if !bindings.has_column(table, name) {
            return Err(format!("unknown column {column}"));
        }
        return Ok((table.clone(), name.into()));
    }
    let mut tables: Vec<_> = bindings
        .values()
        .filter(|table| bindings.has_column(table, column))
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

pub(crate) fn infer_type(expression: &Expr, bindings: &Bindings) -> Result<SqlType, String> {
    match expression {
        Expr::Null => Ok(SqlType::Null),
        Expr::Column(column) => {
            let (table, column) = resolve_column(column, bindings)?;
            Ok(bindings.column_type(&table, &column))
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

pub(crate) fn bind_expr(expression: &Expr, bindings: &Bindings) -> Result<(), String> {
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

pub(crate) fn bind_query(query: &Query) -> Result<Bindings, String> {
    let bindings = query_bindings(query)?;
    bind_query_with(query, &bindings)?;
    Ok(bindings)
}

pub(crate) fn bind_query_with(query: &Query, bindings: &Bindings) -> Result<(), String> {
    for item in &query.select {
        bind_expr(&item.expr, bindings)?;
        infer_type(&item.expr, bindings)?;
    }
    if let Some(filter) = &query.filter {
        bind_expr(filter, bindings)?;
        if !matches!(
            infer_type(filter, bindings)?,
            SqlType::Bool | SqlType::Null | SqlType::Unknown
        ) {
            return Err("WHERE requires a BOOLEAN expression".into());
        }
    }
    for join in &query.joins {
        if let Some(on) = &join.on {
            bind_expr(on, bindings)?;
            if !matches!(
                infer_type(on, bindings)?,
                SqlType::Bool | SqlType::Null | SqlType::Unknown
            ) {
                return Err("JOIN ON requires a BOOLEAN expression".into());
            }
        }
    }
    if let Some(having) = &query.having {
        bind_expr(having, bindings)?;
        if !matches!(
            infer_type(having, bindings)?,
            SqlType::Bool | SqlType::Null | SqlType::Unknown
        ) {
            return Err("HAVING requires a BOOLEAN expression".into());
        }
    }
    for group in &query.group_by {
        resolve_column(group, bindings)?;
    }
    Ok(())
}

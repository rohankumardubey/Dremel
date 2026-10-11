use super::ColumnarQuery;
use super::nested::{canonical_type, collect_paths, expression_type, reject_nested};
use crate::relational::output_columns;
use crate::sql::*;
use crate::storage::{
    Bindings, SqlType, bind_query_with, infer_type, numeric_type, resolve_column,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use std::sync::Arc;

pub(super) fn prepare(
    table_name: &str,
    schema: SchemaRef,
    sql: &str,
) -> Result<ColumnarQuery, String> {
    let table_name = table_name.to_ascii_lowercase();
    if !identifier(&table_name) {
        return Err("--table requires a SQL identifier".into());
    }
    let mut query = Parser::new(sql)?.parse()?;
    if !query.joins.is_empty() || !query.ctes.is_empty() || query.union.is_some() {
        return Err("named-table queries currently support one table; joins, CTEs, and UNION require a catalog".into());
    }
    let bindings = Bindings::from_schema(&query, &table_name, &schema)?;
    let mut select = Vec::new();
    for item in query.select {
        if matches!(item.expr, Expr::Star) {
            if item.alias.is_some() {
                return Err("SELECT * cannot have an alias".into());
            }
            for field in schema.fields() {
                let name = field.name().to_ascii_lowercase();
                if !identifier(&name) {
                    return Err(format!(
                        "column {} requires quoted identifiers, which are not supported",
                        field.name()
                    ));
                }
                select.push(SelectItem {
                    expr: Expr::Column(name),
                    alias: None,
                });
            }
        } else {
            select.push(item);
        }
    }
    if select.is_empty() {
        return Err("SELECT * requires at least one field".into());
    }
    query.select = select;
    query.plan()?;
    bind_query_with(&query, &bindings)?;
    let mut paths = std::collections::BTreeSet::new();
    for item in &query.select {
        collect_paths(&item.expr, &mut paths);
    }
    if let Some(filter) = &query.filter {
        collect_paths(filter, &mut paths);
    }
    if let Some(having) = &query.having {
        collect_paths(having, &mut paths);
    }
    paths.extend(query.group_by.iter().cloned());
    query.columns = paths.into_iter().collect();
    for item in &query.select {
        validate(&item.expr, &bindings, false, None)?;
        if query.distinct {
            reject_nested(&item.expr, &bindings)?;
        }
    }
    for group in &query.group_by {
        reject_nested(&Expr::Column(group.clone()), &bindings)?;
    }
    if let Some(filter) = &query.filter {
        if contains_agg(filter) {
            return Err("aggregate functions are not allowed in WHERE".into());
        }
        validate(filter, &bindings, false, None)?;
    }
    if let Some(having) = &query.having {
        if query.group_by.is_empty()
            && !query.select.iter().any(|item| contains_agg(&item.expr))
            && !contains_agg(having)
        {
            return Err("HAVING requires aggregation or GROUP BY".into());
        }
        validate(having, &bindings, false, Some(&query.group_by))?;
    }
    let names = output_columns(&query);
    for order in &query.order_by {
        let count = query.select.iter().filter(|item| {
            item.alias.as_ref() == Some(&order.key)
                || matches!(&item.expr, Expr::Column(column) if column == &order.key || column.rsplit('.').next() == Some(order.key.as_str()))
        }).count();
        if count != 1 {
            return Err(format!(
                "ORDER BY {} must resolve to one projected column or alias",
                order.key
            ));
        }
        let item = query.select.iter().find(|item| {
            item.alias.as_ref() == Some(&order.key)
                || matches!(&item.expr, Expr::Column(column) if column == &order.key || column.rsplit('.').next() == Some(order.key.as_str()))
        }).expect("one matching order expression");
        reject_nested(&item.expr, &bindings)?;
    }
    let fields = query
        .select
        .iter()
        .zip(names)
        .map(|(item, name)| {
            Ok(Field::new(
                name,
                expression_type(&item.expr, &bindings)?,
                true,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let aggregate = query.select.iter().any(|item| contains_agg(&item.expr))
        || query.having.as_ref().is_some_and(contains_agg)
        || !query.group_by.is_empty();
    query.physical = vec![format!(
        "ArrowScanExec(table={table_name};columns=[{}])",
        query.columns.join(",")
    )];
    if query.filter.is_some() {
        query.physical.push("FilterExec".into());
    }
    query.physical.push(
        if aggregate {
            "HashAggregateExec"
        } else {
            "ProjectExec"
        }
        .into(),
    );
    if query.having.is_some() {
        query.physical.push("HavingExec".into());
    }
    if query.distinct {
        query.physical.push("HashDistinctExec".into());
    }
    if !query.order_by.is_empty() {
        query.physical.push("SortExec".into());
    }
    if query.limit.is_some() || query.offset > 0 {
        query.physical.push("LimitExec".into());
    }
    Ok(ColumnarQuery {
        column_paths: query
            .columns
            .iter()
            .map(|column| Ok((column.clone(), resolve_column(column, &bindings)?.1)))
            .collect::<Result<_, String>>()?,
        query,
        input_schema: schema,
        output_schema: Arc::new(Schema::new(fields)),
    })
}

fn identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

pub(super) fn output_type(kind: SqlType) -> Result<DataType, String> {
    Ok(match kind {
        SqlType::Null => DataType::Null,
        SqlType::Bool => DataType::Boolean,
        SqlType::Int | SqlType::Interval => DataType::Int64,
        SqlType::Double => DataType::Float64,
        SqlType::Decimal => DataType::Decimal128(18, 2),
        SqlType::String => DataType::Utf8,
        SqlType::Date => DataType::Date32,
        SqlType::Timestamp => DataType::Timestamp(TimeUnit::Second, None),
        SqlType::Unknown | SqlType::Struct | SqlType::List => {
            return Err("expression has an unsupported SQL type".into());
        }
    })
}

fn validate(
    expression: &Expr,
    bindings: &Bindings,
    inside_aggregate: bool,
    groups: Option<&[String]>,
) -> Result<(), String> {
    let child = |expr: &Expr| validate(expr, bindings, inside_aggregate, groups);
    match expression {
        Expr::Column(column) => {
            let (table, name) = resolve_column(column, bindings)?;
            if !inside_aggregate
                && let Some(groups) = groups
                && !groups.iter().any(|group| {
                    resolve_column(group, bindings)
                        .is_ok_and(|resolved| resolved == (table.clone(), name.clone()))
                })
            {
                return Err(format!("HAVING column {column} must be grouped"));
            }
            if bindings.column_type(&table, &name) == SqlType::Unknown {
                return Err(format!("column {column} has an unsupported SQL type"));
            }
            canonical_type(bindings.arrow_type(&name).ok_or("missing Arrow type")?)?;
        }
        Expr::Func(name, argument) => validate_function(
            name,
            &[argument.as_ref()],
            bindings,
            inside_aggregate,
            groups,
        )?,
        Expr::Call(name, arguments) => validate_function(
            name,
            &arguments.iter().collect::<Vec<_>>(),
            bindings,
            inside_aggregate,
            groups,
        )?,
        Expr::Cast(value, data_type) => {
            reject_nested(value, bindings)?;
            if data_type.starts_with("decimal(") && data_type != "decimal(18,2)" {
                return Err("only DECIMAL(18,2) is supported".into());
            }
            child(value)?;
        }
        Expr::Unary(_, value) => {
            reject_nested(value, bindings)?;
            child(value)?;
        }
        Expr::IsNull(value, _) => child(value)?,
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            reject_nested(left, bindings)?;
            reject_nested(right, bindings)?;
            child(left)?;
            child(right)?;
        }
        Expr::Case(branches, fallback) => {
            expression_type(expression, bindings)?;
            for (condition, value) in branches {
                child(condition)?;
                child(value)?;
            }
            child(fallback)?;
        }
        Expr::InList(value, candidates, _) => {
            reject_nested(value, bindings)?;
            child(value)?;
            for candidate in candidates {
                reject_nested(candidate, bindings)?;
                child(candidate)?;
            }
        }
        Expr::Between(value, low, high, _) => {
            for value in [value, low, high] {
                reject_nested(value, bindings)?;
            }
            child(value)?;
            child(low)?;
            child(high)?;
        }
        Expr::Window { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::InSubquery(_, _, _) => {
            return Err("windows and subqueries are not supported for named-table queries".into());
        }
        _ => {}
    }
    Ok(())
}

fn validate_function(
    name: &str,
    arguments: &[&Expr],
    bindings: &Bindings,
    inside_aggregate: bool,
    groups: Option<&[String]>,
) -> Result<(), String> {
    let aggregate = ["count", "sum", "avg", "min", "max"].contains(&name);
    if aggregate && inside_aggregate {
        return Err("nested aggregate functions are not supported".into());
    }
    let arity = arguments.len();
    let valid = match name {
        "count" | "sum" | "avg" | "min" | "max" | "lower" | "upper" | "length" | "abs" | "date"
        | "timestamp" | "interval" | "cardinality" => arity == 1,
        "nullif" | "extract" | "date_trunc" => arity == 2,
        "substring" => (2..=3).contains(&arity),
        "concat" | "coalesce" => arity > 0,
        _ => return Err(format!("unsupported function {name}")),
    };
    if !valid {
        return Err(format!("invalid argument count for {name}"));
    }
    for argument in arguments {
        if !["count", "coalesce", "cardinality"].contains(&name) {
            reject_nested(argument, bindings)?;
        }
        if matches!(argument, Expr::Star) && name != "count" {
            return Err(format!("{name}(*) is unsupported"));
        }
        validate(argument, bindings, inside_aggregate || aggregate, groups)?;
    }
    let kind = infer_type(arguments[0], bindings)?;
    if name == "cardinality" && !matches!(kind, SqlType::List | SqlType::Null) {
        return Err("CARDINALITY requires a LIST argument".into());
    }
    if name == "coalesce" {
        expression_type(
            &Expr::Call(
                name.into(),
                arguments.iter().map(|arg| (*arg).clone()).collect(),
            ),
            bindings,
        )?;
    }
    if ["sum", "avg", "abs"].contains(&name) && !numeric_type(kind) && kind != SqlType::Null {
        return Err(format!("{name} requires a numeric argument"));
    }
    if [
        "lower",
        "upper",
        "length",
        "substring",
        "extract",
        "date_trunc",
        "interval",
    ]
    .contains(&name)
        && !matches!(kind, SqlType::String | SqlType::Null)
    {
        return Err(format!("{name} requires a string first argument"));
    }
    if name == "substring"
        && arguments[1..]
            .iter()
            .any(|arg| !matches!(infer_type(arg, bindings), Ok(SqlType::Int | SqlType::Null)))
    {
        return Err("SUBSTRING requires integer positions".into());
    }
    Ok(())
}

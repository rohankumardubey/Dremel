use super::bind::output_type;
use crate::sql::Expr;
use crate::storage::{Bindings, SqlType, arrow_sql_type, infer_type, resolve_column};
use arrow::datatypes::{DataType, Fields};
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::sync::Arc;

pub(super) fn collect_paths(expression: &Expr, paths: &mut BTreeSet<String>) {
    match expression {
        Expr::Column(path) => {
            paths.insert(path.clone());
        }
        Expr::Unary(_, value)
        | Expr::Func(_, value)
        | Expr::IsNull(value, _)
        | Expr::Cast(value, _) => collect_paths(value, paths),
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            collect_paths(left, paths);
            collect_paths(right, paths);
        }
        Expr::Call(_, arguments) => arguments
            .iter()
            .for_each(|value| collect_paths(value, paths)),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                collect_paths(condition, paths);
                collect_paths(value, paths);
            }
            collect_paths(fallback, paths);
        }
        Expr::InList(value, candidates, _) => {
            collect_paths(value, paths);
            candidates
                .iter()
                .for_each(|value| collect_paths(value, paths));
        }
        Expr::Between(value, low, high, _) => {
            for value in [value, low, high] {
                collect_paths(value, paths);
            }
        }
        _ => {}
    }
}

pub(super) fn is_nested(kind: SqlType) -> bool {
    matches!(kind, SqlType::Struct | SqlType::List)
}

pub(super) fn canonical_type(data_type: &DataType) -> Result<DataType, String> {
    Ok(match data_type {
        DataType::Struct(fields) => {
            let mut names = HashSet::new();
            let children = fields
                .iter()
                .map(|field| {
                    if !names.insert(field.name().to_ascii_lowercase()) {
                        return Err(format!("ambiguous nested field {}", field.name()));
                    }
                    Ok(Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_data_type(canonical_type(field.data_type())?),
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?;
            DataType::Struct(Fields::from(children))
        }
        DataType::List(element) => DataType::List(Arc::new(
            element
                .as_ref()
                .clone()
                .with_data_type(canonical_type(element.data_type())?),
        )),
        DataType::LargeList(element) => DataType::LargeList(Arc::new(
            element
                .as_ref()
                .clone()
                .with_data_type(canonical_type(element.data_type())?),
        )),
        DataType::FixedSizeList(element, size) => DataType::FixedSizeList(
            Arc::new(
                element
                    .as_ref()
                    .clone()
                    .with_data_type(canonical_type(element.data_type())?),
            ),
            *size,
        ),
        DataType::Dictionary(_, value) => {
            if is_nested(arrow_sql_type(value)) {
                return Err("nested dictionary values are unsupported".into());
            }
            canonical_type(value)?
        }
        _ => output_type(arrow_sql_type(data_type))?,
    })
}

pub(super) fn expression_type(expression: &Expr, bindings: &Bindings) -> Result<DataType, String> {
    match expression {
        Expr::Column(column) => {
            let (table, path) = resolve_column(column, bindings)?;
            canonical_type(
                bindings
                    .arrow_type(&table, &path)
                    .ok_or("missing Arrow column type")?,
            )
        }
        Expr::Func(name, argument) if ["coalesce", "min", "max"].contains(&name.as_str()) => {
            expression_type(argument, bindings)
        }
        Expr::Call(name, arguments) if name == "nullif" => expression_type(&arguments[0], bindings),
        Expr::Call(name, arguments) if name == "coalesce" => merge_types(
            arguments
                .iter()
                .map(|argument| expression_type(argument, bindings)),
            expression,
            bindings,
        ),
        Expr::Case(branches, fallback) => merge_types(
            branches
                .iter()
                .map(|(_, value)| expression_type(value, bindings))
                .chain(std::iter::once(expression_type(fallback, bindings))),
            expression,
            bindings,
        ),
        _ => output_type(infer_type(expression, bindings)?),
    }
}

fn merge_types(
    types: impl Iterator<Item = Result<DataType, String>>,
    expression: &Expr,
    bindings: &Bindings,
) -> Result<DataType, String> {
    let mut selected = DataType::Null;
    for kind in types {
        let kind = kind?;
        if kind == DataType::Null {
            continue;
        }
        if selected == DataType::Null {
            selected = kind;
        } else if selected != kind
            && (is_nested(arrow_sql_type(&selected)) || is_nested(arrow_sql_type(&kind)))
        {
            return Err(
                "nested CASE/COALESCE branches must have identical normalized field layouts".into(),
            );
        }
    }
    if is_nested(arrow_sql_type(&selected)) {
        Ok(selected)
    } else {
        output_type(infer_type(expression, bindings)?)
    }
}

pub(super) fn reject_nested(expression: &Expr, bindings: &Bindings) -> Result<(), String> {
    if is_nested(infer_type(expression, bindings)?) {
        Err("operator requires scalar operands; container arithmetic, string functions, and casting are unsupported".into())
    } else {
        Ok(())
    }
}

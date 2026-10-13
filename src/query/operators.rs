use super::nested::{expression_type, is_nested};
use crate::sql::Expr;
use crate::storage::{Bindings, infer_type};
use arrow::datatypes::DataType;

/// No implicit coercion within containers. Physical list offset widths and
/// primitive widths may differ, but their normalized logical layouts must agree.
pub(super) fn comparable(left: &Expr, right: &Expr, bindings: &Bindings) -> Result<(), String> {
    if (is_nested(infer_type(left, bindings)?) || is_nested(infer_type(right, bindings)?))
        && !same_layout(
            &expression_type(left, bindings)?,
            &expression_type(right, bindings)?,
        )
    {
        return Err("nested operands require matching normalized field layouts; no implicit container coercion".into());
    }
    Ok(())
}

fn element(kind: &DataType) -> Option<&DataType> {
    match kind {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Some(field.data_type())
        }
        _ => None,
    }
}

fn same_layout(left: &DataType, right: &DataType) -> bool {
    if left == &DataType::Null || right == &DataType::Null {
        return true;
    }
    if let (Some(left), Some(right)) = (element(left), element(right)) {
        return same_layout(left, right);
    }
    match (left, right) {
        (DataType::Struct(left), DataType::Struct(right)) => {
            left.len() == right.len()
                && left.iter().zip(right).all(|(left, right)| {
                    left.name().eq_ignore_ascii_case(right.name())
                        && same_layout(left.data_type(), right.data_type())
                })
        }
        _ => left == right,
    }
}

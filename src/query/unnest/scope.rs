//! Freeze ON-column scope before subsequent relations introduce more names.
use crate::sql::Expr;
use crate::storage::{Bindings, resolve_column};

pub(super) fn qualify(expr: &mut Expr, bindings: &Bindings) -> Result<(), String> {
    match expr {
        Expr::Column(name) => {
            let (table, column) = resolve_column(name, bindings)?;
            *name = format!("{table}.{column}");
        }
        Expr::Unary(_, value)
        | Expr::Func(_, value)
        | Expr::IsNull(value, _)
        | Expr::Cast(value, _) => qualify(value, bindings)?,
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            qualify(left, bindings)?;
            qualify(right, bindings)?;
        }
        Expr::Call(_, arguments) => {
            for value in arguments {
                qualify(value, bindings)?;
            }
        }
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                qualify(condition, bindings)?;
                qualify(value, bindings)?;
            }
            qualify(fallback, bindings)?;
        }
        Expr::InList(value, candidates, _) => {
            qualify(value, bindings)?;
            for value in candidates {
                qualify(value, bindings)?;
            }
        }
        Expr::Between(value, low, high, _) => {
            for value in [value, low, high] {
                qualify(value, bindings)?;
            }
        }
        _ => {}
    }
    Ok(())
}

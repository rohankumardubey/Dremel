use super::*;

pub(crate) fn is_agg(e: &Expr) -> bool {
    matches!(e,Expr::Func(n,_) if ["count","sum","avg","min","max"].contains(&n.as_str()))
}
pub(crate) fn contains_agg(e: &Expr) -> bool {
    if is_agg(e) {
        return true;
    }
    match e {
        Expr::Unary(_, value) | Expr::Func(_, value) | Expr::IsNull(value, _) => {
            contains_agg(value)
        }
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            contains_agg(left) || contains_agg(right)
        }
        Expr::Call(_, args) => args.iter().any(contains_agg),
        Expr::Case(branches, fallback) => {
            branches
                .iter()
                .any(|(condition, value)| contains_agg(condition) || contains_agg(value))
                || contains_agg(fallback)
        }
        Expr::Cast(value, _) => contains_agg(value),
        Expr::InList(value, values, _) => contains_agg(value) || values.iter().any(contains_agg),
        Expr::Between(value, low, high, _) => {
            contains_agg(value) || contains_agg(low) || contains_agg(high)
        }
        Expr::Window { .. } => false,
        _ => false,
    }
}
pub(crate) fn contains_window(expression: &Expr) -> bool {
    match expression {
        Expr::Window { .. } => true,
        Expr::Unary(_, value) | Expr::Func(_, value) | Expr::IsNull(value, _) => {
            contains_window(value)
        }
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            contains_window(left) || contains_window(right)
        }
        Expr::Call(_, args) => args.iter().any(contains_window),
        Expr::Case(branches, fallback) => {
            branches
                .iter()
                .any(|(condition, value)| contains_window(condition) || contains_window(value))
                || contains_window(fallback)
        }
        Expr::Cast(value, _) => contains_window(value),
        Expr::InList(value, values, _) => {
            contains_window(value) || values.iter().any(contains_window)
        }
        Expr::Between(value, low, high, _) => {
            contains_window(value) || contains_window(low) || contains_window(high)
        }
        _ => false,
    }
}
pub(crate) fn contains_subquery(expression: &Expr) -> bool {
    match expression {
        Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_, _, _) => true,
        Expr::Unary(_, value) | Expr::Func(_, value) | Expr::IsNull(value, _) => {
            contains_subquery(value)
        }
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            contains_subquery(left) || contains_subquery(right)
        }
        Expr::Call(_, args) => args.iter().any(contains_subquery),
        Expr::Case(branches, fallback) => {
            branches
                .iter()
                .any(|(condition, value)| contains_subquery(condition) || contains_subquery(value))
                || contains_subquery(fallback)
        }
        Expr::Cast(value, _) => contains_subquery(value),
        Expr::InList(value, values, _) => {
            contains_subquery(value) || values.iter().any(contains_subquery)
        }
        Expr::Between(value, low, high, _) => {
            contains_subquery(value) || contains_subquery(low) || contains_subquery(high)
        }
        Expr::Window { args, .. } => args.iter().any(contains_subquery),
        _ => false,
    }
}
pub(crate) fn collect(e: &Expr, s: &mut BTreeSet<String>) {
    match e {
        Expr::Column(c) => {
            s.insert(c.rsplit('.').next().unwrap_or(c).to_string());
        }
        Expr::Unary(_, a) | Expr::Func(_, a) | Expr::IsNull(a, _) => collect(a, s),
        Expr::Call(_, args) => args.iter().for_each(|arg| collect(arg, s)),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                collect(condition, s);
                collect(value, s);
            }
            collect(fallback, s);
        }
        Expr::Cast(value, _) => collect(value, s),
        Expr::InList(value, values, _) => {
            collect(value, s);
            values.iter().for_each(|item| collect(item, s));
        }
        Expr::Between(value, low, high, _) => {
            collect(value, s);
            collect(low, s);
            collect(high, s);
        }
        Expr::Like(value, pattern, _) => {
            collect(value, s);
            collect(pattern, s);
        }
        Expr::Window {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().for_each(|arg| collect(arg, s));
            partition_by.iter().for_each(|column| {
                s.insert(column.rsplit('.').next().unwrap_or(column).into());
            });
            order_by.iter().for_each(|order| {
                s.insert(order.key.rsplit('.').next().unwrap_or(&order.key).into());
            });
        }
        Expr::InSubquery(value, _, _) => collect(value, s),
        Expr::Binary(_, a, b) => {
            collect(a, s);
            collect(b, s)
        }
        Expr::DictEq(c, _, _) => {
            s.insert(c.clone());
        }
        _ => {}
    }
}

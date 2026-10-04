use super::*;

pub(crate) fn prepare_expr(e: &mut Expr, t: &Table) {
    match e {
        Expr::Binary(op, a, b) if (op == "=" || op == "!=") => {
            prepare_expr(a, t);
            prepare_expr(b, t);
            let replacement = match (&**a, &**b) {
                (Expr::Column(c), Expr::String(s)) => t
                    .dict_id(c, s)
                    .map(|id| Expr::DictEq(c.clone(), id, op == "!=")),
                (Expr::String(s), Expr::Column(c)) => t
                    .dict_id(c, s)
                    .map(|id| Expr::DictEq(c.clone(), id, op == "!=")),
                _ => None,
            };
            if let Some(x) = replacement {
                *e = x
            }
        }
        Expr::Binary(_, a, b) => {
            prepare_expr(a, t);
            prepare_expr(b, t)
        }
        Expr::Unary(_, a) | Expr::Func(_, a) | Expr::IsNull(a, _) => prepare_expr(a, t),
        Expr::Call(_, args) => args.iter_mut().for_each(|arg| prepare_expr(arg, t)),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                prepare_expr(condition, t);
                prepare_expr(value, t);
            }
            prepare_expr(fallback, t);
        }
        Expr::Cast(value, _) => prepare_expr(value, t),
        Expr::InList(value, values, _) => {
            prepare_expr(value, t);
            values.iter_mut().for_each(|item| prepare_expr(item, t));
        }
        Expr::Between(value, low, high, _) => {
            prepare_expr(value, t);
            prepare_expr(low, t);
            prepare_expr(high, t);
        }
        Expr::Like(value, pattern, _) => {
            prepare_expr(value, t);
            prepare_expr(pattern, t);
        }
        Expr::Window { args, .. } => {
            args.iter_mut()
                .for_each(|argument| prepare_expr(argument, t));
        }
        Expr::InSubquery(value, _, _) => prepare_expr(value, t),
        _ => {}
    }
}

pub(crate) fn literal_value(expression: &Expr) -> Option<Scalar> {
    match expression {
        Expr::Null => Some(Scalar::Null),
        Expr::Int(value) => Some(Scalar::Int(*value)),
        Expr::Float(value) => Some(Scalar::Float(*value)),
        Expr::Bool(value) => Some(Scalar::Bool(*value)),
        Expr::String(value) => Some(Scalar::Str(value.clone())),
        _ => None,
    }
}

pub(crate) fn literal_expression(value: Scalar) -> Expr {
    match value {
        Scalar::Null => Expr::Null,
        Scalar::Int(value) => Expr::Int(value),
        Scalar::Decimal(value) => Expr::Cast(
            Box::new(Expr::String(decimal_text(value))),
            "decimal(18,2)".into(),
        ),
        Scalar::Float(value) => Expr::Float(value),
        Scalar::Bool(value) => Expr::Bool(value),
        Scalar::Str(value) => Expr::String(value),
    }
}

/// Fold immutable scalar expressions and apply identities that are valid under
/// SQL three-valued logic. Returns the number of rewritten AST nodes.
pub(crate) fn optimize_expression(expression: &mut Expr) -> usize {
    let mut rewrites = 0;
    match expression {
        Expr::Unary(_, value) | Expr::Func(_, value) | Expr::IsNull(value, _) => {
            rewrites += optimize_expression(value);
        }
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            rewrites += optimize_expression(left);
            rewrites += optimize_expression(right);
        }
        Expr::Call(_, arguments) => {
            rewrites += arguments.iter_mut().map(optimize_expression).sum::<usize>();
        }
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                rewrites += optimize_expression(condition);
                rewrites += optimize_expression(value);
            }
            rewrites += optimize_expression(fallback);
        }
        Expr::Cast(value, _) => rewrites += optimize_expression(value),
        Expr::InList(value, candidates, _) => {
            rewrites += optimize_expression(value);
            rewrites += candidates
                .iter_mut()
                .map(optimize_expression)
                .sum::<usize>();
        }
        Expr::Between(value, low, high, _) => {
            rewrites += optimize_expression(value);
            rewrites += optimize_expression(low);
            rewrites += optimize_expression(high);
        }
        Expr::Window { args, .. } => {
            rewrites += args.iter_mut().map(optimize_expression).sum::<usize>();
        }
        Expr::InSubquery(value, _, _) => rewrites += optimize_expression(value),
        _ => {}
    }

    let replacement = match expression {
        Expr::Unary(op, value) => literal_value(value).map(|value| {
            if op == "not" {
                literal_expression(
                    value
                        .sql_bool()
                        .map_or(Scalar::Null, |boolean| Scalar::Bool(!boolean)),
                )
            } else {
                literal_expression(match value {
                    Scalar::Int(value) => value.checked_neg().map_or(Scalar::Null, Scalar::Int),
                    Scalar::Decimal(value) => {
                        value.checked_neg().map_or(Scalar::Null, Scalar::Decimal)
                    }
                    Scalar::Float(value) => Scalar::Float(-value),
                    _ => Scalar::Null,
                })
            }
        }),
        Expr::Binary(op, left, right) => {
            let left_value = literal_value(left);
            let right_value = literal_value(right);
            if let (Some(left), Some(right)) = (left_value.clone(), right_value.clone()) {
                Some(literal_expression(apply_binary(op, left, right)))
            } else {
                match (op.as_str(), left_value.as_ref(), right_value.as_ref()) {
                    ("and", Some(Scalar::Bool(true)), _) => Some((**right).clone()),
                    ("and", _, Some(Scalar::Bool(true))) => Some((**left).clone()),
                    ("and", Some(Scalar::Bool(false)), _)
                    | ("and", _, Some(Scalar::Bool(false))) => Some(Expr::Bool(false)),
                    ("or", Some(Scalar::Bool(false)), _) => Some((**right).clone()),
                    ("or", _, Some(Scalar::Bool(false))) => Some((**left).clone()),
                    ("or", Some(Scalar::Bool(true)), _) | ("or", _, Some(Scalar::Bool(true))) => {
                        Some(Expr::Bool(true))
                    }
                    _ => None,
                }
            }
        }
        Expr::Func(name, value) if !matches!(name.as_str(), "date" | "timestamp" | "interval") => {
            literal_value(value).map(|value| literal_expression(eval_values(name, vec![value])))
        }
        Expr::Call(name, arguments)
            if !matches!(
                name.as_str(),
                "date" | "timestamp" | "interval" | "date_trunc" | "extract"
            ) && arguments
                .iter()
                .all(|argument| literal_value(argument).is_some()) =>
        {
            Some(literal_expression(eval_values(
                name,
                arguments.iter().filter_map(literal_value).collect(),
            )))
        }
        Expr::Cast(value, data_type) => {
            literal_value(value).map(|value| literal_expression(cast_value(data_type, value)))
        }
        Expr::IsNull(value, negated) => {
            literal_value(value).map(|value| Expr::Bool(matches!(value, Scalar::Null) ^ *negated))
        }
        Expr::Between(value, low, high, negated) => {
            match (
                literal_value(value),
                literal_value(low),
                literal_value(high),
            ) {
                (Some(value), Some(low), Some(high)) => {
                    let result = apply_binary(
                        "and",
                        apply_binary(">=", value.clone(), low),
                        apply_binary("<=", value, high),
                    );
                    Some(literal_expression(if *negated {
                        result
                            .sql_bool()
                            .map_or(Scalar::Null, |boolean| Scalar::Bool(!boolean))
                    } else {
                        result
                    }))
                }
                _ => None,
            }
        }
        Expr::Like(value, pattern, negated) => match (literal_value(value), literal_value(pattern))
        {
            (Some(Scalar::Str(value)), Some(Scalar::Str(pattern))) => {
                Some(Expr::Bool(like_matches(&value, &pattern) ^ *negated))
            }
            (Some(Scalar::Null), _) | (_, Some(Scalar::Null)) => Some(Expr::Null),
            _ => None,
        },
        Expr::Case(branches, fallback) => {
            let mut replacement = None;
            let mut all_constant_false_or_null = true;
            for (condition, value) in branches.iter() {
                match literal_value(condition) {
                    Some(Scalar::Bool(true)) => {
                        replacement = Some(value.clone());
                        break;
                    }
                    Some(Scalar::Bool(false) | Scalar::Null) => {}
                    _ => {
                        all_constant_false_or_null = false;
                        break;
                    }
                }
            }
            if replacement.is_some() {
                replacement
            } else if all_constant_false_or_null {
                Some((**fallback).clone())
            } else {
                None
            }
        }
        _ => None,
    };
    if let Some(replacement) = replacement {
        *expression = replacement;
        rewrites += 1;
    }
    rewrites
}

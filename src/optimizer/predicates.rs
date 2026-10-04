use super::*;

fn comparison_constraint(expression: &Expr) -> Option<(String, String, i64)> {
    let Expr::Binary(operator, left, right) = expression else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (Expr::Column(column), Expr::Int(value)) => {
            Some((column.clone(), operator.clone(), *value))
        }
        (Expr::Int(value), Expr::Column(column)) => {
            let flipped = match operator.as_str() {
                "<" => ">",
                "<=" => ">=",
                ">" => "<",
                ">=" => "<=",
                value => value,
            };
            Some((column.clone(), flipped.into(), *value))
        }
        _ => None,
    }
}

pub(super) fn contradictory_filter(expression: &Expr) -> bool {
    #[derive(Default)]
    struct Bounds {
        equal: Option<i64>,
        lower: Option<(i64, bool)>,
        upper: Option<(i64, bool)>,
    }
    let mut conjuncts = Vec::new();
    split_conjuncts(expression, &mut conjuncts);
    if conjuncts
        .iter()
        .any(|value| matches!(value, Expr::Bool(false) | Expr::Null))
    {
        return true;
    }
    let mut columns = HashMap::<String, Bounds>::new();
    for conjunct in conjuncts {
        let Some((column, operator, value)) = comparison_constraint(conjunct) else {
            continue;
        };
        let bounds = columns.entry(column).or_default();
        match operator.as_str() {
            "=" => {
                if bounds.equal.is_some_and(|existing| existing != value) {
                    return true;
                }
                bounds.equal = Some(value);
            }
            ">" | ">=" => {
                let candidate = (value, operator == ">=");
                if bounds.lower.is_none_or(|current| {
                    candidate.0 > current.0
                        || (candidate.0 == current.0 && !candidate.1 && current.1)
                }) {
                    bounds.lower = Some(candidate);
                }
            }
            "<" | "<=" => {
                let candidate = (value, operator == "<=");
                if bounds.upper.is_none_or(|current| {
                    candidate.0 < current.0
                        || (candidate.0 == current.0 && !candidate.1 && current.1)
                }) {
                    bounds.upper = Some(candidate);
                }
            }
            _ => {}
        }
    }
    columns.into_values().any(|bounds| {
        if let Some(equal) = bounds.equal
            && (bounds
                .lower
                .is_some_and(|(value, inclusive)| equal < value || (equal == value && !inclusive))
                || bounds.upper.is_some_and(|(value, inclusive)| {
                    equal > value || (equal == value && !inclusive)
                }))
        {
            return true;
        }
        bounds.lower.zip(bounds.upper).is_some_and(
            |((lower, lower_inclusive), (upper, upper_inclusive))| {
                lower > upper || (lower == upper && !(lower_inclusive && upper_inclusive))
            },
        )
    })
}

fn predicate_rank(expression: &Expr) -> usize {
    match expression {
        Expr::DictEq(_, _, _) => 0,
        Expr::Binary(operator, left, right)
            if ["=", "!=", "<", "<=", ">", ">="].contains(&operator.as_str())
                && (literal_value(left).is_some() || literal_value(right).is_some()) =>
        {
            let non_literal = if literal_value(left).is_some() {
                right.as_ref()
            } else {
                left.as_ref()
            };
            if matches!(non_literal, Expr::Column(_)) {
                1
            } else {
                4
            }
        }
        Expr::IsNull(_, _) | Expr::Between(_, _, _, _) | Expr::InList(_, _, _) => 2,
        Expr::Like(_, _, _) | Expr::Func(_, _) | Expr::Call(_, _) => 4,
        _ => 3,
    }
}

pub(super) fn reorder_conjuncts(filter: &mut Expr) -> bool {
    let original = format!("{filter:?}");
    let mut conjuncts = Vec::new();
    owned_conjuncts(filter.clone(), &mut conjuncts);
    if conjuncts.len() < 2 {
        return false;
    }
    conjuncts.sort_by_key(predicate_rank);
    let mut values = conjuncts.into_iter();
    let mut rebuilt = values.next().expect("non-empty conjuncts");
    for value in values {
        rebuilt = Expr::Binary("and".into(), Box::new(rebuilt), Box::new(value));
    }
    let changed = original != format!("{rebuilt:?}");
    *filter = rebuilt;
    changed
}

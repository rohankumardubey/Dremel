use super::*;

pub(crate) fn collect_aggregates(expression: &Expr, output: &mut Vec<Expr>) {
    if is_agg(expression) {
        let key = format!("{expression:?}");
        if !output.iter().any(|existing| format!("{existing:?}") == key) {
            output.push(expression.clone());
        }
        return;
    }
    match expression {
        Expr::Unary(_, value) | Expr::Func(_, value) | Expr::IsNull(value, _) => {
            collect_aggregates(value, output)
        }
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            collect_aggregates(left, output);
            collect_aggregates(right, output);
        }
        Expr::Call(_, values) => values
            .iter()
            .for_each(|value| collect_aggregates(value, output)),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                collect_aggregates(condition, output);
                collect_aggregates(value, output);
            }
            collect_aggregates(fallback, output);
        }
        Expr::Cast(value, _) => collect_aggregates(value, output),
        Expr::InList(value, values, _) => {
            collect_aggregates(value, output);
            values
                .iter()
                .for_each(|item| collect_aggregates(item, output));
        }
        Expr::Between(value, low, high, _) => {
            collect_aggregates(value, output);
            collect_aggregates(low, output);
            collect_aggregates(high, output);
        }
        _ => {}
    }
}

pub(crate) fn aggregate_state(expression: &Expr) -> AggState {
    match expression {
        Expr::Func(name, _) if name == "count" => AggState::Count(0),
        Expr::Func(name, _) if name == "sum" => AggState::Sum(SumState::new()),
        Expr::Func(name, _) if name == "avg" => AggState::Avg { sum: 0.0, count: 0 },
        Expr::Func(name, _) if name == "min" => AggState::Min(None),
        Expr::Func(_, _) => AggState::Max(None),
        _ => unreachable!(),
    }
}

pub(crate) fn update_rel_aggregate(
    state: &mut AggState,
    expression: &Expr,
    catalog: &Catalog,
    row: RelRow,
    bindings: &Bindings,
) {
    let Expr::Func(_, argument) = expression else {
        unreachable!()
    };
    let value = eval_rel(argument, catalog, row, bindings);
    match state {
        AggState::Count(count) => {
            if matches!(argument.as_ref(), Expr::Star) || !matches!(value, Scalar::Null) {
                *count += 1;
            }
        }
        AggState::Sum(sum) => sum.add(&value),
        AggState::Avg { sum, count } => {
            if let Some(number) = value.number() {
                *sum += number;
                *count += 1;
            }
        }
        AggState::Min(current) => {
            if !matches!(value, Scalar::Null)
                && current
                    .as_ref()
                    .is_none_or(|old| cmp(&value, old) == Some(Ordering::Less))
            {
                *current = Some(value);
            }
        }
        AggState::Max(current) => {
            if !matches!(value, Scalar::Null)
                && current
                    .as_ref()
                    .is_none_or(|old| cmp(&value, old) == Some(Ordering::Greater))
            {
                *current = Some(value);
            }
        }
    }
}

pub(crate) fn eval_group_expr(
    expression: &Expr,
    catalog: &Catalog,
    row: RelRow,
    bindings: &Bindings,
    aggregates: &[Expr],
    values: &[Scalar],
) -> Scalar {
    if is_agg(expression) {
        let key = format!("{expression:?}");
        return aggregates
            .iter()
            .position(|candidate| format!("{candidate:?}") == key)
            .map_or(Scalar::Null, |index| values[index].clone());
    }
    if !contains_agg(expression) {
        return eval_rel(expression, catalog, row, bindings);
    }
    let evaluate =
        |value: &Expr| eval_group_expr(value, catalog, row, bindings, aggregates, values);
    match expression {
        Expr::Unary(operator, value) => {
            let value = evaluate(value);
            if operator == "not" {
                value
                    .sql_bool()
                    .map_or(Scalar::Null, |value| Scalar::Bool(!value))
            } else {
                match value {
                    Scalar::Int(value) => value.checked_neg().map_or(Scalar::Null, Scalar::Int),
                    Scalar::Decimal(value) => {
                        value.checked_neg().map_or(Scalar::Null, Scalar::Decimal)
                    }
                    Scalar::Float(value) => Scalar::Float(-value),
                    _ => Scalar::Null,
                }
            }
        }
        Expr::Binary(operator, left, right) => {
            apply_binary(operator, evaluate(left), evaluate(right))
        }
        Expr::Func(name, argument) => eval_values(name, vec![evaluate(argument)]),
        Expr::Call(name, arguments) => eval_values(name, arguments.iter().map(evaluate).collect()),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                if evaluate(condition).sql_bool() == Some(true) {
                    return evaluate(value);
                }
            }
            evaluate(fallback)
        }
        Expr::Cast(value, data_type) => cast_value(data_type, evaluate(value)),
        Expr::IsNull(value, negated) => {
            Scalar::Bool(matches!(evaluate(value), Scalar::Null) ^ *negated)
        }
        Expr::InList(value, candidates, negated) => {
            let value = evaluate(value);
            if matches!(value, Scalar::Null) {
                return Scalar::Null;
            }
            let mut saw_null = false;
            for candidate in candidates {
                let candidate = evaluate(candidate);
                if matches!(candidate, Scalar::Null) {
                    saw_null = true;
                } else if cmp(&value, &candidate) == Some(Ordering::Equal) {
                    return Scalar::Bool(!negated);
                }
            }
            if saw_null {
                Scalar::Null
            } else {
                Scalar::Bool(*negated)
            }
        }
        Expr::Between(value, low, high, negated) => {
            let value = evaluate(value);
            let low = evaluate(low);
            let high = evaluate(high);
            if matches!(value, Scalar::Null)
                || matches!(low, Scalar::Null)
                || matches!(high, Scalar::Null)
            {
                Scalar::Null
            } else {
                let inside = matches!(cmp(&value, &low), Some(Ordering::Equal | Ordering::Greater))
                    && matches!(cmp(&value, &high), Some(Ordering::Equal | Ordering::Less));
                Scalar::Bool(inside ^ *negated)
            }
        }
        Expr::Like(value, pattern, negated) => match (evaluate(value), evaluate(pattern)) {
            (Scalar::Str(value), Scalar::Str(pattern)) => {
                Scalar::Bool(like_matches(&value, &pattern) ^ *negated)
            }
            _ => Scalar::Null,
        },
        _ => eval_rel(expression, catalog, row, bindings),
    }
}

pub(crate) struct RelGroup {
    pub(crate) row: RelRow,
    pub(crate) states: Vec<AggState>,
}

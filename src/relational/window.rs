use super::*;

pub(crate) fn collect_windows(expression: &Expr, output: &mut Vec<Expr>) {
    if matches!(expression, Expr::Window { .. }) {
        let key = format!("{expression:?}");
        if !output
            .iter()
            .any(|candidate| format!("{candidate:?}") == key)
        {
            output.push(expression.clone());
        }
        return;
    }
    match expression {
        Expr::Unary(_, value) | Expr::Func(_, value) | Expr::IsNull(value, _) => {
            collect_windows(value, output)
        }
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            collect_windows(left, output);
            collect_windows(right, output);
        }
        Expr::Call(_, args) => args.iter().for_each(|arg| collect_windows(arg, output)),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                collect_windows(condition, output);
                collect_windows(value, output);
            }
            collect_windows(fallback, output);
        }
        Expr::Cast(value, _) => collect_windows(value, output),
        Expr::InList(value, values, _) => {
            collect_windows(value, output);
            values.iter().for_each(|item| collect_windows(item, output));
        }
        Expr::Between(value, low, high, _) => {
            collect_windows(value, output);
            collect_windows(low, output);
            collect_windows(high, output);
        }
        _ => {}
    }
}

pub(crate) fn compare_rel_order(
    left: RelRow,
    right: RelRow,
    order: &[(String, String, bool, Option<bool>)],
    catalog: &Catalog,
) -> Ordering {
    for (table, column, ascending, requested_nulls_first) in order {
        let left = relation_scalar(catalog, left, table, column);
        let right = relation_scalar(catalog, right, table, column);
        let left_null = matches!(left, Scalar::Null);
        let right_null = matches!(right, Scalar::Null);
        let nulls_first = requested_nulls_first.unwrap_or(!ascending);
        let ordering = match (left_null, right_null) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let ordering = cmp(&left, &right).unwrap_or(Ordering::Equal);
                if *ascending {
                    ordering
                } else {
                    ordering.reverse()
                }
            }
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

pub(crate) fn compute_window(
    expression: &Expr,
    relation: &[RelRow],
    catalog: &Catalog,
    bindings: &std::collections::HashMap<String, String>,
) -> Vec<Scalar> {
    if !account_query_memory_or_stop(
        relation
            .len()
            .saturating_mul(std::mem::size_of::<Scalar>() + std::mem::size_of::<usize>() + 64),
        "window partitions and values",
    ) {
        return Vec::new();
    }
    let Expr::Window {
        name,
        args,
        partition_by,
        order_by,
    } = expression
    else {
        unreachable!()
    };
    let partition_columns: Vec<_> = partition_by
        .iter()
        .map(|column| resolve_column(column, bindings).expect("bound window partition column"))
        .collect();
    let resolved_order: Vec<_> = order_by
        .iter()
        .map(|spec| {
            let (table, column) =
                resolve_column(&spec.key, bindings).expect("bound window order column");
            (table, column, spec.ascending, spec.nulls_first)
        })
        .collect();
    let mut partitions = std::collections::HashMap::<Vec<ScalarKey>, Vec<usize>>::new();
    for (index, &row) in relation.iter().enumerate() {
        let key = partition_columns
            .iter()
            .map(|(table, column)| relation_group_key(catalog, row, table, column))
            .collect();
        partitions.entry(key).or_default().push(index);
    }
    let mut result = vec![Scalar::Null; relation.len()];
    for mut indices in partitions.into_values() {
        indices.sort_by(|&left, &right| {
            compare_rel_order(relation[left], relation[right], &resolved_order, catalog)
                .then_with(|| left.cmp(&right))
        });
        match name.as_str() {
            "row_number" => {
                for (position, &index) in indices.iter().enumerate() {
                    result[index] = Scalar::Int((position + 1) as i64);
                }
            }
            "rank" | "dense_rank" => {
                let mut rank = 1usize;
                let mut dense = 1usize;
                for position in 0..indices.len() {
                    if position > 0
                        && compare_rel_order(
                            relation[indices[position - 1]],
                            relation[indices[position]],
                            &resolved_order,
                            catalog,
                        ) != Ordering::Equal
                    {
                        rank = position + 1;
                        dense += 1;
                    }
                    result[indices[position]] =
                        Scalar::Int(if name == "rank" { rank } else { dense } as i64);
                }
            }
            "lag" | "lead" => {
                for (position, &index) in indices.iter().enumerate() {
                    let offset = args
                        .get(1)
                        .map(|arg| eval_rel(arg, catalog, relation[index], bindings))
                        .and_then(|value| match value {
                            Scalar::Int(value) if value >= 0 => Some(value as usize),
                            _ => None,
                        })
                        .unwrap_or(1);
                    let target = if name == "lag" {
                        position.checked_sub(offset)
                    } else {
                        position
                            .checked_add(offset)
                            .filter(|target| *target < indices.len())
                    };
                    result[index] = target.map_or_else(
                        || {
                            args.get(2).map_or(Scalar::Null, |fallback| {
                                eval_rel(fallback, catalog, relation[index], bindings)
                            })
                        },
                        |target| eval_rel(&args[0], catalog, relation[indices[target]], bindings),
                    );
                }
            }
            "count" | "sum" | "avg" | "min" | "max" => {
                let aggregate = Expr::Func(name.clone(), Box::new(args[0].clone()));
                if order_by.is_empty() {
                    let mut state = aggregate_state(&aggregate);
                    for &index in &indices {
                        update_rel_aggregate(
                            &mut state,
                            &aggregate,
                            catalog,
                            relation[index],
                            bindings,
                        );
                    }
                    let value = finish(&state);
                    for &index in &indices {
                        result[index] = value.clone();
                    }
                } else {
                    let mut state = aggregate_state(&aggregate);
                    for &index in &indices {
                        update_rel_aggregate(
                            &mut state,
                            &aggregate,
                            catalog,
                            relation[index],
                            bindings,
                        );
                        result[index] = finish(&state);
                    }
                }
            }
            _ => {}
        }
    }
    result
}

pub(crate) fn eval_window_expr(
    expression: &Expr,
    row_index: usize,
    relation: &[RelRow],
    catalog: &Catalog,
    bindings: &std::collections::HashMap<String, String>,
    windows: &[Expr],
    values: &[Vec<Scalar>],
) -> Scalar {
    if matches!(expression, Expr::Window { .. }) {
        let key = format!("{expression:?}");
        return windows
            .iter()
            .position(|candidate| format!("{candidate:?}") == key)
            .map_or(Scalar::Null, |index| values[index][row_index].clone());
    }
    if !contains_window(expression) {
        return eval_rel(expression, catalog, relation[row_index], bindings);
    }
    let evaluate = |value: &Expr| {
        eval_window_expr(
            value, row_index, relation, catalog, bindings, windows, values,
        )
    };
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
        Expr::Call(name, args) => eval_values(name, args.iter().map(evaluate).collect()),
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
        _ => Scalar::Null,
    }
}

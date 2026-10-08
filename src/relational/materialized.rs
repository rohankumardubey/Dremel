use super::*;

pub(crate) struct MaterializedRelation {
    pub(crate) name: String,
    pub(crate) columns: Vec<String>,
    pub(crate) rows: Vec<Vec<Scalar>>,
}

pub(crate) fn output_columns(query: &Query) -> Vec<String> {
    query
        .select
        .iter()
        .enumerate()
        .map(|(index, item)| {
            item.alias.clone().unwrap_or_else(|| match &item.expr {
                Expr::Column(column) => column.rsplit('.').next().unwrap_or(column).into(),
                _ => format!("column{}", index + 1),
            })
        })
        .collect()
}

pub(crate) fn eval_materialized_values(
    expression: &Expr,
    columns: &[String],
    row: &[Scalar],
) -> Scalar {
    eval_with_columns(expression, &|column| {
        let exact = columns.iter().position(|candidate| candidate == column);
        let name = column.rsplit('.').next().unwrap_or(column);
        let mut unqualified = columns
            .iter()
            .enumerate()
            .filter(|(_, candidate)| candidate.rsplit('.').next() == Some(name));
        let fallback = unqualified
            .next()
            .and_then(|(index, _)| unqualified.next().is_none().then_some(index));
        exact
            .or(fallback)
            .map_or(Scalar::Null, |index| row[index].clone())
    })
}

pub(crate) fn eval_with_columns(
    expression: &Expr,
    column_value: &impl Fn(&str) -> Scalar,
) -> Scalar {
    let evaluate = |value: &Expr| eval_with_columns(value, column_value);
    match expression {
        Expr::Null => Scalar::Null,
        Expr::Column(column) => column_value(column),
        Expr::Int(value) => Scalar::Int(*value),
        Expr::Float(value) => Scalar::Float(*value),
        Expr::Bool(value) => Scalar::Bool(*value),
        Expr::String(value) => Scalar::Str(value.clone()),
        Expr::Star => Scalar::Int(1),
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
        Expr::DictEq(_, _, _)
        | Expr::Window { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::InSubquery(_, _, _) => Scalar::Null,
    }
}

pub(crate) fn eval_materialized(
    expression: &Expr,
    relation: &MaterializedRelation,
    row: usize,
) -> Scalar {
    eval_materialized_values(expression, &relation.columns, &relation.rows[row])
}

pub(crate) fn update_materialized_aggregate(
    state: &mut AggState,
    expression: &Expr,
    relation: &MaterializedRelation,
    row: usize,
) {
    let Expr::Func(_, argument) = expression else {
        unreachable!()
    };
    let value = eval_materialized(argument, relation, row);
    update_scalar_aggregate(state, matches!(argument.as_ref(), Expr::Star), value);
}

pub(crate) fn update_scalar_aggregate(state: &mut AggState, count_star: bool, value: Scalar) {
    match state {
        AggState::Count(count) => {
            if count_star || !matches!(value, Scalar::Null) {
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

pub(crate) fn eval_materialized_group(
    expression: &Expr,
    relation: &MaterializedRelation,
    row: usize,
    aggregates: &[Expr],
    values: &[Scalar],
) -> Scalar {
    eval_group_with_columns(expression, aggregates, values, &|column| {
        eval_materialized(&Expr::Column(column.into()), relation, row)
    })
}

pub(crate) fn eval_group_with_columns(
    expression: &Expr,
    aggregates: &[Expr],
    values: &[Scalar],
    column_value: &impl Fn(&str) -> Scalar,
) -> Scalar {
    if is_agg(expression) {
        let key = format!("{expression:?}");
        return aggregates
            .iter()
            .position(|candidate| format!("{candidate:?}") == key)
            .map_or(Scalar::Null, |index| values[index].clone());
    }
    if !contains_agg(expression) {
        return eval_with_columns(expression, column_value);
    }
    let evaluate = |value: &Expr| eval_group_with_columns(value, aggregates, values, column_value);
    match expression {
        Expr::Binary(operator, left, right) => {
            apply_binary(operator, evaluate(left), evaluate(right))
        }
        Expr::Unary(operator, value) if operator == "not" => evaluate(value)
            .sql_bool()
            .map_or(Scalar::Null, |value| Scalar::Bool(!value)),
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

pub(crate) fn execute_materialized(
    query: &Query,
    relation: &MaterializedRelation,
) -> Result<Vec<Vec<Scalar>>, String> {
    if query.from.name != relation.name {
        return Err(format!("unknown CTE {}", query.from.name));
    }
    if !account_query_memory_or_stop(
        relation
            .rows
            .len()
            .saturating_mul(std::mem::size_of::<usize>()),
        "filter selection",
    ) {
        return Ok(Vec::new());
    }
    let mut selected: Vec<_> = (0..relation.rows.len()).collect();
    if let Some(filter) = &query.filter {
        selected.retain(|&row| eval_materialized(filter, relation, row).truthy());
    }
    let aggregate = query.select.iter().any(|item| contains_agg(&item.expr))
        || query.having.as_ref().is_some_and(contains_agg)
        || !query.group_by.is_empty();
    let mut rows = Vec::new();
    if aggregate {
        let mut aggregate_expressions = Vec::new();
        for item in &query.select {
            collect_aggregates(&item.expr, &mut aggregate_expressions);
        }
        if let Some(having) = &query.having {
            collect_aggregates(having, &mut aggregate_expressions);
        }
        let templates: Vec<_> = aggregate_expressions.iter().map(aggregate_state).collect();
        let mut groups = std::collections::HashMap::<Vec<ScalarKey>, (usize, Vec<AggState>)>::new();
        if query.group_by.is_empty() {
            groups.insert(Vec::new(), (0, templates.clone()));
        }
        for row in selected {
            let key: Vec<ScalarKey> = query
                .group_by
                .iter()
                .map(|column| {
                    scalar_group_key(eval_materialized(
                        &Expr::Column(column.clone()),
                        relation,
                        row,
                    ))
                })
                .collect();
            if !groups.contains_key(&key)
                && !account_query_memory_or_stop(
                    key.len().saturating_mul(std::mem::size_of::<ScalarKey>())
                        + templates
                            .len()
                            .saturating_mul(std::mem::size_of::<AggState>())
                        + 64,
                    "materialized hash aggregation",
                )
            {
                return Ok(Vec::new());
            }
            let group = groups
                .entry(key)
                .or_insert_with(|| (row, templates.clone()));
            group.0 = row;
            for (state, expression) in group.1.iter_mut().zip(&aggregate_expressions) {
                update_materialized_aggregate(state, expression, relation, row);
            }
        }
        for (_, (row, states)) in groups {
            let values: Vec<_> = states.iter().map(finish).collect();
            if query.having.as_ref().is_some_and(|having| {
                !eval_materialized_group(having, relation, row, &aggregate_expressions, &values)
                    .truthy()
            }) {
                continue;
            }
            let output: Vec<_> = query
                .select
                .iter()
                .map(|item| {
                    eval_materialized_group(
                        &item.expr,
                        relation,
                        row,
                        &aggregate_expressions,
                        &values,
                    )
                })
                .collect();
            if !account_query_memory_or_stop(
                crate::execution::row_bytes(&output),
                "result materialization",
            ) {
                return Ok(Vec::new());
            }
            rows.push(output);
        }
    } else {
        for row in selected {
            let output: Vec<_> = query
                .select
                .iter()
                .map(|item| eval_materialized(&item.expr, relation, row))
                .collect();
            if !account_query_memory_or_stop(
                crate::execution::row_bytes(&output),
                "result materialization",
            ) {
                return Ok(Vec::new());
            }
            rows.push(output);
        }
    }
    finalize_rows(query, &mut rows);
    Ok(rows)
}

pub(crate) fn execute_materialized_joins(
    query: &Query,
    catalog: &Catalog,
) -> Result<Option<Vec<Vec<Scalar>>>, String> {
    let find_cte = |table: &TableRef| query.ctes.iter().find(|(name, _)| name == &table.name);
    if find_cte(&query.from).is_none()
        && query
            .joins
            .iter()
            .all(|join| find_cte(&join.table).is_none())
    {
        return Ok(None);
    }
    let materialize = |table: &TableRef| -> Result<(Vec<String>, Vec<Vec<Scalar>>), String> {
        if let Some((_, cte_query)) = find_cte(table) {
            return Ok((output_columns(cte_query), execute_rel(cte_query, catalog)?));
        }
        let columns: Vec<_> = table_columns(&table.name)
            .iter()
            .map(|column| (*column).to_string())
            .collect();
        if columns.is_empty() {
            return Err(format!("unknown table {}", table.name));
        }
        let mut rows = Vec::new();
        for row in base_relation_rows(&table.name, catalog) {
            let output: Vec<_> = columns
                .iter()
                .map(|column| relation_scalar(catalog, row, &table.name, column))
                .collect();
            if !account_query_memory_or_stop(
                crate::execution::row_bytes(&output),
                "materialized relation",
            ) {
                return Ok((columns, Vec::new()));
            }
            rows.push(output);
        }
        Ok((columns, rows))
    };
    let (from_columns, mut rows) = materialize(&query.from)?;
    let mut columns: Vec<_> = from_columns
        .iter()
        .map(|column| format!("{}.{}", query.from.alias, column))
        .collect();
    for join in &query.joins {
        let (raw_right_columns, right_rows) = materialize(&join.table)?;
        let right_columns: Vec<_> = raw_right_columns
            .into_iter()
            .map(|column| format!("{}.{}", join.table.alias, column))
            .collect();
        let mut combined_columns = columns.clone();
        combined_columns.extend(right_columns.iter().cloned());
        let left_width = columns.len();
        let right_width = right_columns.len();
        let mut joined = Vec::new();
        let mut matched_right = vec![false; right_rows.len()];
        for left in rows {
            let mut matched = false;
            for (right_index, right) in right_rows.iter().enumerate() {
                let mut combined = left.clone();
                combined.extend(right.iter().cloned());
                let matches = join.kind == JoinKind::Cross
                    || join.on.as_ref().is_some_and(|on| {
                        eval_materialized_values(on, &combined_columns, &combined).truthy()
                    });
                if matches {
                    if !account_query_memory_or_stop(
                        crate::execution::row_bytes(&combined),
                        "materialized join output",
                    ) {
                        return Ok(Some(Vec::new()));
                    }
                    joined.push(combined);
                    matched = true;
                    matched_right[right_index] = true;
                }
            }
            if !matched && matches!(join.kind, JoinKind::Left | JoinKind::Full) {
                let mut combined = left;
                combined.extend(std::iter::repeat_n(Scalar::Null, right_width));
                if !account_query_memory_or_stop(
                    crate::execution::row_bytes(&combined),
                    "materialized join output",
                ) {
                    return Ok(Some(Vec::new()));
                }
                joined.push(combined);
            }
        }
        if matches!(join.kind, JoinKind::Right | JoinKind::Full) {
            for (right, matched) in right_rows.iter().zip(matched_right) {
                if !matched {
                    let mut combined = vec![Scalar::Null; left_width];
                    combined.extend(right.iter().cloned());
                    if !account_query_memory_or_stop(
                        crate::execution::row_bytes(&combined),
                        "materialized join output",
                    ) {
                        return Ok(Some(Vec::new()));
                    }
                    joined.push(combined);
                }
            }
        }
        rows = joined;
        columns = combined_columns;
    }
    let relation = MaterializedRelation {
        name: query.from.name.clone(),
        columns,
        rows,
    };
    Ok(Some(execute_materialized(query, &relation)?))
}

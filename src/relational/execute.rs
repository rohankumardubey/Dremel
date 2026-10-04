use super::*;

pub(crate) fn execute_rel(query: &Query, catalog: &Catalog) -> Result<Vec<Vec<Scalar>>, String> {
    execute_rel_inner(query, catalog, false)
}

pub(crate) fn execute_rel_inner(
    query: &Query,
    catalog: &Catalog,
    skip_union: bool,
) -> Result<Vec<Vec<Scalar>>, String> {
    if !query.joins.is_empty()
        && !query.ctes.is_empty()
        && let Some(rows) = execute_materialized_joins(query, catalog)?
    {
        return Ok(rows);
    }
    if !query.ctes.is_empty()
        && let Some((name, cte_query)) =
            query.ctes.iter().find(|(name, _)| name == &query.from.name)
    {
        let relation = MaterializedRelation {
            name: name.clone(),
            columns: output_columns(cte_query),
            rows: execute_rel(cte_query, catalog)?,
        };
        return execute_materialized(query, &relation);
    }
    if !skip_union && let Some(right) = &query.union {
        let mut rows = execute_rel_inner(query, catalog, true)?;
        let right_rows = execute_rel(right, catalog)?;
        if rows.first().map(Vec::len) != right_rows.first().map(Vec::len)
            && !rows.is_empty()
            && !right_rows.is_empty()
        {
            return Err("UNION inputs must have the same column count".into());
        }
        rows.extend(right_rows);
        if !query.union_all {
            let mut seen = std::collections::HashSet::new();
            rows.retain(|row| {
                seen.insert(row.iter().map(scalar_group_key_ref).collect::<Vec<_>>())
            });
        }
        return Ok(rows);
    }
    let bindings = bind_query(query)?;
    let scan_filters = pushed_filters(query);
    let residual_filter = residual_filter(query);
    let impossible = filter_always_false(query.filter.as_ref());
    let mut relation = if impossible {
        Vec::new()
    } else {
        base_relation_rows_filtered(&query.from.name, catalog, &bindings, &scan_filters)
    };
    if execution_cancelled() {
        return Ok(Vec::new());
    }
    for join in &query.joins {
        if impossible {
            break;
        }
        relation = apply_join(
            relation,
            join,
            catalog,
            &bindings,
            query.optimizer_enabled,
            &scan_filters,
        );
        if execution_cancelled() {
            return Ok(Vec::new());
        }
    }
    if !impossible && let Some(filter) = &residual_filter {
        relation.retain(|row| {
            !execution_cancelled() && eval_rel(filter, catalog, *row, &bindings).truthy()
        });
    }
    let aggregate = query.select.iter().any(|item| contains_agg(&item.expr))
        || query.having.as_ref().is_some_and(contains_agg)
        || !query.group_by.is_empty();
    let mut rows: Vec<Vec<Scalar>> = Vec::new();
    let has_windows = query.select.iter().any(|item| contains_window(&item.expr));
    if aggregate {
        let mut aggregate_expressions = Vec::new();
        for item in &query.select {
            collect_aggregates(&item.expr, &mut aggregate_expressions);
        }
        if let Some(having) = &query.having {
            collect_aggregates(having, &mut aggregate_expressions);
        }
        let templates: Vec<_> = aggregate_expressions.iter().map(aggregate_state).collect();
        let mut groups = std::collections::HashMap::<Vec<ScalarKey>, RelGroup>::new();
        if query.group_by.is_empty() {
            if !account_query_memory_or_stop(
                templates
                    .len()
                    .saturating_mul(std::mem::size_of::<AggState>())
                    + 64,
                "relational hash aggregation",
            ) {
                return Ok(Vec::new());
            }
            groups.insert(
                Vec::new(),
                RelGroup {
                    row: RelRow::default(),
                    states: templates.clone(),
                },
            );
        }
        let group_columns = query
            .group_by
            .iter()
            .map(|column| resolve_column(column, &bindings))
            .collect::<Result<Vec<_>, _>>()?;
        for row in relation {
            if execution_cancelled() {
                break;
            }
            let key: Vec<ScalarKey> = group_columns
                .iter()
                .map(|(table, column)| relation_group_key(catalog, row, table, column))
                .collect();
            if !groups.contains_key(&key)
                && !account_query_memory_or_stop(
                    key.len().saturating_mul(std::mem::size_of::<ScalarKey>())
                        + templates
                            .len()
                            .saturating_mul(std::mem::size_of::<AggState>())
                        + 64,
                    "relational hash aggregation",
                )
            {
                return Ok(Vec::new());
            }
            let group = groups.entry(key).or_insert_with(|| RelGroup {
                row,
                states: templates.clone(),
            });
            group.row = row;
            for (state, expression) in group.states.iter_mut().zip(&aggregate_expressions) {
                update_rel_aggregate(state, expression, catalog, row, &bindings);
            }
        }
        for group in groups.into_values() {
            let aggregate_values: Vec<_> = group.states.iter().map(finish).collect();
            if query.having.as_ref().is_some_and(|having| {
                !eval_group_expr(
                    having,
                    catalog,
                    group.row,
                    &bindings,
                    &aggregate_expressions,
                    &aggregate_values,
                )
                .truthy()
            }) {
                continue;
            }
            let output: Vec<_> = query
                .select
                .iter()
                .map(|item| {
                    eval_group_expr(
                        &item.expr,
                        catalog,
                        group.row,
                        &bindings,
                        &aggregate_expressions,
                        &aggregate_values,
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
    } else if has_windows {
        let mut window_expressions = Vec::new();
        for item in &query.select {
            collect_windows(&item.expr, &mut window_expressions);
        }
        let window_values: Vec<_> = window_expressions
            .iter()
            .map(|window| compute_window(window, &relation, catalog, &bindings))
            .collect();
        if execution_cancelled() {
            return Ok(Vec::new());
        }
        for row_index in 0..relation.len() {
            let output: Vec<_> = query
                .select
                .iter()
                .map(|item| {
                    eval_window_expr(
                        &item.expr,
                        row_index,
                        &relation,
                        catalog,
                        &bindings,
                        &window_expressions,
                        &window_values,
                    )
                })
                .collect();
            if !account_query_memory_or_stop(
                crate::execution::row_bytes(&output),
                "window result materialization",
            ) {
                return Ok(Vec::new());
            }
            rows.push(output);
        }
    } else {
        let top_k = query
            .optimizer_enabled
            .then_some(query.limit)
            .flatten()
            .filter(|_| !query.order_by.is_empty())
            .map(|limit| limit.saturating_add(query.offset));
        if let Some(top_k) = top_k {
            account_query_memory(
                top_k.saturating_add(1).saturating_mul(
                    std::mem::size_of::<Vec<Scalar>>()
                        + query
                            .select
                            .len()
                            .saturating_mul(std::mem::size_of::<Scalar>() + 64),
                ),
                "top-k buffer",
            )?;
        }
        let order = query_output_order(query);
        let mut worst_top_k = None;
        for row in relation {
            let output: Vec<_> = query
                .select
                .iter()
                .map(|item| eval_rel(&item.expr, catalog, row, &bindings))
                .collect();
            if let Some(top_k) = top_k {
                if top_k == 0 {
                    continue;
                }
                if rows.len() < top_k {
                    rows.push(output);
                    if rows.len() == top_k {
                        worst_top_k = (0..rows.len()).max_by(|&left, &right| {
                            compare_output_rows(&rows[left], &rows[right], &order)
                        });
                    }
                } else {
                    let worst = worst_top_k.expect("non-empty top-k");
                    if compare_output_rows(&output, &rows[worst], &order) == Ordering::Less {
                        rows[worst] = output;
                        worst_top_k = (0..rows.len()).max_by(|&left, &right| {
                            compare_output_rows(&rows[left], &rows[right], &order)
                        });
                    }
                }
            } else {
                if !account_query_memory_or_stop(
                    crate::execution::row_bytes(&output),
                    "result materialization",
                ) {
                    return Ok(Vec::new());
                }
                rows.push(output);
            }
        }
    }
    finalize_rows(query, &mut rows);
    Ok(rows)
}

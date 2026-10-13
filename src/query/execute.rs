use super::unnest::{self, ColumnAccess, Expansion, Position};
use crate::execution::aggregate::{AggState, finish};
use crate::relational::{
    aggregate_state, collect_aggregates, eval_group_with_columns, eval_with_columns, finalize_rows,
    scalar_group_key, update_scalar_aggregate,
};
use crate::sql::*;
use crate::storage::{ColumnarTable, ValueBatch};
use crate::types::{
    Scalar, ScalarKey, account_query_memory, current_query_memory, execution_cancelled,
};
use std::collections::HashMap;

struct Group {
    location: Option<(usize, Position)>,
    states: Vec<AggState>,
}

fn state_heap_bytes(states: &[AggState]) -> usize {
    states
        .iter()
        .map(|state| match state {
            AggState::Min(Some(value)) | AggState::Max(Some(value)) => value.heap_bytes(),
            _ => 0,
        })
        .sum()
}

pub(super) fn execute(
    query: &Query,
    table: &ColumnarTable,
    paths: &HashMap<String, ColumnAccess>,
    unnests: &[Expansion],
) -> Result<Vec<Vec<Scalar>>, String> {
    let value_paths = paths
        .iter()
        .filter_map(|(name, access)| match access {
            ColumnAccess::Value { path, .. } => Some((name.clone(), path.clone())),
            ColumnAccess::Ordinality { .. } => None,
        })
        .collect();
    let batches = ValueBatch::from_table(table, &value_paths)?;
    let sources = crate::storage::list_sources(
        table,
        &unnests
            .iter()
            .map(|stage| stage.path.as_str())
            .collect::<Vec<_>>(),
    )?;
    let mut position = Position::new(0, unnests.len());
    account_query_memory(position.heap_bytes(), "UNNEST cursor")?;
    let aggregate = query.select.iter().any(|item| contains_agg(&item.expr))
        || query.having.as_ref().is_some_and(contains_agg)
        || !query.group_by.is_empty();
    let mut rows = Vec::new();
    let mut expressions = Vec::new();
    for item in &query.select {
        collect_aggregates(&item.expr, &mut expressions);
    }
    if let Some(having) = &query.having {
        collect_aggregates(having, &mut expressions);
    }
    let templates: Vec<_> = expressions.iter().map(aggregate_state).collect();
    let metadata_count = unnests.is_empty() && aggregate && query.filter.is_none() && query.group_by.is_empty()
        && expressions.iter().all(|expression| matches!(expression, Expr::Func(name, argument) if name == "count" && matches!(argument.as_ref(), Expr::Star)));
    let mut groups = HashMap::<Vec<ScalarKey>, Group>::new();
    let group_bytes =
        templates.len() * std::mem::size_of::<AggState>() + std::mem::size_of::<Group>() + 64;
    if aggregate && query.group_by.is_empty() {
        account_query_memory(group_bytes, "generic hash aggregation")?;
        groups.insert(
            Vec::new(),
            Group {
                location: None,
                states: if metadata_count {
                    templates
                        .iter()
                        .map(|_| AggState::Count(table.row_count() as u64))
                        .collect()
                } else {
                    templates.clone()
                },
            },
        );
    }
    'scan: for (batch_index, batch) in batches.iter().enumerate() {
        if metadata_count {
            break;
        }
        for row in 0..batch.rows {
            if execution_cancelled() {
                return Err("query cancelled during Arrow scan".into());
            }
            position.indices[0] = Some(row);
            let mut consume = |position: &Position| -> Result<bool, String> {
                let value = |column: &str| unnest::value(column, batch, paths, position);
                if query
                    .filter
                    .as_ref()
                    .is_some_and(|filter| !eval_with_columns(filter, &value).truthy())
                {
                    return Ok(true);
                }
                if aggregate {
                    let key: Vec<_> = query
                        .group_by
                        .iter()
                        .map(|column| scalar_group_key(value(column)))
                        .collect();
                    if !groups.contains_key(&key) {
                        let heap_bytes: usize = key.iter().map(ScalarKey::heap_bytes).sum();
                        account_query_memory(
                            group_bytes + heap_bytes + key.len() * std::mem::size_of::<ScalarKey>(),
                            "generic hash aggregation",
                        )?;
                    }
                    let group = groups.entry(key).or_insert_with(|| Group {
                        location: None,
                        states: templates.clone(),
                    });
                    if group.location.is_none() {
                        account_query_memory(position.heap_bytes(), "aggregate row cursor")?;
                        group.location = Some((batch_index, position.clone()));
                    }
                    let before = state_heap_bytes(&group.states);
                    for (state, expression) in group.states.iter_mut().zip(&expressions) {
                        let Expr::Func(_, argument) = expression else {
                            unreachable!()
                        };
                        update_scalar_aggregate(
                            state,
                            matches!(argument.as_ref(), Expr::Star),
                            eval_with_columns(argument, &value),
                        );
                    }
                    let after = state_heap_bytes(&group.states);
                    if after > before {
                        account_query_memory(after - before, "aggregate retained values")?;
                    } else if let Some(memory) = current_query_memory() {
                        memory.release(before - after);
                    }
                } else {
                    if query.limit == Some(0) {
                        return Ok(false);
                    }
                    let output: Vec<_> = query
                        .select
                        .iter()
                        .map(|item| eval_with_columns(&item.expr, &value))
                        .collect();
                    account_query_memory(
                        crate::execution::row_bytes(&output),
                        "result materialization",
                    )?;
                    rows.push(output);
                    if query.order_by.is_empty()
                        && !query.distinct
                        && query
                            .limit
                            .is_some_and(|limit| rows.len() >= limit.saturating_add(query.offset))
                    {
                        return Ok(false);
                    }
                }
                Ok(true)
            };
            let keep_scanning = if unnests.is_empty() {
                consume(&position)?
            } else {
                unnest::visit(
                    unnests,
                    &sources[batch_index],
                    batch,
                    paths,
                    &mut position,
                    &mut consume,
                )?
            };
            if !keep_scanning {
                break 'scan;
            }
        }
    }
    if aggregate {
        for group in groups.into_values() {
            let values: Vec<_> = group.states.iter().map(finish).collect();
            let column = |name: &str| {
                group
                    .location
                    .as_ref()
                    .map_or(Scalar::Null, |(batch, position)| {
                        unnest::value(name, &batches[*batch], paths, position)
                    })
            };
            if query.having.as_ref().is_some_and(|having| {
                !eval_group_with_columns(having, &expressions, &values, &column).truthy()
            }) {
                continue;
            }
            let output: Vec<_> = query
                .select
                .iter()
                .map(|item| eval_group_with_columns(&item.expr, &expressions, &values, &column))
                .collect();
            account_query_memory(
                crate::execution::row_bytes(&output),
                "result materialization",
            )?;
            rows.push(output);
        }
    }
    if execution_cancelled() {
        return Err("query cancelled before result finalization".into());
    }
    let sort_bytes = if query.order_by.is_empty() {
        0
    } else {
        rows.len()
            .saturating_mul(std::mem::size_of::<Vec<Scalar>>())
    };
    account_query_memory(sort_bytes, "result sorting workspace")?;
    finalize_rows(query, &mut rows);
    if let Some(memory) = current_query_memory() {
        memory.release(sort_bytes);
    }
    if execution_cancelled() {
        return Err("query cancelled during result finalization".into());
    }
    Ok(rows)
}

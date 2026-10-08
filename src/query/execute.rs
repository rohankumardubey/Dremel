use crate::execution::aggregate::{AggState, finish};
use crate::relational::{
    aggregate_state, collect_aggregates, eval_group_with_columns, eval_with_columns, finalize_rows,
    scalar_group_key, update_scalar_aggregate,
};
use crate::sql::*;
use crate::storage::{ColumnarTable, PrimitiveBatch};
use crate::types::{
    Scalar, ScalarKey, account_query_memory, current_query_memory, execution_cancelled,
};
use std::collections::HashMap;

struct Group {
    location: Option<(usize, usize)>,
    states: Vec<AggState>,
}

fn state_strings(states: &[AggState]) -> usize {
    states
        .iter()
        .map(|state| match state {
            AggState::Min(Some(Scalar::Str(text))) | AggState::Max(Some(Scalar::Str(text))) => {
                text.capacity()
            }
            _ => 0,
        })
        .sum()
}

pub(super) fn execute(query: &Query, table: &ColumnarTable) -> Result<Vec<Vec<Scalar>>, String> {
    let batches = PrimitiveBatch::from_table(table, &query.columns)?;
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
    let metadata_count = aggregate && query.filter.is_none() && query.group_by.is_empty()
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
            let value = |column: &str| batch.scalar(column, row);
            if query
                .filter
                .as_ref()
                .is_some_and(|filter| !eval_with_columns(filter, &value).truthy())
            {
                continue;
            }
            if aggregate {
                let key: Vec<_> = query
                    .group_by
                    .iter()
                    .map(|column| scalar_group_key(value(column)))
                    .collect();
                if !groups.contains_key(&key) {
                    let strings: usize = key
                        .iter()
                        .map(|value| match value {
                            ScalarKey::Str(text) => text.capacity(),
                            _ => 0,
                        })
                        .sum();
                    account_query_memory(
                        group_bytes + strings + key.len() * std::mem::size_of::<ScalarKey>(),
                        "generic hash aggregation",
                    )?;
                }
                let group = groups.entry(key).or_insert_with(|| Group {
                    location: None,
                    states: templates.clone(),
                });
                group.location = Some((batch_index, row));
                let before = state_strings(&group.states);
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
                let after = state_strings(&group.states);
                if after > before {
                    account_query_memory(after - before, "aggregate string values")?;
                } else if let Some(memory) = current_query_memory() {
                    memory.release(before - after);
                }
            } else {
                if query.limit == Some(0) {
                    break 'scan;
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
                    break 'scan;
                }
            }
        }
    }
    if aggregate {
        for group in groups.into_values() {
            let values: Vec<_> = group.states.iter().map(finish).collect();
            let column = |name: &str| {
                group.location.map_or(Scalar::Null, |(batch, row)| {
                    batches[batch].scalar(name, row)
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
    finalize_rows(query, &mut rows);
    Ok(rows)
}

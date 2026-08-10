pub(crate) mod aggregate;
pub(crate) mod scalar;

use crate::execution::aggregate::*;
use crate::execution::scalar::eval;
use crate::optimizer::prepare;
use crate::relational::{execute_rel, finalize_rows};
use crate::sql::*;
use crate::storage::*;
use crate::types::*;
use std::sync::Arc;
use std::time::Instant;

pub(crate) fn execute(q: &Query, t: Arc<Table>, pool: &Pool, batch: usize) -> Vec<Vec<Scalar>> {
    let aggregate = q.select.iter().any(|s| is_agg(&s.expr)) || !q.group_by.is_empty();
    let mut rows = Vec::new();
    if aggregate {
        let groups = pool.aggregate(Arc::new(q.clone()), t.clone(), batch);
        for e in groups.into_entries() {
            let mut row = Vec::new();
            let mut ai = 0;
            for item in &q.select {
                if is_agg(&item.expr) {
                    row.push(finish(&e.s[ai]));
                    ai += 1
                } else if let Expr::Column(c) = &item.expr {
                    let ki = q.group_by.iter().position(|x| x == c).expect("grouped");
                    row.push(t.key_scalar(c, e.k.v[ki]))
                }
            }
            rows.push(row)
        }
    } else {
        let mut selection = Vec::with_capacity(batch.max(1));
        'outer: for bs in (0..t.len()).step_by(batch.max(1)) {
            selection.clear();
            for i in bs..(bs + batch).min(t.len()) {
                if q.filter.as_ref().is_none_or(|f| eval(f, &t, i).truthy()) {
                    selection.push(i);
                }
            }
            for &i in &selection {
                rows.push(
                    q.select
                        .iter()
                        .map(|x| eval(&x.expr, t.as_ref(), i))
                        .collect(),
                );
                if q.order_by.is_empty()
                    && !q.distinct
                    && q.limit
                        .is_some_and(|n| rows.len() >= n.saturating_add(q.offset))
                {
                    break 'outer;
                }
            }
        }
    }
    finalize_rows(q, &mut rows);
    rows
}
pub(crate) fn rows_json(rows: &[Vec<Scalar>]) -> String {
    format!(
        "[{}]",
        rows.iter()
            .map(|r| format!(
                "[{}]",
                r.iter().map(Scalar::json).collect::<Vec<_>>().join(",")
            ))
            .collect::<Vec<_>>()
            .join(",")
    )
}
pub(crate) fn strings_json(values: &[String]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\"")))
            .collect::<Vec<_>>()
            .join(",")
    )
}
pub(crate) fn enforce_table_limit(options: &Options, table: &Table) -> Result<(), String> {
    let bytes = table.approximate_bytes();
    if options.memory_limit_mb > 0 && bytes > options.memory_limit_mb * 1024 * 1024 {
        return Err(format!(
            "RESOURCE_EXHAUSTED table requires approximately {bytes} bytes; memory limit is {} MiB",
            options.memory_limit_mb
        ));
    }
    Ok(())
}
pub(crate) fn enforce_result_limit(
    options: &Options,
    query: &Query,
    table: &Table,
) -> Result<(), String> {
    if options.max_result_rows == 0 {
        return Ok(());
    }
    let aggregate =
        query.select.iter().any(|item| is_agg(&item.expr)) || !query.group_by.is_empty();
    let upper = if aggregate {
        if query.group_by.is_empty() {
            1
        } else {
            table.group_upper_bound(&query.group_by)
        }
    } else {
        query.limit.unwrap_or(table.len()).min(table.len())
    };
    if upper > options.max_result_rows {
        return Err(format!(
            "RESOURCE_EXHAUSTED result upper bound {upper} exceeds max-result-rows {}",
            options.max_result_rows
        ));
    }
    Ok(())
}

pub(crate) fn is_relational(query: &Query) -> bool {
    query.from.name != "events"
        || !query.joins.is_empty()
        || query.having.is_some()
        || query.select.iter().any(|item| contains_window(&item.expr))
        || query
            .select
            .iter()
            .any(|item| contains_subquery(&item.expr))
        || query.filter.as_ref().is_some_and(contains_subquery)
        || query.union.is_some()
        || !query.ctes.is_empty()
}

pub fn run_query(o: Options, sql: &str, explain: bool, stats: bool) -> Result<(), String> {
    let t = Arc::new(Table::load(&o.data)?);
    enforce_table_limit(&o, &t)?;
    let q = prepare(Parser::new(sql)?.parse()?, &t);
    if q.ctes.is_empty() {
        bind_query(&q)?;
    }
    enforce_result_limit(&o, &q, &t)?;
    if explain {
        println!("{}", q.explain(o.batch_size));
        return Ok(());
    }
    let pool = Pool::new(o.threads);
    let started = Instant::now();
    let rows = if is_relational(&q) {
        execute_rel(&q, &Catalog::load(&o.data, t.clone())?)?
    } else {
        execute(&q, t.clone(), &pool, o.batch_size)
    };
    let elapsed_ns = started.elapsed().as_nanos();
    for row in &rows {
        println!(
            "{}",
            row.iter()
                .map(|v| format!("{v:?}"))
                .collect::<Vec<_>>()
                .join("\t")
        )
    }
    if stats {
        eprintln!(
            "{{\"rows_scanned\":{},\"rows_returned\":{},\"batches_scanned\":{},\"columns_scanned\":{},\"worker_threads\":{},\"logical_partitions\":{},\"elapsed_ns\":{}}}",
            t.len(),
            rows.len(),
            t.len().div_ceil(o.batch_size),
            q.columns.len(),
            o.threads,
            o.threads * 4,
            elapsed_ns
        );
    }
    Ok(())
}

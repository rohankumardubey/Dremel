pub(crate) mod aggregate;
pub(crate) mod scalar;

use crate::execution::aggregate::*;
use crate::execution::scalar::eval;
use crate::optimizer::{filter_always_false, prepare};
use crate::relational::{execute_rel, finalize_rows};
use crate::sql::*;
use crate::storage::*;
use crate::types::*;
use std::sync::Arc;
use std::time::Instant;

pub(crate) fn row_bytes(row: &Vec<Scalar>) -> usize {
    std::mem::size_of::<Vec<Scalar>>()
        + row.capacity().saturating_mul(std::mem::size_of::<Scalar>())
        + row
            .iter()
            .map(|value| match value {
                Scalar::Str(text) => text.capacity(),
                _ => 0,
            })
            .sum::<usize>()
}

pub(crate) fn execute(
    q: &Query,
    t: Arc<Table>,
    pool: &Pool,
    batch: usize,
) -> Result<Vec<Vec<Scalar>>, String> {
    let aggregate = q.select.iter().any(|s| is_agg(&s.expr)) || !q.group_by.is_empty();
    let mut rows = Vec::new();
    if aggregate {
        let groups = pool.aggregate(Arc::new(q.clone()), t.clone(), batch)?;
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
            account_query_memory(row_bytes(&row), "result materialization")?;
            rows.push(row)
        }
    } else {
        if filter_always_false(q.filter.as_ref()) {
            return Ok(rows);
        }
        account_query_memory(
            batch.max(1).saturating_mul(std::mem::size_of::<usize>()),
            "scan selection",
        )?;
        let mut selection = Vec::with_capacity(batch.max(1));
        let top_k = q
            .optimizer_enabled
            .then_some(q.limit)
            .flatten()
            .filter(|_| !q.order_by.is_empty())
            .map(|limit| limit.saturating_add(q.offset));
        if let Some(top_k) = top_k {
            account_query_memory(
                top_k.saturating_add(1).saturating_mul(
                    std::mem::size_of::<Vec<Scalar>>()
                        + q.select
                            .len()
                            .saturating_mul(std::mem::size_of::<Scalar>() + 64),
                ),
                "top-k buffer",
            )?;
        }
        let order = crate::relational::query_output_order(q);
        let mut worst_top_k = None;
        'outer: for bs in (0..t.len()).step_by(batch.max(1)) {
            selection.clear();
            for i in bs..(bs + batch).min(t.len()) {
                if q.filter.as_ref().is_none_or(|f| eval(f, &t, i).truthy()) {
                    selection.push(i);
                }
            }
            for &i in &selection {
                let row: Vec<_> = q
                    .select
                    .iter()
                    .map(|x| eval(&x.expr, t.as_ref(), i))
                    .collect();
                if let Some(top_k) = top_k {
                    if top_k == 0 {
                        continue;
                    }
                    if rows.len() < top_k {
                        rows.push(row);
                        if rows.len() == top_k {
                            worst_top_k = (0..rows.len()).max_by(|&left, &right| {
                                crate::relational::compare_output_rows(
                                    &rows[left],
                                    &rows[right],
                                    &order,
                                )
                            });
                        }
                    } else {
                        let worst = worst_top_k.expect("non-empty top-k");
                        if crate::relational::compare_output_rows(&row, &rows[worst], &order)
                            == std::cmp::Ordering::Less
                        {
                            rows[worst] = row;
                            worst_top_k = (0..rows.len()).max_by(|&left, &right| {
                                crate::relational::compare_output_rows(
                                    &rows[left],
                                    &rows[right],
                                    &order,
                                )
                            });
                        }
                    }
                } else {
                    account_query_memory(row_bytes(&row), "result materialization")?;
                    rows.push(row);
                }
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
    Ok(rows)
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
    if o.direct_parquet && !o.data.ends_with(".parquet") {
        return Err("--direct-parquet requires a .parquet data file".into());
    }
    let t = Arc::new(if o.direct_parquet {
        Table::parquet_metadata(&o.data)?
    } else {
        Table::load(&o.data)?
    });
    enforce_table_limit(&o, &t)?;
    let mut q = prepare(Parser::new(sql)?.parse()?, &t);
    if q.ctes.is_empty() {
        bind_query(&q)?;
    }
    enforce_result_limit(&o, &q, &t)?;
    if o.direct_parquet {
        let scan = Table::parquet_scan_plan(&o.data, &q)?;
        q.physical.insert(
            1,
            format!(
                "ParquetScanExec(columns={}/{};row_groups={}/{};rows={}/{};compressed_bytes={})",
                scan.columns_read,
                scan.total_columns,
                scan.row_groups_read,
                scan.total_row_groups,
                scan.rows_read,
                scan.total_rows,
                scan.compressed_bytes_read
            ),
        );
    }
    if explain {
        println!("{}", q.explain(o.batch_size));
        return Ok(());
    }
    let pool = Pool::new(o.threads);
    let started = Instant::now();
    let (execution_table, scan) = if o.direct_parquet {
        let (table, scan) = Table::load_parquet_direct(&o.data, &q, o.batch_size)?;
        (Arc::new(table), scan)
    } else {
        (t.clone(), ParquetScanMetrics::default())
    };
    enforce_table_limit(&o, &execution_table)?;
    let (rows, query_memory) = with_query_memory(o.query_memory_limit_mb, || {
        if is_relational(&q) {
            execute_rel(&q, &Catalog::load(&o.data, execution_table.clone())?)
        } else {
            execute(&q, execution_table.clone(), &pool, o.batch_size)
        }
    });
    let rows = rows?;
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
            "{{\"rows_scanned\":{},\"rows_returned\":{},\"batches_scanned\":{},\"columns_scanned\":{},\"worker_threads\":{},\"logical_partitions\":{},\"elapsed_ns\":{},\"query_memory_limit_bytes\":{},\"query_memory_accounted_bytes\":{},\"parquet_total_rows\":{},\"parquet_rows_read\":{},\"parquet_total_row_groups\":{},\"parquet_row_groups_read\":{},\"parquet_total_columns\":{},\"parquet_columns_read\":{},\"parquet_compressed_bytes_read\":{}}}",
            execution_table.len(),
            rows.len(),
            execution_table.len().div_ceil(o.batch_size),
            q.columns.len(),
            o.threads,
            o.threads * 4,
            elapsed_ns,
            query_memory.limit_bytes(),
            query_memory.accounted_bytes(),
            scan.total_rows,
            scan.rows_read,
            scan.total_row_groups,
            scan.row_groups_read,
            scan.total_columns,
            scan.columns_read,
            scan.compressed_bytes_read
        );
    }
    Ok(())
}

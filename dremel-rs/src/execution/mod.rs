pub(crate) mod aggregate;
pub(crate) mod scalar;
mod spill;
mod stream;

pub(crate) use spill::spillable_aggregate;
pub(crate) use stream::{stream_parquet_results, stream_table_results, streamable_result};

use crate::execution::aggregate::*;
use crate::execution::scalar::{cmp, eval};
use crate::optimizer::{filter_always_false, prepare};
use crate::relational::{execute_rel, finalize_rows, scalar_group_key_ref};
use crate::sql::*;
use crate::storage::*;
use crate::types::*;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::time::Instant;

use spill::{execute_spilled_aggregate, should_spill_aggregate};

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

fn finish_group_rows(q: &Query, t: &Table, groups: GroupTable) -> Result<Vec<Vec<Scalar>>, String> {
    let mut rows = Vec::new();
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
    Ok(rows)
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
        rows = finish_group_rows(q, &t, groups)?;
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

pub(crate) fn execute_with_spill(
    query: &Query,
    table: Arc<Table>,
    pool: &Pool,
    batch: usize,
    spill_dir: Option<&str>,
) -> Result<(Vec<Vec<Scalar>>, SpillMetrics), String> {
    if should_spill_aggregate(query, &table, spill_dir) {
        return execute_spilled_aggregate(
            query,
            &table,
            batch,
            spill_dir.expect("spill directory checked"),
        );
    }
    Ok((execute(query, table, pool, batch)?, SpillMetrics::default()))
}

fn primary_key_dimension_join(join: &crate::sql::JoinSpec) -> bool {
    let expected_key = match join.table.name.as_str() {
        "users" => "user_id",
        "campaigns" => "campaign_id",
        _ => return false,
    };
    let Some(Expr::Binary(op, left, right)) = join.on.as_ref() else {
        return false;
    };
    if op != "=" {
        return false;
    }
    let is_dimension_key = |expr: &Expr| {
        let Expr::Column(column) = expr else {
            return false;
        };
        let Some((qualifier, name)) = column.rsplit_once('.') else {
            return false;
        };
        name == expected_key && (qualifier == join.table.alias || qualifier == join.table.name)
    };
    is_dimension_key(left) || is_dimension_key(right)
}

fn streamable_parquet_join(query: &Query) -> bool {
    if query.from.name != "events"
        || query.joins.is_empty()
        || query.having.is_some()
        || query.union.is_some()
        || !query.ctes.is_empty()
        || query.joins.iter().any(|join| {
            !matches!(join.kind, JoinKind::Inner | JoinKind::Left)
                || !primary_key_dimension_join(join)
                || join.on.as_ref().is_some_and(crate::sql::contains_subquery)
        })
        || query
            .select
            .iter()
            .any(|item| contains_window(&item.expr) || contains_subquery(&item.expr))
        || query.filter.as_ref().is_some_and(contains_subquery)
    {
        return false;
    }
    let aggregate =
        query.select.iter().any(|item| is_agg(&item.expr)) || !query.group_by.is_empty();
    if !aggregate {
        return true;
    }
    let supported_select = query.select.iter().all(|item| match &item.expr {
        Expr::Column(_) => true,
        Expr::Func(name, _) => matches!(name.as_str(), "count" | "sum" | "min" | "max"),
        _ => false,
    });
    supported_select
        && query.group_by.iter().all(|group| {
            query.select.iter().any(|item| {
                matches!(&item.expr, Expr::Column(column)
                    if column == group
                        || column.rsplit('.').next() == group.rsplit('.').next())
            })
        })
}

pub(crate) fn parquet_streaming_fallback(query: &Query) -> bool {
    is_relational(query) && !streamable_parquet_join(query)
}

fn merge_finished_aggregate(state: &mut AggState, value: &Scalar) -> Result<(), String> {
    match state {
        AggState::Count(total) => match value {
            Scalar::Int(count) if *count >= 0 => *total += *count as u64,
            _ => return Err("invalid streaming COUNT partial".into()),
        },
        AggState::Sum(sum) => sum.add(value),
        AggState::Min(current) => {
            if !matches!(value, Scalar::Null)
                && current
                    .as_ref()
                    .is_none_or(|old| cmp(value, old) == Some(std::cmp::Ordering::Less))
            {
                *current = Some(value.clone());
            }
        }
        AggState::Max(current) => {
            if !matches!(value, Scalar::Null)
                && current
                    .as_ref()
                    .is_none_or(|old| cmp(value, old) == Some(std::cmp::Ordering::Greater))
            {
                *current = Some(value.clone());
            }
        }
        AggState::Avg { .. } => return Err("streaming join AVG requires materialization".into()),
    }
    Ok(())
}

fn execute_parquet_streaming_join(
    query: &Query,
    path: &str,
    options: &Options,
) -> Result<(Vec<Vec<Scalar>>, ParquetScanMetrics), String> {
    let aggregate =
        query.select.iter().any(|item| is_agg(&item.expr)) || !query.group_by.is_empty();
    let mut batch_query = query.clone();
    batch_query.distinct = false;
    batch_query.order_by.clear();
    batch_query.limit = None;
    batch_query.offset = 0;
    let mut catalog = Catalog::load(path, Arc::new(Table::empty()))?;
    if aggregate {
        let template = states(query);
        let mut groups =
            std::collections::HashMap::<Vec<ScalarKey>, (Vec<Scalar>, Vec<AggState>)>::new();
        if query.group_by.is_empty() {
            groups.insert(Vec::new(), (Vec::new(), template.clone()));
        }
        let (_, scan) =
            Table::stream_parquet_direct(path, query, options.batch_size, |batch_table| {
                enforce_table_limit(options, &batch_table)?;
                catalog.events = Arc::new(batch_table);
                for row in execute_rel(&batch_query, &catalog)? {
                    let group_values: Vec<_> = query
                        .select
                        .iter()
                        .zip(&row)
                        .filter(|(item, _)| !is_agg(&item.expr))
                        .map(|(_, value)| value.clone())
                        .collect();
                    let key = group_values
                        .iter()
                        .map(scalar_group_key_ref)
                        .collect::<Vec<_>>();
                    if !groups.contains_key(&key) {
                        account_query_memory(
                            group_values
                                .iter()
                                .map(|value| match value {
                                    Scalar::Str(text) => text.len(),
                                    _ => std::mem::size_of::<Scalar>(),
                                })
                                .sum::<usize>()
                                + template.len() * std::mem::size_of::<AggState>()
                                + 64,
                            "streaming relational hash aggregation",
                        )?;
                        groups.insert(key.clone(), (group_values, template.clone()));
                    }
                    let states = &mut groups.get_mut(&key).expect("group inserted").1;
                    let mut aggregate_index = 0;
                    for (item, value) in query.select.iter().zip(&row) {
                        if is_agg(&item.expr) {
                            merge_finished_aggregate(&mut states[aggregate_index], value)?;
                            aggregate_index += 1;
                        }
                    }
                }
                Ok(())
            })?;
        let mut rows = Vec::with_capacity(groups.len());
        for (_, (group_values, aggregate_states)) in groups {
            let mut group_index = 0;
            let mut aggregate_index = 0;
            let row = query
                .select
                .iter()
                .map(|item| {
                    if is_agg(&item.expr) {
                        let value = finish(&aggregate_states[aggregate_index]);
                        aggregate_index += 1;
                        value
                    } else {
                        let value = group_values[group_index].clone();
                        group_index += 1;
                        value
                    }
                })
                .collect::<Vec<_>>();
            account_query_memory(row_bytes(&row), "result materialization")?;
            rows.push(row);
        }
        finalize_rows(query, &mut rows);
        return Ok((rows, scan));
    }

    let mut bounded_query = query.clone();
    bounded_query.limit = query.limit.map(|limit| limit.saturating_add(query.offset));
    bounded_query.offset = 0;
    let compact_each_batch = query.distinct || query.limit.is_some();
    let mut rows = Vec::new();
    let (_, scan) = Table::stream_parquet_direct(path, query, options.batch_size, |batch_table| {
        enforce_table_limit(options, &batch_table)?;
        catalog.events = Arc::new(batch_table);
        rows.extend(execute_rel(&batch_query, &catalog)?);
        if compact_each_batch {
            finalize_rows(&bounded_query, &mut rows);
        }
        Ok(())
    })?;
    finalize_rows(query, &mut rows);
    Ok((rows, scan))
}

pub(crate) fn execute_parquet_stream(
    q: &Query,
    path: &str,
    pool: &Pool,
    options: &Options,
) -> Result<(Vec<Vec<Scalar>>, ParquetScanMetrics), String> {
    if streamable_parquet_join(q) {
        return execute_parquet_streaming_join(q, path, options);
    }
    if is_relational(q) {
        let (table, mut scan) = Table::load_parquet_direct(path, q, options.batch_size)?;
        scan.streaming_fallback = true;
        let table = Arc::new(table);
        enforce_table_limit(options, &table)?;
        let rows = execute_rel(q, &Catalog::load(path, table)?)?;
        return Ok((rows, scan));
    }
    let metadata_count = q.select.len() == 1
        && q.filter.is_none()
        && q.group_by.is_empty()
        && q.having.is_none()
        && matches!(&q.select[0].expr, Expr::Func(name, argument)
            if name == "count" && matches!(argument.as_ref(), Expr::Star));
    if metadata_count {
        let scan = Table::parquet_scan_plan(path, q)?;
        let mut rows = vec![vec![Scalar::Int(scan.rows_read as i64)]];
        account_query_memory(row_bytes(&rows[0]), "result materialization")?;
        finalize_rows(q, &mut rows);
        return Ok((rows, scan));
    }
    let aggregate = q.select.iter().any(|item| is_agg(&item.expr)) || !q.group_by.is_empty();
    if aggregate {
        let template = states(q);
        let mut final_groups = GroupTable::new()?;
        let (dictionaries, scan) =
            Table::stream_parquet_direct(path, q, options.batch_size, |batch_table| {
                enforce_table_limit(options, &batch_table)?;
                let groups = pool.aggregate(
                    Arc::new(q.clone()),
                    Arc::new(batch_table),
                    options.batch_size,
                )?;
                for entry in groups.into_entries() {
                    let target = final_groups.get_or_insert(entry.k, &template)?;
                    merge(target, &entry.s);
                }
                Ok(())
            })?;
        let mut rows = finish_group_rows(q, &dictionaries, final_groups)?;
        finalize_rows(q, &mut rows);
        return Ok((rows, scan));
    }
    let mut batch_query = q.clone();
    batch_query.distinct = false;
    batch_query.order_by.clear();
    batch_query.limit = None;
    batch_query.offset = 0;
    let mut bounded_query = q.clone();
    bounded_query.limit = q.limit.map(|limit| limit.saturating_add(q.offset));
    bounded_query.offset = 0;
    let compact_each_batch = q.distinct || q.limit.is_some();
    let mut rows = Vec::new();
    let (_, scan) = Table::stream_parquet_direct(path, q, options.batch_size, |batch_table| {
        enforce_table_limit(options, &batch_table)?;
        rows.extend(execute(
            &batch_query,
            Arc::new(batch_table),
            pool,
            options.batch_size,
        )?);
        if compact_each_batch {
            finalize_rows(&bounded_query, &mut rows);
        }
        Ok(())
    })?;
    finalize_rows(q, &mut rows);
    Ok((rows, scan))
}
pub(crate) fn rows_json(rows: &[Vec<Scalar>]) -> String {
    format!(
        "[{}]",
        rows.iter()
            .map(|row| row_json(row))
            .collect::<Vec<_>>()
            .join(",")
    )
}
pub(crate) fn row_json(row: &[Scalar]) -> String {
    format!(
        "[{}]",
        row.iter().map(Scalar::json).collect::<Vec<_>>().join(",")
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

#[allow(clippy::too_many_arguments)]
fn print_query_stats(
    options: &Options,
    query: &Query,
    rows_scanned: usize,
    rows_returned: usize,
    batches_scanned: usize,
    elapsed_ns: u128,
    query_memory: &QueryMemory,
    scan: &ParquetScanMetrics,
    spill: &SpillMetrics,
    result_streamed: bool,
    stream: &ResultStreamMetrics,
) {
    eprintln!(
        "{{\"rows_scanned\":{rows_scanned},\"rows_returned\":{rows_returned},\"batches_scanned\":{batches_scanned},\"columns_scanned\":{},\"worker_threads\":{},\"logical_partitions\":{},\"elapsed_ns\":{elapsed_ns},\"query_memory_limit_bytes\":{},\"query_memory_accounted_bytes\":{},\"query_memory_peak_bytes\":{},\"parquet_total_rows\":{},\"parquet_rows_read\":{},\"parquet_total_row_groups\":{},\"parquet_row_groups_read\":{},\"parquet_total_columns\":{},\"parquet_columns_read\":{},\"parquet_compressed_bytes_read\":{},\"parquet_batches_read\":{},\"parquet_peak_decoded_batch_bytes\":{},\"parquet_streaming_fallback\":{},\"spill_files_created\":{},\"spill_partitions\":{},\"spill_bytes_written\":{},\"spill_bytes_read\":{},\"spill_passes\":{},\"spilled\":{},\"result_streamed\":{result_streamed},\"result_output_bytes\":{},\"result_batches\":{}}}",
        query.columns.len(),
        options.threads,
        options.threads * 4,
        query_memory.limit_bytes(),
        query_memory.accounted_bytes(),
        query_memory.peak_accounted_bytes(),
        scan.total_rows,
        scan.rows_read,
        scan.total_row_groups,
        scan.row_groups_read,
        scan.total_columns,
        scan.columns_read,
        scan.compressed_bytes_read,
        scan.batches_read,
        scan.peak_decoded_batch_bytes,
        scan.streaming_fallback,
        spill.files_created,
        spill.partitions,
        spill.bytes_written,
        spill.bytes_read,
        spill.passes,
        spill.spilled(),
        stream.output_bytes,
        stream.batches_scanned
    );
}

pub fn run_query(o: Options, sql: &str, explain: bool, stats: bool) -> Result<(), String> {
    if o.direct_parquet && o.streaming_parquet {
        return Err("choose either --direct-parquet or --streaming-parquet".into());
    }
    let parquet_query = o.direct_parquet || o.streaming_parquet;
    if parquet_query && !o.data.ends_with(".parquet") {
        return Err("direct and streaming Parquet execution require a .parquet data file".into());
    }
    let t = Arc::new(if parquet_query {
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
    if parquet_query {
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
        if o.streaming_parquet {
            q.physical.insert(
                2,
                format!(
                    "ParquetStreamExec(batch_size={};fallback={})",
                    o.batch_size,
                    parquet_streaming_fallback(&q)
                ),
            );
        }
    }
    if o.spill_dir.is_some() && !o.streaming_parquet && spillable_aggregate(&q) {
        q.physical
            .insert(1, "SpillAggregateExec(partitions=auto)".into());
    }
    if o.stream_results {
        if !streamable_result(&q) {
            return Err("STREAMING_UNSUPPORTED result streaming requires a single events scan with projection/filter and no DISTINCT, aggregation, ORDER BY, joins, windows, CTEs, unions, or subqueries".into());
        }
        q.physical.insert(
            if parquet_query { 2 } else { 1 },
            format!("ResultStreamExec(batch_size={})", o.batch_size),
        );
    }
    if explain {
        println!("{}", q.explain(o.batch_size));
        return Ok(());
    }
    let pool = Pool::new(o.threads);
    let started = Instant::now();
    if o.stream_results {
        let stdout = std::io::stdout();
        let mut writer = BufWriter::with_capacity(8192, stdout.lock());
        let (result, query_memory) = with_query_memory(o.query_memory_limit_mb, || {
            account_query_memory(8192, "streaming output buffer")?;
            let execution = {
                let mut write_row = |row: &[Scalar]| -> Result<usize, String> {
                    let mut line = row_json(row);
                    line.push('\n');
                    let bytes = line.len();
                    account_query_memory(bytes, "streaming output encoding")?;
                    let result = writer
                        .write_all(line.as_bytes())
                        .map_err(|error| format!("cannot write streaming result: {error}"));
                    if let Some(memory) = current_query_memory() {
                        memory.release(bytes);
                    }
                    result.map(|()| bytes)
                };
                if o.streaming_parquet {
                    stream_parquet_results(
                        &q,
                        &o.data,
                        o.batch_size,
                        o.memory_limit_mb,
                        &mut write_row,
                    )
                } else if o.direct_parquet {
                    let (table, scan) = Table::load_parquet_direct(&o.data, &q, o.batch_size)?;
                    enforce_table_limit(&o, &table)?;
                    stream_table_results(&q, &table, o.batch_size, &mut write_row)
                        .map(|metrics| (metrics, scan))
                } else {
                    stream_table_results(&q, &t, o.batch_size, &mut write_row)
                        .map(|metrics| (metrics, ParquetScanMetrics::default()))
                }
            };
            let flush = writer
                .flush()
                .map_err(|error| format!("cannot flush streaming result: {error}"));
            if let Some(memory) = current_query_memory() {
                memory.release(8192);
            }
            let output = execution?;
            flush?;
            Ok(output)
        });
        let (stream, scan) = result?;
        if stats {
            let parquet = o.direct_parquet || o.streaming_parquet;
            print_query_stats(
                &o,
                &q,
                if parquet { scan.rows_read } else { t.len() },
                stream.rows_returned,
                if parquet {
                    scan.batches_read
                } else {
                    stream.batches_scanned
                },
                started.elapsed().as_nanos(),
                &query_memory,
                &scan,
                &SpillMetrics::default(),
                true,
                &stream,
            );
        }
        return Ok(());
    }
    let (execution_table, planned_scan) = if o.direct_parquet {
        let (table, scan) = Table::load_parquet_direct(&o.data, &q, o.batch_size)?;
        (Arc::new(table), scan)
    } else {
        (t.clone(), ParquetScanMetrics::default())
    };
    enforce_table_limit(&o, &execution_table)?;
    let (result, query_memory) = with_query_memory(o.query_memory_limit_mb, || {
        if o.streaming_parquet {
            execute_parquet_stream(&q, &o.data, &pool, &o)
                .map(|(rows, scan)| (rows, scan, SpillMetrics::default()))
        } else if is_relational(&q) {
            Ok((
                execute_rel(&q, &Catalog::load(&o.data, execution_table.clone())?)?,
                planned_scan.clone(),
                SpillMetrics::default(),
            ))
        } else {
            let (rows, spill) = execute_with_spill(
                &q,
                execution_table.clone(),
                &pool,
                o.batch_size,
                o.spill_dir.as_deref(),
            )?;
            Ok((rows, planned_scan.clone(), spill))
        }
    });
    let (rows, scan, spill) = result?;
    let elapsed_ns = started.elapsed().as_nanos();
    for row in &rows {
        println!("{}", row_json(row))
    }
    if stats {
        print_query_stats(
            &o,
            &q,
            if parquet_query {
                scan.rows_read
            } else {
                execution_table.len()
            },
            rows.len(),
            if parquet_query {
                scan.batches_read
            } else {
                execution_table.len().div_ceil(o.batch_size)
            },
            elapsed_ns,
            &query_memory,
            &scan,
            &spill,
            false,
            &ResultStreamMetrics::default(),
        );
    }
    Ok(())
}

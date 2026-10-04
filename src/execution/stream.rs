use super::row_bytes;
use super::scalar::eval;
use crate::optimizer::filter_always_false;
use crate::sql::*;
use crate::storage::{ParquetScanMetrics, Table};
use crate::types::*;
use std::sync::Arc;

struct MemoryReservation {
    memory: Option<Arc<QueryMemory>>,
    bytes: usize,
}

impl MemoryReservation {
    fn new(bytes: usize, operator: &str) -> Result<Self, String> {
        account_query_memory(bytes, operator)?;
        Ok(Self {
            memory: current_query_memory(),
            bytes,
        })
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        if let Some(memory) = &self.memory {
            memory.release(self.bytes);
        }
    }
}

pub(crate) fn streamable_result(query: &Query) -> bool {
    query.from.name == "events"
        && query.joins.is_empty()
        && query.group_by.is_empty()
        && query.having.is_none()
        && query.order_by.is_empty()
        && query.union.is_none()
        && query.ctes.is_empty()
        && !query.distinct
        && !query.select.iter().any(|item| {
            contains_agg(&item.expr) || contains_window(&item.expr) || contains_subquery(&item.expr)
        })
        && !query.filter.as_ref().is_some_and(contains_subquery)
}

struct ResultStreamer<'a, Sink>
where
    Sink: FnMut(&[Scalar]) -> Result<usize, String>,
{
    query: &'a Query,
    batch_size: usize,
    remaining_offset: usize,
    remaining_limit: Option<usize>,
    sink: Sink,
    metrics: ResultStreamMetrics,
}

impl<'a, Sink> ResultStreamer<'a, Sink>
where
    Sink: FnMut(&[Scalar]) -> Result<usize, String>,
{
    fn new(query: &'a Query, batch_size: usize, sink: Sink) -> Self {
        Self {
            query,
            batch_size: batch_size.max(1),
            remaining_offset: query.offset,
            remaining_limit: query.limit,
            sink,
            metrics: ResultStreamMetrics::default(),
        }
    }

    fn finished(&self) -> bool {
        self.remaining_limit == Some(0)
    }

    fn consume(&mut self, table: &Table) -> Result<(), String> {
        if self.finished() || filter_always_false(self.query.filter.as_ref()) {
            return Ok(());
        }
        for start in (0..table.len()).step_by(self.batch_size) {
            if execution_cancelled() {
                return Err("query cancelled during result streaming".into());
            }
            self.metrics.batches_scanned += 1;
            let capacity = self.batch_size.min(table.len() - start);
            let selection_bytes = capacity.saturating_mul(std::mem::size_of::<usize>());
            let _selection_memory =
                MemoryReservation::new(selection_bytes, "streaming scan selection")?;
            let mut selection = Vec::with_capacity(capacity);
            for row in start..(start + self.batch_size).min(table.len()) {
                if self
                    .query
                    .filter
                    .as_ref()
                    .is_none_or(|filter| eval(filter, table, row).truthy())
                {
                    selection.push(row);
                }
            }
            for row_index in selection {
                if self.remaining_offset > 0 {
                    self.remaining_offset -= 1;
                    continue;
                }
                if self.finished() {
                    return Ok(());
                }
                let row = self
                    .query
                    .select
                    .iter()
                    .map(|item| eval(&item.expr, table, row_index))
                    .collect::<Vec<_>>();
                let _row_memory = MemoryReservation::new(row_bytes(&row), "streaming result row")?;
                self.metrics.output_bytes = self
                    .metrics
                    .output_bytes
                    .saturating_add((self.sink)(&row)? as u64);
                self.metrics.rows_returned += 1;
                if let Some(remaining) = &mut self.remaining_limit {
                    *remaining -= 1;
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn stream_table_results<Sink>(
    query: &Query,
    table: &Table,
    batch_size: usize,
    sink: Sink,
) -> Result<ResultStreamMetrics, String>
where
    Sink: FnMut(&[Scalar]) -> Result<usize, String>,
{
    let mut streamer = ResultStreamer::new(query, batch_size, sink);
    streamer.consume(table)?;
    Ok(streamer.metrics)
}

pub(crate) fn stream_parquet_results<Sink>(
    query: &Query,
    path: &str,
    batch_size: usize,
    memory_limit_mb: usize,
    sink: Sink,
) -> Result<(ResultStreamMetrics, ParquetScanMetrics), String>
where
    Sink: FnMut(&[Scalar]) -> Result<usize, String>,
{
    let mut streamer = ResultStreamer::new(query, batch_size, sink);
    let (_, scan) = Table::stream_parquet_direct(path, query, batch_size, |table| {
        if memory_limit_mb > 0 && table.approximate_bytes() > memory_limit_mb * 1024 * 1024 {
            return Err(format!(
                "RESOURCE_EXHAUSTED streaming Parquet batch requires approximately {} bytes",
                table.approximate_bytes()
            ));
        }
        streamer.consume(&table)
    })?;
    Ok((streamer.metrics, scan))
}

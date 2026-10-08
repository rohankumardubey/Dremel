//! SQL over one named Arrow table with types derived from its schema.

mod benchmark;
mod bind;
pub use benchmark::run_columnar_bench_server;
mod execute;
#[cfg(test)]
mod tests;

use crate::execution::row_json;
use crate::storage::ColumnarTable;
use crate::types::{Options, Scalar, with_query_memory};
use arrow::datatypes::SchemaRef;
use std::time::Instant;

/// A query bound to one table name and a concrete Arrow field layout.
pub struct ColumnarQuery {
    query: crate::sql::Query,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
}

/// SQL result fields and rows in the engine's canonical scalar representation.
pub struct QueryResult {
    pub schema: SchemaRef,
    pub rows: Vec<Vec<Scalar>>,
}

impl QueryResult {
    /// Writes one canonical typed JSON row per line after successful execution.
    pub fn write_ndjson(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        for row in &self.rows {
            writeln!(writer, "{}", row_json(row))?;
        }
        Ok(())
    }
}

impl ColumnarQuery {
    pub fn prepare(table_name: &str, schema: SchemaRef, sql: &str) -> Result<Self, String> {
        bind::prepare(table_name, schema, sql)
    }

    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    pub fn explain(&self) -> String {
        self.query.explain(4096)
    }

    pub fn execute(&self, table: &ColumnarTable) -> Result<QueryResult, String> {
        self.execute_with_memory_limit(table, 0)
    }

    /// Limits query workspace in MiB. Zero disables the limit; resident input
    /// Arrow buffers are owned by the caller and excluded from this budget.
    pub fn execute_with_memory_limit(
        &self,
        table: &ColumnarTable,
        limit_mb: usize,
    ) -> Result<QueryResult, String> {
        if table.schema().fields() != self.input_schema.fields() {
            return Err("query input schema changed; prepare the query again".into());
        }
        let (rows, _) = with_query_memory(limit_mb, || execute::execute(&self.query, table));
        Ok(QueryResult {
            schema: self.output_schema.clone(),
            rows: rows?,
        })
    }
}

pub fn run_columnar_query(
    options: Options,
    table_name: &str,
    sql: &str,
    explain: bool,
    stats: bool,
) -> Result<(), String> {
    let table = load_table(&options)?;
    let query = ColumnarQuery::prepare(table_name, table.schema().clone(), sql)?;
    if explain {
        println!("{}", query.explain());
        return Ok(());
    }
    let started = Instant::now();
    let (rows, memory) = with_query_memory(options.query_memory_limit_mb, || {
        execute::execute(&query.query, &table)
    });
    let rows = rows?;
    let returned = rows.len();
    if options.max_result_rows > 0 && rows.len() > options.max_result_rows {
        return Err("RESOURCE_EXHAUSTED named-table result exceeds --max-result-rows".into());
    }
    QueryResult {
        schema: query.output_schema.clone(),
        rows,
    }
    .write_ndjson(&mut std::io::stdout().lock())
    .map_err(|error| error.to_string())?;
    if stats {
        eprintln!(
            "{{\"rows_scanned\":{},\"rows_returned\":{returned},\"columns_scanned\":{},\"elapsed_ns\":{},\"query_memory_peak_bytes\":{}}}",
            table.row_count(),
            query.query.columns.len(),
            started.elapsed().as_nanos(),
            memory.peak_accounted_bytes()
        );
    }
    Ok(())
}

fn load_table(options: &Options) -> Result<ColumnarTable, String> {
    if options.direct_parquet
        || options.streaming_parquet
        || options.stream_results
        || options.spill_dir.is_some()
    {
        return Err("named-table queries currently use eager Arrow/Parquet input and materialized results; direct/streaming/spill flags are unsupported".into());
    }
    let table = ColumnarTable::read(&options.data)?;
    if options.memory_limit_mb > 0
        && table.approximate_bytes() > options.memory_limit_mb.saturating_mul(1024 * 1024)
    {
        return Err("RESOURCE_EXHAUSTED named table exceeds --memory-limit-mb".into());
    }
    Ok(table)
}

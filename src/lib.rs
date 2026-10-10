#![forbid(unsafe_code)]

mod execution;
mod optimizer;
mod query;
mod relational;
mod scan;
mod server;
mod sql;
mod storage;
mod types;

pub mod nested;

pub use execution::run_query;
pub use query::{ColumnarQuery, QueryResult, run_columnar_bench_server, run_columnar_query};
pub use scan::{run_parquet_scan, run_scan_cli};
pub use server::run_bench_server;
pub use storage::ColumnarTable;
pub use storage::{ParquetScan, ParquetScanOptions, ParquetScanPlan};
pub use types::{Options, Scalar};

#[cfg(test)]
mod tests;

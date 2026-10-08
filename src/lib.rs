#![forbid(unsafe_code)]

mod execution;
mod optimizer;
mod query;
mod relational;
mod server;
mod sql;
mod storage;
mod types;

pub mod nested;

pub use execution::run_query;
pub use query::{ColumnarQuery, QueryResult, run_columnar_bench_server, run_columnar_query};
pub use server::run_bench_server;
pub use storage::ColumnarTable;
pub use types::{Options, Scalar};

#[cfg(test)]
mod tests;

//! CLI adapter for the typed nested Parquet scanner.
mod cli;
mod export;
use crate::{ParquetScan, ParquetScanOptions};
pub use cli::run_scan_cli;
use export::IpcExport;
use std::path::Path;
use std::time::Instant;

pub fn run_parquet_scan(
    data: &str,
    columns: Vec<String>,
    groups: Option<Vec<usize>>,
    batch_size: usize,
    output: Option<&str>,
) -> Result<(), String> {
    let started = Instant::now();
    let mut scan = ParquetScan::open(
        data,
        ParquetScanOptions {
            columns,
            row_groups: groups,
            batch_size,
        },
    )?;
    let mut writer = output
        .map(|path| IpcExport::new(Path::new(path), &scan.plan().schema))
        .transpose()?;
    for batch in scan.by_ref() {
        let batch = batch?;
        if let Some(writer) = &mut writer {
            writer.write(&batch)?;
        }
    }
    if let Some(writer) = writer {
        writer.commit()?;
    }
    let plan = scan.plan();
    println!(
        "{}",
        serde_json::json!({
            "rows": scan.rows_read, "batches": scan.batches_read, "elapsed_ns": started.elapsed().as_nanos(),
            "selected_leaf_columns": plan.selected_leaf_columns, "total_leaf_columns": plan.total_leaf_columns,
            "row_groups": plan.selected_row_groups, "total_row_groups": plan.total_row_groups,
            "selected_rows": plan.selected_rows, "selected_compressed_bytes": plan.selected_compressed_bytes,
            "peak_decoded_batch_bytes": scan.peak_decoded_batch_bytes,
            "leaf_paths": plan.leaf_paths, "max_definition_levels": plan.max_definition_levels,
            "max_repetition_levels": plan.max_repetition_levels, "schema": plan.schema.to_string(), "output": output,
        })
    );
    Ok(())
}

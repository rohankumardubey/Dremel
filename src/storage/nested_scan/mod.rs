//! Projected, batch-streaming scans delegated to Apache Parquet's nested reader.
mod projection;
#[cfg(test)]
mod tests;

use arrow::array::RecordBatchReader;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use std::collections::HashSet;
use std::fs::File;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct ParquetScanOptions {
    /// Case-sensitive logical paths. Empty means all columns. LIST elements
    /// are traversed implicitly, e.g. `items.price`, without exploding rows.
    pub columns: Vec<String>,
    /// None selects every row group. Some(empty) deliberately selects no rows.
    /// Groups are read in file order; duplicates and invalid indexes are errors.
    pub row_groups: Option<Vec<usize>>,
    pub batch_size: usize,
}

impl Default for ParquetScanOptions {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            row_groups: None,
            batch_size: 4096,
        }
    }
}

#[derive(Debug)]
pub struct ParquetScanPlan {
    pub schema: SchemaRef,
    pub selected_leaf_columns: Vec<usize>,
    pub total_leaf_columns: usize,
    pub selected_row_groups: Vec<usize>,
    pub total_row_groups: usize,
    pub selected_rows: u64,
    /// Selected column-chunk compressed sizes, not observed filesystem I/O.
    pub selected_compressed_bytes: u64,
    pub leaf_paths: Vec<String>,
    pub max_definition_levels: Vec<i16>,
    pub max_repetition_levels: Vec<i16>,
}

/// A single-pass reader. Retaining returned batches retains their Arrow buffers;
/// the reader itself does not collect all decoded batches in memory.
pub struct ParquetScan {
    reader: ParquetRecordBatchReader,
    plan: ParquetScanPlan,
    finished: bool,
    pub rows_read: usize,
    pub batches_read: usize,
    pub peak_decoded_batch_bytes: usize,
}

impl ParquetScan {
    pub fn open(path: impl AsRef<Path>, options: ParquetScanOptions) -> Result<Self, String> {
        if options.batch_size == 0 {
            return Err("Parquet scan batch size must be positive".into());
        }
        let file = File::open(path.as_ref())
            .map_err(|error| format!("cannot open {}: {error}", path.as_ref().display()))?;
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| error.to_string())?;
        let metadata = builder.metadata();
        let descriptor = builder.parquet_schema();
        let paths = projection::leaf_paths(builder.schema())?;
        if paths.len() != descriptor.num_columns() {
            return Err("Arrow/Parquet leaf layout differs; cannot project safely".into());
        }
        let selected = projection::select(&paths, &options.columns)?;
        let mut groups = options
            .row_groups
            .unwrap_or_else(|| (0..metadata.num_row_groups()).collect());
        let mut unique = HashSet::new();
        for &index in &groups {
            if index >= metadata.num_row_groups() {
                return Err(format!("invalid row group {index}"));
            }
            if !unique.insert(index) {
                return Err(format!("duplicate row group {index}"));
            }
        }
        groups.sort_unstable();
        let mut selected_rows = 0u64;
        let mut selected_bytes = 0u64;
        for &group in &groups {
            let row_group = metadata.row_group(group);
            selected_rows = selected_rows
                .checked_add(
                    row_group
                        .num_rows()
                        .try_into()
                        .map_err(|_| "negative row count")?,
                )
                .ok_or("row count overflow")?;
            for &column in &selected {
                selected_bytes = selected_bytes
                    .checked_add(
                        row_group
                            .column(column)
                            .compressed_size()
                            .try_into()
                            .map_err(|_| "negative compressed size")?,
                    )
                    .ok_or("compressed size overflow")?;
            }
        }
        let mut plan = ParquetScanPlan {
            schema: builder.schema().clone(),
            selected_leaf_columns: selected.clone(),
            total_leaf_columns: descriptor.num_columns(),
            selected_row_groups: groups.clone(),
            total_row_groups: metadata.num_row_groups(),
            selected_rows,
            selected_compressed_bytes: selected_bytes,
            leaf_paths: selected
                .iter()
                .map(|&index| paths[index].join("."))
                .collect(),
            max_definition_levels: selected
                .iter()
                .map(|&index| descriptor.column(index).max_def_level())
                .collect(),
            max_repetition_levels: selected
                .iter()
                .map(|&index| descriptor.column(index).max_rep_level())
                .collect(),
        };
        let mask = ProjectionMask::leaves(descriptor, selected);
        let reader = builder
            .with_batch_size(options.batch_size)
            .with_projection(mask)
            .with_row_groups(groups)
            .build()
            .map_err(|error| error.to_string())?;
        plan.schema = reader.schema();
        Ok(Self {
            reader,
            plan,
            finished: false,
            rows_read: 0,
            batches_read: 0,
            peak_decoded_batch_bytes: 0,
        })
    }

    pub fn plan(&self) -> &ParquetScanPlan {
        &self.plan
    }
}

impl Iterator for ParquetScan {
    type Item = Result<RecordBatch, String>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let result = match self.reader.next() {
            Some(result) => result,
            None => {
                self.finished = true;
                return (self.rows_read as u64 != self.plan.selected_rows).then(|| {
                    Err("Parquet scan ended before all selected rows were decoded".into())
                });
            }
        };
        Some(
            result
                .map_err(|error| {
                    self.finished = true;
                    error.to_string()
                })
                .inspect(|batch| {
                    self.rows_read += batch.num_rows();
                    self.batches_read += 1;
                    self.peak_decoded_batch_bytes = self
                        .peak_decoded_batch_bytes
                        .max(batch.get_array_memory_size());
                }),
        )
    }
}

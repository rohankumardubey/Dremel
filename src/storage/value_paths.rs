//! Typed array projection into the index domain of a correlated list element.
use super::{ColumnarTable, list_range, values::with_parent_nulls};
use crate::types::{account_query_memory, execution_cancelled};
use arrow::array::{Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, StructArray};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::record_batch::RecordBatch;

pub(crate) fn list_sources(
    table: &ColumnarTable,
    paths: &[&str],
) -> Result<Vec<Vec<ArrayRef>>, String> {
    table
        .batches()
        .iter()
        .map(|batch| paths.iter().map(|path| project_path(batch, path)).collect())
        .collect()
}

/// Internal [] segments enter list values; they are never exposed as SQL syntax.
pub(crate) fn project_path(batch: &RecordBatch, path: &str) -> Result<ArrayRef, String> {
    let steps = path.replace("[]", ".[]");
    let mut segments = steps.split('.');
    let root = segments.next().ok_or("empty column path")?;
    let index = batch
        .schema()
        .fields()
        .iter()
        .position(|field| field.name().eq_ignore_ascii_case(root))
        .ok_or_else(|| format!("unknown column {path}"))?;
    let mut array = batch.column(index).clone();
    for segment in segments {
        if execution_cancelled() {
            return Err("query cancelled during nested array projection".into());
        }
        if segment == "[]" {
            array = visible_values(&array)?;
        } else {
            let structure = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| format!("{path}: LIST traversal requires UNNEST"))?;
            let index = structure
                .fields()
                .iter()
                .position(|field| field.name().eq_ignore_ascii_case(segment))
                .ok_or_else(|| format!("unknown field {path}"))?;
            array = with_parent_nulls(structure.column(index).clone(), array.nulls())?;
        }
    }
    Ok(array)
}

fn visible_values(array: &ArrayRef) -> Result<ArrayRef, String> {
    let values = if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        list.values().clone()
    } else if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        list.values().clone()
    } else if let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        list.values().clone()
    } else {
        return Err("UNNEST source must be a list".into());
    };
    account_query_memory(values.len(), "UNNEST visibility workspace")?;
    let mut visible = vec![false; values.len()];
    for row in 0..array.len() {
        if row % 4096 == 0 && execution_cancelled() {
            return Err("query cancelled during UNNEST validity projection".into());
        }
        if array.is_valid(row) {
            visible[list_range(array, row)].fill(true);
        }
    }
    let validity = NullBuffer::new(BooleanBuffer::from(visible));
    with_parent_nulls(values, Some(&validity))
}

use super::{ColumnAccess, Expansion, value};
use crate::relational::eval_with_columns;
use crate::sql::JoinKind;
use crate::storage::{ValueBatch, list_range};
use crate::types::execution_cancelled;
use arrow::array::{Array, ArrayRef};
use std::collections::HashMap;

/// Only row indices and ordinalities are retained, never expanded parent rows.
#[derive(Clone)]
pub(in crate::query) struct Position {
    pub indices: Vec<Option<usize>>,
    pub ordinalities: Vec<Option<i64>>,
}

impl Position {
    pub(in crate::query) fn new(row: usize, stages: usize) -> Self {
        let mut indices = vec![None; stages + 1];
        indices[0] = Some(row);
        Self {
            indices,
            ordinalities: vec![None; stages + 1],
        }
    }

    pub(in crate::query) fn heap_bytes(&self) -> usize {
        self.indices.capacity() * std::mem::size_of::<Option<usize>>()
            + self.ordinalities.capacity() * std::mem::size_of::<Option<i64>>()
    }
}

/// Returns false on a consumer's early stop (for example unordered LIMIT).
pub(in crate::query) fn visit(
    stages: &[Expansion],
    arrays: &[ArrayRef],
    batch: &ValueBatch,
    columns: &HashMap<String, ColumnAccess>,
    position: &mut Position,
    consumer: &mut impl FnMut(&Position) -> Result<bool, String>,
) -> Result<bool, String> {
    Walker {
        stages,
        arrays,
        batch,
        columns,
    }
    .walk(0, position, consumer)
}

struct Walker<'a> {
    stages: &'a [Expansion],
    arrays: &'a [ArrayRef],
    batch: &'a ValueBatch,
    columns: &'a HashMap<String, ColumnAccess>,
}

impl Walker<'_> {
    fn walk(
        &self,
        level: usize,
        position: &mut Position,
        consumer: &mut impl FnMut(&Position) -> Result<bool, String>,
    ) -> Result<bool, String> {
        if execution_cancelled() {
            return Err("query cancelled during correlated UNNEST".into());
        }
        if level == self.stages.len() {
            return consumer(position);
        }
        let stage = &self.stages[level];
        let range = position.indices[stage.source_domain]
            .filter(|row| self.arrays[level].is_valid(*row))
            .map_or(0..0, |row| list_range(&self.arrays[level], row));
        let mut matched = false;
        for (ordinal, index) in range.enumerate() {
            if ordinal % 4096 == 0 && execution_cancelled() {
                return Err("query cancelled during correlated UNNEST".into());
            }
            position.indices[level + 1] = Some(index);
            position.ordinalities[level + 1] =
                Some(i64::try_from(ordinal + 1).map_err(|_| "UNNEST ordinality exceeds BIGINT")?);
            let column = |name: &str| value(name, self.batch, self.columns, position);
            if stage
                .on
                .as_ref()
                .is_some_and(|on| !eval_with_columns(on, &column).truthy())
            {
                continue;
            }
            matched = true;
            if !self.walk(level + 1, position, consumer)? {
                return Ok(false);
            }
        }
        if !matched && stage.kind == JoinKind::Left {
            position.indices[level + 1] = None;
            position.ordinalities[level + 1] = None;
            if !self.walk(level + 1, position, consumer)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

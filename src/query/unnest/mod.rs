mod bind;
mod cursor;
mod scope;
#[cfg(test)]
mod tests;

use crate::sql::{Expr, JoinKind};
use crate::storage::ValueBatch;
use crate::types::Scalar;
pub(super) use bind::{bind_expansions, collect_join_paths, column_access};
pub(super) use cursor::{Position, visit};
use std::collections::HashMap;

pub(super) enum ColumnAccess {
    Value { path: String, domain: usize },
    Ordinality { domain: usize },
}

pub(super) struct Expansion {
    pub alias: String,
    pub column: String,
    pub ordinality: Option<String>,
    pub path: String,
    pub source_domain: usize,
    pub kind: JoinKind,
    pub on: Option<Expr>,
}

pub(super) fn scan_paths<'a>(
    columns: &'a HashMap<String, ColumnAccess>,
    stages: &'a [Expansion],
) -> Vec<&'a str> {
    columns
        .values()
        .filter_map(|column| match column {
            ColumnAccess::Value { path, .. } => Some(path.as_str()),
            ColumnAccess::Ordinality { .. } => None,
        })
        .chain(stages.iter().map(|stage| stage.path.as_str()))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub(super) fn value(
    name: &str,
    batch: &ValueBatch,
    columns: &HashMap<String, ColumnAccess>,
    position: &Position,
) -> Scalar {
    match &columns[name] {
        ColumnAccess::Value { domain, .. } => {
            position.indices[*domain].map_or(Scalar::Null, |index| batch.scalar(name, index))
        }
        ColumnAccess::Ordinality { domain } => {
            position.ordinalities[*domain].map_or(Scalar::Null, Scalar::Int)
        }
    }
}

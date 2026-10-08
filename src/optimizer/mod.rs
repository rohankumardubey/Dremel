use crate::execution::scalar::{apply_binary, cast_value, decimal_text, eval_values, like_matches};
use crate::sql::*;
use crate::storage::{Bindings, Table, query_bindings, resolve_column};
use crate::types::Scalar;
use std::collections::{BTreeMap, HashMap, HashSet};

mod estimates;
mod expressions;
mod plan;
mod predicates;
mod pushdown;

use estimates::estimated_filtered_rows;
pub(crate) use expressions::{literal_value, optimize_expression, prepare_expr};
#[cfg(test)]
pub(crate) use plan::optimize_query;
pub(crate) use plan::prepare;
use predicates::{contradictory_filter, reorder_conjuncts};
pub(crate) use pushdown::{
    PushedFilter, filter_always_false, pushed_filters, residual_filter, split_conjuncts,
};
use pushdown::{column_literal_filter, owned_conjuncts};

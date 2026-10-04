use super::*;

pub(crate) fn join_equality<'a>(
    expression: &'a Expr,
    right_table: &str,
    bindings: &std::collections::HashMap<String, String>,
) -> Option<(&'a Expr, &'a Expr)> {
    let Expr::Binary(operator, left, right) = expression else {
        return None;
    };
    if operator != "=" {
        return None;
    }
    let Expr::Column(left_column) = left.as_ref() else {
        return None;
    };
    let Expr::Column(right_column) = right.as_ref() else {
        return None;
    };
    let (left_relation, _) = resolve_column(left_column, bindings).ok()?;
    let (right_relation, _) = resolve_column(right_column, bindings).ok()?;
    if right_relation == right_table && left_relation != right_table {
        Some((left, right))
    } else if left_relation == right_table && right_relation != right_table {
        Some((right, left))
    } else {
        None
    }
}

pub(crate) fn apply_join(
    left_rows: Vec<RelRow>,
    join: &JoinSpec,
    catalog: &Catalog,
    bindings: &std::collections::HashMap<String, String>,
    optimizer_enabled: bool,
    filters: &[PushedFilter],
) -> Vec<RelRow> {
    if optimizer_enabled
        && matches!(join.kind, JoinKind::Inner | JoinKind::Left)
        && let Some(on) = join.on.as_ref()
        && let Some((left_key, right_key)) = join_equality(on, &join.table.name, bindings)
        && let Expr::Column(left_column) = left_key
        && let Expr::Column(right_column) = right_key
        && let Ok((left_table, left_column)) = resolve_column(left_column, bindings)
        && let Ok((right_table, right_column)) = resolve_column(right_column, bindings)
        && ((right_table == "users"
            && right_column == "user_id"
            && (catalog.users.row_count() == 0 || !catalog.users.index.is_empty()))
            || (right_table == "campaigns"
                && right_column == "campaign_id"
                && (catalog.campaigns.row_count() == 0 || !catalog.campaigns.index.is_empty())))
    {
        let right_filters: Vec<_> = filters
            .iter()
            .filter(|filter| filter.table == right_table)
            .collect();
        let mut output = Vec::new();
        for left in left_rows {
            if execution_cancelled() {
                break;
            }
            let key = match relation_scalar(catalog, left, &left_table, &left_column) {
                Scalar::Int(value) => Some(value),
                _ => None,
            };
            let candidates = key.and_then(|value| {
                if right_table == "users" {
                    catalog.users.index.get(&value)
                } else {
                    catalog.campaigns.index.get(&value)
                }
            });
            let mut matched = false;
            if let Some(candidates) = candidates {
                for &index in candidates {
                    let right = if right_table == "users" {
                        RelRow {
                            user: Some(index),
                            ..RelRow::default()
                        }
                    } else {
                        RelRow {
                            campaign: Some(index),
                            ..RelRow::default()
                        }
                    };
                    if !right_filters.iter().all(|filter| {
                        eval_rel(&filter.expression, catalog, right, bindings).truthy()
                    }) {
                        continue;
                    }
                    let combined = merge_rel_rows(left, right);
                    if eval_rel(on, catalog, combined, bindings).truthy() {
                        if !account_query_memory_or_stop(
                            std::mem::size_of::<RelRow>(),
                            "join output",
                        ) {
                            return Vec::new();
                        }
                        output.push(combined);
                        matched = true;
                    }
                }
            }
            if !matched && join.kind == JoinKind::Left {
                if !account_query_memory_or_stop(std::mem::size_of::<RelRow>(), "join output") {
                    return Vec::new();
                }
                output.push(left);
            }
        }
        return output;
    }
    let right_rows = base_relation_rows_filtered(&join.table.name, catalog, bindings, filters);
    if join.kind == JoinKind::Cross {
        let count = left_rows.len().saturating_mul(right_rows.len());
        if !account_query_memory_or_stop(
            count.saturating_mul(std::mem::size_of::<RelRow>()),
            "cross join output",
        ) {
            return Vec::new();
        }
        return left_rows
            .into_iter()
            .flat_map(|left| {
                right_rows
                    .iter()
                    .copied()
                    .map(move |right| merge_rel_rows(left, right))
            })
            .collect();
    }
    let on = join.on.as_ref().expect("non-cross join has ON");
    let equality = join_equality(on, &join.table.name, bindings);
    let mut output = Vec::new();
    if !account_query_memory_or_stop(right_rows.len(), "join match bitmap") {
        return Vec::new();
    }
    let mut matched_right = vec![false; right_rows.len()];
    if let (true, Some((left_key, right_key))) = (optimizer_enabled, equality) {
        let Expr::Column(left_column) = left_key else {
            unreachable!()
        };
        let Expr::Column(right_column) = right_key else {
            unreachable!()
        };
        let (left_table, left_column) =
            resolve_column(left_column, bindings).expect("bound join column");
        let (right_table, right_column) =
            resolve_column(right_column, bindings).expect("bound join column");
        let mut hash = std::collections::HashMap::<ScalarKey, Vec<usize>>::new();
        for (index, &right) in right_rows.iter().enumerate() {
            if let Some(key) =
                scalar_hash_key(relation_scalar(catalog, right, &right_table, &right_column))
            {
                if !hash.contains_key(&key)
                    && !account_query_memory_or_stop(
                        std::mem::size_of::<ScalarKey>() + 64,
                        "hash join build table",
                    )
                {
                    return Vec::new();
                }
                if !account_query_memory_or_stop(
                    std::mem::size_of::<usize>(),
                    "hash join build candidates",
                ) {
                    return Vec::new();
                }
                hash.entry(key).or_default().push(index);
            }
        }
        for left in left_rows {
            if execution_cancelled() {
                break;
            }
            let mut matched = false;
            if let Some(key) =
                scalar_hash_key(relation_scalar(catalog, left, &left_table, &left_column))
                && let Some(candidates) = hash.get(&key)
            {
                for &index in candidates {
                    let combined = merge_rel_rows(left, right_rows[index]);
                    if eval_rel(on, catalog, combined, bindings).truthy() {
                        if !account_query_memory_or_stop(
                            std::mem::size_of::<RelRow>(),
                            "join output",
                        ) {
                            return Vec::new();
                        }
                        output.push(combined);
                        matched = true;
                        matched_right[index] = true;
                    }
                }
            }
            if !matched && matches!(join.kind, JoinKind::Left | JoinKind::Full) {
                if !account_query_memory_or_stop(std::mem::size_of::<RelRow>(), "join output") {
                    return Vec::new();
                }
                output.push(left);
            }
        }
    } else {
        for left in left_rows {
            if execution_cancelled() {
                break;
            }
            let mut matched = false;
            for (index, &right) in right_rows.iter().enumerate() {
                let combined = merge_rel_rows(left, right);
                if eval_rel(on, catalog, combined, bindings).truthy() {
                    if !account_query_memory_or_stop(std::mem::size_of::<RelRow>(), "join output") {
                        return Vec::new();
                    }
                    output.push(combined);
                    matched = true;
                    matched_right[index] = true;
                }
            }
            if !matched && matches!(join.kind, JoinKind::Left | JoinKind::Full) {
                if !account_query_memory_or_stop(std::mem::size_of::<RelRow>(), "join output") {
                    return Vec::new();
                }
                output.push(left);
            }
        }
    }
    if matches!(join.kind, JoinKind::Right | JoinKind::Full) {
        let unmatched = matched_right.iter().filter(|matched| !**matched).count();
        if !account_query_memory_or_stop(
            unmatched.saturating_mul(std::mem::size_of::<RelRow>()),
            "join output",
        ) {
            return Vec::new();
        }
        output.extend(
            right_rows
                .into_iter()
                .zip(matched_right)
                .filter_map(|(row, matched)| (!matched).then_some(row)),
        );
    }
    output
}

use super::*;

pub(crate) fn compare_output_rows(
    left: &[Scalar],
    right: &[Scalar],
    order: &[(usize, &OrderSpec)],
) -> Ordering {
    for (index, spec) in order {
        let left_null = matches!(left[*index], Scalar::Null);
        let right_null = matches!(right[*index], Scalar::Null);
        let nulls_first = spec.nulls_first.unwrap_or(!spec.ascending);
        let ordering = match (left_null, right_null) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let ordering = cmp(&left[*index], &right[*index]).unwrap_or(Ordering::Equal);
                if spec.ascending {
                    ordering
                } else {
                    ordering.reverse()
                }
            }
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    let scalar_rank = |value: &Scalar| match value {
        Scalar::Null => 0,
        Scalar::Int(_) => 1,
        Scalar::Decimal(_) => 2,
        Scalar::Float(_) => 3,
        Scalar::Bool(_) => 4,
        Scalar::Str(_) => 5,
    };
    for (left, right) in left.iter().zip(right) {
        let ordering = scalar_rank(left)
            .cmp(&scalar_rank(right))
            .then_with(|| cmp(left, right).unwrap_or(Ordering::Equal));
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

pub(crate) fn query_output_order(query: &Query) -> Vec<(usize, &OrderSpec)> {
    query
        .order_by
        .iter()
        .map(|spec| {
            let index = query
                .select
                .iter()
                .position(|item| {
                    item.alias.as_ref() == Some(&spec.key)
                        || matches!(&item.expr, Expr::Column(column) if column == &spec.key || column.rsplit('.').next() == Some(spec.key.as_str()))
                })
                .unwrap_or(0);
            (index, spec)
        })
        .collect()
}

pub(crate) fn finalize_rows(query: &Query, rows: &mut Vec<Vec<Scalar>>) {
    if query.distinct {
        if !account_query_memory_or_stop(
            rows.iter().map(crate::execution::row_bytes).sum(),
            "distinct set",
        ) {
            rows.clear();
            return;
        }
        let mut seen = std::collections::HashSet::new();
        rows.retain(|row| seen.insert(row.iter().map(scalar_group_key_ref).collect::<Vec<_>>()));
    }
    if !query.order_by.is_empty() {
        let order = query_output_order(query);
        let top_k = query
            .optimizer_enabled
            .then_some(query.limit)
            .flatten()
            .map(|limit| limit.saturating_add(query.offset))
            .unwrap_or(rows.len())
            .min(rows.len());
        if top_k == 0 {
            rows.clear();
        } else if top_k < rows.len() {
            rows.select_nth_unstable_by(top_k, |left, right| {
                compare_output_rows(left, right, &order)
            });
            rows.truncate(top_k);
        }
        rows.sort_by(|left, right| compare_output_rows(left, right, &order));
    }
    if query.offset > 0 {
        rows.drain(..query.offset.min(rows.len()));
    }
    if let Some(limit) = query.limit {
        rows.truncate(limit);
    }
}

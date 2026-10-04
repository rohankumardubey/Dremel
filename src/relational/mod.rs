use crate::execution::aggregate::*;
use crate::execution::scalar::*;
use crate::optimizer::{PushedFilter, filter_always_false, pushed_filters, residual_filter};
use crate::sql::*;
use crate::storage::*;
use crate::types::*;
use std::cmp::Ordering;

pub(crate) fn base_relation_rows(table: &str, catalog: &Catalog) -> Vec<RelRow> {
    base_relation_rows_filtered(table, catalog, &std::collections::HashMap::new(), &[])
}

pub(crate) fn base_relation_rows_filtered(
    table: &str,
    catalog: &Catalog,
    bindings: &std::collections::HashMap<String, String>,
    filters: &[PushedFilter],
) -> Vec<RelRow> {
    let (rows, relation) = match table {
        "events" => (catalog.events.len(), 0),
        "users" => (catalog.users.row_count(), 1),
        "campaigns" => (catalog.campaigns.row_count(), 2),
        _ => return Vec::new(),
    };
    let table_filters: Vec<_> = filters
        .iter()
        .filter(|filter| filter.table == table)
        .collect();
    if table_filters.is_empty()
        && !account_query_memory_or_stop(
            rows.saturating_mul(std::mem::size_of::<RelRow>()),
            "relation materialization",
        )
    {
        return Vec::new();
    }
    let mut output = if table_filters.is_empty() {
        Vec::with_capacity(rows)
    } else {
        Vec::new()
    };
    for index in 0..rows {
        if index % 4096 == 0 && execution_cancelled() {
            break;
        }
        let mut row = RelRow::default();
        match relation {
            0 => row.event = Some(index),
            1 => row.user = Some(index),
            _ => row.campaign = Some(index),
        }
        if !table_filters
            .iter()
            .all(|filter| eval_rel(&filter.expression, catalog, row, bindings).truthy())
        {
            continue;
        }
        if !table_filters.is_empty() && output.len() == output.capacity() {
            let additional = 4096.min(rows.saturating_sub(index));
            if !account_query_memory_or_stop(
                additional.saturating_mul(std::mem::size_of::<RelRow>()),
                "pushed scan output",
            ) {
                return Vec::new();
            }
            output.reserve_exact(additional);
        }
        output.push(row);
    }
    output
}

pub(crate) fn merge_rel_rows(mut left: RelRow, right: RelRow) -> RelRow {
    left.event = left.event.or(right.event);
    left.user = left.user.or(right.user);
    left.campaign = left.campaign.or(right.campaign);
    left
}

pub(crate) fn scalar_hash_key(value: Scalar) -> Option<ScalarKey> {
    match value {
        Scalar::Null => None,
        Scalar::Int(value) => Some(ScalarKey::Int(value)),
        Scalar::Decimal(value) => Some(ScalarKey::Decimal(value)),
        Scalar::Float(value) => Some(ScalarKey::Float(value.to_bits())),
        Scalar::Bool(value) => Some(ScalarKey::Bool(value)),
        Scalar::Str(value) => Some(ScalarKey::Str(value)),
    }
}

pub(crate) fn scalar_group_key(value: Scalar) -> ScalarKey {
    scalar_hash_key(value).unwrap_or(ScalarKey::Null)
}

pub(crate) fn scalar_group_key_ref(value: &Scalar) -> ScalarKey {
    match value {
        Scalar::Null => ScalarKey::Null,
        Scalar::Int(value) => ScalarKey::Int(*value),
        Scalar::Decimal(value) => ScalarKey::Decimal(*value),
        Scalar::Float(value) => ScalarKey::Float(value.to_bits()),
        Scalar::Bool(value) => ScalarKey::Bool(*value),
        Scalar::Str(value) => ScalarKey::Str(value.clone()),
    }
}

pub(crate) fn relation_group_key(
    catalog: &Catalog,
    row: RelRow,
    table: &str,
    column: &str,
) -> ScalarKey {
    if table == "events"
        && let Some(index) = row.event
    {
        return match column {
            "country" => ScalarKey::Dict(catalog.events.country[index]),
            "device" => ScalarKey::Dict(catalog.events.device[index]),
            "event_type" => ScalarKey::Dict(catalog.events.event_type[index]),
            "success" => ScalarKey::Bool(catalog.events.success[index] != 0),
            "campaign_id" if catalog.events.campaign_def[index] == 0 => ScalarKey::Null,
            "campaign_id" => ScalarKey::Int(catalog.events.campaign[index]),
            _ => scalar_group_key(relation_scalar(catalog, row, table, column)),
        };
    }
    scalar_group_key(relation_scalar(catalog, row, table, column))
}

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

pub(crate) fn collect_aggregates(expression: &Expr, output: &mut Vec<Expr>) {
    if is_agg(expression) {
        let key = format!("{expression:?}");
        if !output.iter().any(|existing| format!("{existing:?}") == key) {
            output.push(expression.clone());
        }
        return;
    }
    match expression {
        Expr::Unary(_, value) | Expr::Func(_, value) | Expr::IsNull(value, _) => {
            collect_aggregates(value, output)
        }
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            collect_aggregates(left, output);
            collect_aggregates(right, output);
        }
        Expr::Call(_, values) => values
            .iter()
            .for_each(|value| collect_aggregates(value, output)),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                collect_aggregates(condition, output);
                collect_aggregates(value, output);
            }
            collect_aggregates(fallback, output);
        }
        Expr::Cast(value, _) => collect_aggregates(value, output),
        Expr::InList(value, values, _) => {
            collect_aggregates(value, output);
            values
                .iter()
                .for_each(|item| collect_aggregates(item, output));
        }
        Expr::Between(value, low, high, _) => {
            collect_aggregates(value, output);
            collect_aggregates(low, output);
            collect_aggregates(high, output);
        }
        _ => {}
    }
}

pub(crate) fn aggregate_state(expression: &Expr) -> AggState {
    match expression {
        Expr::Func(name, _) if name == "count" => AggState::Count(0),
        Expr::Func(name, _) if name == "sum" => AggState::Sum(SumState::new()),
        Expr::Func(name, _) if name == "avg" => AggState::Avg { sum: 0.0, count: 0 },
        Expr::Func(name, _) if name == "min" => AggState::Min(None),
        Expr::Func(_, _) => AggState::Max(None),
        _ => unreachable!(),
    }
}

pub(crate) fn update_rel_aggregate(
    state: &mut AggState,
    expression: &Expr,
    catalog: &Catalog,
    row: RelRow,
    bindings: &std::collections::HashMap<String, String>,
) {
    let Expr::Func(_, argument) = expression else {
        unreachable!()
    };
    let value = eval_rel(argument, catalog, row, bindings);
    match state {
        AggState::Count(count) => {
            if matches!(argument.as_ref(), Expr::Star) || !matches!(value, Scalar::Null) {
                *count += 1;
            }
        }
        AggState::Sum(sum) => sum.add(&value),
        AggState::Avg { sum, count } => {
            if let Some(number) = value.number() {
                *sum += number;
                *count += 1;
            }
        }
        AggState::Min(current) => {
            if !matches!(value, Scalar::Null)
                && current
                    .as_ref()
                    .is_none_or(|old| cmp(&value, old) == Some(Ordering::Less))
            {
                *current = Some(value);
            }
        }
        AggState::Max(current) => {
            if !matches!(value, Scalar::Null)
                && current
                    .as_ref()
                    .is_none_or(|old| cmp(&value, old) == Some(Ordering::Greater))
            {
                *current = Some(value);
            }
        }
    }
}

pub(crate) fn eval_group_expr(
    expression: &Expr,
    catalog: &Catalog,
    row: RelRow,
    bindings: &std::collections::HashMap<String, String>,
    aggregates: &[Expr],
    values: &[Scalar],
) -> Scalar {
    if is_agg(expression) {
        let key = format!("{expression:?}");
        return aggregates
            .iter()
            .position(|candidate| format!("{candidate:?}") == key)
            .map_or(Scalar::Null, |index| values[index].clone());
    }
    if !contains_agg(expression) {
        return eval_rel(expression, catalog, row, bindings);
    }
    let evaluate =
        |value: &Expr| eval_group_expr(value, catalog, row, bindings, aggregates, values);
    match expression {
        Expr::Unary(operator, value) => {
            let value = evaluate(value);
            if operator == "not" {
                value
                    .sql_bool()
                    .map_or(Scalar::Null, |value| Scalar::Bool(!value))
            } else {
                match value {
                    Scalar::Int(value) => value.checked_neg().map_or(Scalar::Null, Scalar::Int),
                    Scalar::Decimal(value) => {
                        value.checked_neg().map_or(Scalar::Null, Scalar::Decimal)
                    }
                    Scalar::Float(value) => Scalar::Float(-value),
                    _ => Scalar::Null,
                }
            }
        }
        Expr::Binary(operator, left, right) => {
            apply_binary(operator, evaluate(left), evaluate(right))
        }
        Expr::Func(name, argument) => eval_values(name, vec![evaluate(argument)]),
        Expr::Call(name, arguments) => eval_values(name, arguments.iter().map(evaluate).collect()),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                if evaluate(condition).sql_bool() == Some(true) {
                    return evaluate(value);
                }
            }
            evaluate(fallback)
        }
        Expr::Cast(value, data_type) => cast_value(data_type, evaluate(value)),
        Expr::IsNull(value, negated) => {
            Scalar::Bool(matches!(evaluate(value), Scalar::Null) ^ *negated)
        }
        Expr::InList(value, candidates, negated) => {
            let value = evaluate(value);
            if matches!(value, Scalar::Null) {
                return Scalar::Null;
            }
            let mut saw_null = false;
            for candidate in candidates {
                let candidate = evaluate(candidate);
                if matches!(candidate, Scalar::Null) {
                    saw_null = true;
                } else if cmp(&value, &candidate) == Some(Ordering::Equal) {
                    return Scalar::Bool(!negated);
                }
            }
            if saw_null {
                Scalar::Null
            } else {
                Scalar::Bool(*negated)
            }
        }
        Expr::Between(value, low, high, negated) => {
            let value = evaluate(value);
            let low = evaluate(low);
            let high = evaluate(high);
            if matches!(value, Scalar::Null)
                || matches!(low, Scalar::Null)
                || matches!(high, Scalar::Null)
            {
                Scalar::Null
            } else {
                let inside = matches!(cmp(&value, &low), Some(Ordering::Equal | Ordering::Greater))
                    && matches!(cmp(&value, &high), Some(Ordering::Equal | Ordering::Less));
                Scalar::Bool(inside ^ *negated)
            }
        }
        Expr::Like(value, pattern, negated) => match (evaluate(value), evaluate(pattern)) {
            (Scalar::Str(value), Scalar::Str(pattern)) => {
                Scalar::Bool(like_matches(&value, &pattern) ^ *negated)
            }
            _ => Scalar::Null,
        },
        _ => eval_rel(expression, catalog, row, bindings),
    }
}

pub(crate) struct RelGroup {
    pub(crate) row: RelRow,
    pub(crate) states: Vec<AggState>,
}

pub(crate) fn collect_windows(expression: &Expr, output: &mut Vec<Expr>) {
    if matches!(expression, Expr::Window { .. }) {
        let key = format!("{expression:?}");
        if !output
            .iter()
            .any(|candidate| format!("{candidate:?}") == key)
        {
            output.push(expression.clone());
        }
        return;
    }
    match expression {
        Expr::Unary(_, value) | Expr::Func(_, value) | Expr::IsNull(value, _) => {
            collect_windows(value, output)
        }
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            collect_windows(left, output);
            collect_windows(right, output);
        }
        Expr::Call(_, args) => args.iter().for_each(|arg| collect_windows(arg, output)),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                collect_windows(condition, output);
                collect_windows(value, output);
            }
            collect_windows(fallback, output);
        }
        Expr::Cast(value, _) => collect_windows(value, output),
        Expr::InList(value, values, _) => {
            collect_windows(value, output);
            values.iter().for_each(|item| collect_windows(item, output));
        }
        Expr::Between(value, low, high, _) => {
            collect_windows(value, output);
            collect_windows(low, output);
            collect_windows(high, output);
        }
        _ => {}
    }
}

pub(crate) fn compare_rel_order(
    left: RelRow,
    right: RelRow,
    order: &[(String, String, bool, Option<bool>)],
    catalog: &Catalog,
) -> Ordering {
    for (table, column, ascending, requested_nulls_first) in order {
        let left = relation_scalar(catalog, left, table, column);
        let right = relation_scalar(catalog, right, table, column);
        let left_null = matches!(left, Scalar::Null);
        let right_null = matches!(right, Scalar::Null);
        let nulls_first = requested_nulls_first.unwrap_or(!ascending);
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
                let ordering = cmp(&left, &right).unwrap_or(Ordering::Equal);
                if *ascending {
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
    Ordering::Equal
}

pub(crate) fn compute_window(
    expression: &Expr,
    relation: &[RelRow],
    catalog: &Catalog,
    bindings: &std::collections::HashMap<String, String>,
) -> Vec<Scalar> {
    if !account_query_memory_or_stop(
        relation
            .len()
            .saturating_mul(std::mem::size_of::<Scalar>() + std::mem::size_of::<usize>() + 64),
        "window partitions and values",
    ) {
        return Vec::new();
    }
    let Expr::Window {
        name,
        args,
        partition_by,
        order_by,
    } = expression
    else {
        unreachable!()
    };
    let partition_columns: Vec<_> = partition_by
        .iter()
        .map(|column| resolve_column(column, bindings).expect("bound window partition column"))
        .collect();
    let resolved_order: Vec<_> = order_by
        .iter()
        .map(|spec| {
            let (table, column) =
                resolve_column(&spec.key, bindings).expect("bound window order column");
            (table, column, spec.ascending, spec.nulls_first)
        })
        .collect();
    let mut partitions = std::collections::HashMap::<Vec<ScalarKey>, Vec<usize>>::new();
    for (index, &row) in relation.iter().enumerate() {
        let key = partition_columns
            .iter()
            .map(|(table, column)| relation_group_key(catalog, row, table, column))
            .collect();
        partitions.entry(key).or_default().push(index);
    }
    let mut result = vec![Scalar::Null; relation.len()];
    for mut indices in partitions.into_values() {
        indices.sort_by(|&left, &right| {
            compare_rel_order(relation[left], relation[right], &resolved_order, catalog)
                .then_with(|| left.cmp(&right))
        });
        match name.as_str() {
            "row_number" => {
                for (position, &index) in indices.iter().enumerate() {
                    result[index] = Scalar::Int((position + 1) as i64);
                }
            }
            "rank" | "dense_rank" => {
                let mut rank = 1usize;
                let mut dense = 1usize;
                for position in 0..indices.len() {
                    if position > 0
                        && compare_rel_order(
                            relation[indices[position - 1]],
                            relation[indices[position]],
                            &resolved_order,
                            catalog,
                        ) != Ordering::Equal
                    {
                        rank = position + 1;
                        dense += 1;
                    }
                    result[indices[position]] =
                        Scalar::Int(if name == "rank" { rank } else { dense } as i64);
                }
            }
            "lag" | "lead" => {
                for (position, &index) in indices.iter().enumerate() {
                    let offset = args
                        .get(1)
                        .map(|arg| eval_rel(arg, catalog, relation[index], bindings))
                        .and_then(|value| match value {
                            Scalar::Int(value) if value >= 0 => Some(value as usize),
                            _ => None,
                        })
                        .unwrap_or(1);
                    let target = if name == "lag" {
                        position.checked_sub(offset)
                    } else {
                        position
                            .checked_add(offset)
                            .filter(|target| *target < indices.len())
                    };
                    result[index] = target.map_or_else(
                        || {
                            args.get(2).map_or(Scalar::Null, |fallback| {
                                eval_rel(fallback, catalog, relation[index], bindings)
                            })
                        },
                        |target| eval_rel(&args[0], catalog, relation[indices[target]], bindings),
                    );
                }
            }
            "count" | "sum" | "avg" | "min" | "max" => {
                let aggregate = Expr::Func(name.clone(), Box::new(args[0].clone()));
                if order_by.is_empty() {
                    let mut state = aggregate_state(&aggregate);
                    for &index in &indices {
                        update_rel_aggregate(
                            &mut state,
                            &aggregate,
                            catalog,
                            relation[index],
                            bindings,
                        );
                    }
                    let value = finish(&state);
                    for &index in &indices {
                        result[index] = value.clone();
                    }
                } else {
                    let mut state = aggregate_state(&aggregate);
                    for &index in &indices {
                        update_rel_aggregate(
                            &mut state,
                            &aggregate,
                            catalog,
                            relation[index],
                            bindings,
                        );
                        result[index] = finish(&state);
                    }
                }
            }
            _ => {}
        }
    }
    result
}

pub(crate) fn eval_window_expr(
    expression: &Expr,
    row_index: usize,
    relation: &[RelRow],
    catalog: &Catalog,
    bindings: &std::collections::HashMap<String, String>,
    windows: &[Expr],
    values: &[Vec<Scalar>],
) -> Scalar {
    if matches!(expression, Expr::Window { .. }) {
        let key = format!("{expression:?}");
        return windows
            .iter()
            .position(|candidate| format!("{candidate:?}") == key)
            .map_or(Scalar::Null, |index| values[index][row_index].clone());
    }
    if !contains_window(expression) {
        return eval_rel(expression, catalog, relation[row_index], bindings);
    }
    let evaluate = |value: &Expr| {
        eval_window_expr(
            value, row_index, relation, catalog, bindings, windows, values,
        )
    };
    match expression {
        Expr::Unary(operator, value) => {
            let value = evaluate(value);
            if operator == "not" {
                value
                    .sql_bool()
                    .map_or(Scalar::Null, |value| Scalar::Bool(!value))
            } else {
                match value {
                    Scalar::Int(value) => value.checked_neg().map_or(Scalar::Null, Scalar::Int),
                    Scalar::Decimal(value) => {
                        value.checked_neg().map_or(Scalar::Null, Scalar::Decimal)
                    }
                    Scalar::Float(value) => Scalar::Float(-value),
                    _ => Scalar::Null,
                }
            }
        }
        Expr::Binary(operator, left, right) => {
            apply_binary(operator, evaluate(left), evaluate(right))
        }
        Expr::Func(name, argument) => eval_values(name, vec![evaluate(argument)]),
        Expr::Call(name, args) => eval_values(name, args.iter().map(evaluate).collect()),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                if evaluate(condition).sql_bool() == Some(true) {
                    return evaluate(value);
                }
            }
            evaluate(fallback)
        }
        Expr::Cast(value, data_type) => cast_value(data_type, evaluate(value)),
        Expr::IsNull(value, negated) => {
            Scalar::Bool(matches!(evaluate(value), Scalar::Null) ^ *negated)
        }
        _ => Scalar::Null,
    }
}

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

pub(crate) struct MaterializedRelation {
    pub(crate) name: String,
    pub(crate) columns: Vec<String>,
    pub(crate) rows: Vec<Vec<Scalar>>,
}

pub(crate) fn output_columns(query: &Query) -> Vec<String> {
    query
        .select
        .iter()
        .enumerate()
        .map(|(index, item)| {
            item.alias.clone().unwrap_or_else(|| match &item.expr {
                Expr::Column(column) => column.rsplit('.').next().unwrap_or(column).into(),
                _ => format!("column{}", index + 1),
            })
        })
        .collect()
}

pub(crate) fn eval_materialized_values(
    expression: &Expr,
    columns: &[String],
    row: &[Scalar],
) -> Scalar {
    let evaluate = |value: &Expr| eval_materialized_values(value, columns, row);
    match expression {
        Expr::Null => Scalar::Null,
        Expr::Column(column) => {
            let exact = columns.iter().position(|candidate| candidate == column);
            let name = column.rsplit('.').next().unwrap_or(column);
            let mut unqualified = columns
                .iter()
                .enumerate()
                .filter(|(_, candidate)| candidate.rsplit('.').next() == Some(name));
            let fallback = unqualified
                .next()
                .and_then(|(index, _)| unqualified.next().is_none().then_some(index));
            exact
                .or(fallback)
                .map_or(Scalar::Null, |index| row[index].clone())
        }
        Expr::Int(value) => Scalar::Int(*value),
        Expr::Float(value) => Scalar::Float(*value),
        Expr::Bool(value) => Scalar::Bool(*value),
        Expr::String(value) => Scalar::Str(value.clone()),
        Expr::Star => Scalar::Int(1),
        Expr::Unary(operator, value) => {
            let value = evaluate(value);
            if operator == "not" {
                value
                    .sql_bool()
                    .map_or(Scalar::Null, |value| Scalar::Bool(!value))
            } else {
                match value {
                    Scalar::Int(value) => value.checked_neg().map_or(Scalar::Null, Scalar::Int),
                    Scalar::Decimal(value) => {
                        value.checked_neg().map_or(Scalar::Null, Scalar::Decimal)
                    }
                    Scalar::Float(value) => Scalar::Float(-value),
                    _ => Scalar::Null,
                }
            }
        }
        Expr::Binary(operator, left, right) => {
            apply_binary(operator, evaluate(left), evaluate(right))
        }
        Expr::Func(name, argument) => eval_values(name, vec![evaluate(argument)]),
        Expr::Call(name, args) => eval_values(name, args.iter().map(evaluate).collect()),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                if evaluate(condition).sql_bool() == Some(true) {
                    return evaluate(value);
                }
            }
            evaluate(fallback)
        }
        Expr::Cast(value, data_type) => cast_value(data_type, evaluate(value)),
        Expr::IsNull(value, negated) => {
            Scalar::Bool(matches!(evaluate(value), Scalar::Null) ^ *negated)
        }
        Expr::InList(value, candidates, negated) => {
            let value = evaluate(value);
            if matches!(value, Scalar::Null) {
                return Scalar::Null;
            }
            let mut saw_null = false;
            for candidate in candidates {
                let candidate = evaluate(candidate);
                if matches!(candidate, Scalar::Null) {
                    saw_null = true;
                } else if cmp(&value, &candidate) == Some(Ordering::Equal) {
                    return Scalar::Bool(!negated);
                }
            }
            if saw_null {
                Scalar::Null
            } else {
                Scalar::Bool(*negated)
            }
        }
        Expr::Between(value, low, high, negated) => {
            let value = evaluate(value);
            let low = evaluate(low);
            let high = evaluate(high);
            if matches!(value, Scalar::Null)
                || matches!(low, Scalar::Null)
                || matches!(high, Scalar::Null)
            {
                Scalar::Null
            } else {
                let inside = matches!(cmp(&value, &low), Some(Ordering::Equal | Ordering::Greater))
                    && matches!(cmp(&value, &high), Some(Ordering::Equal | Ordering::Less));
                Scalar::Bool(inside ^ *negated)
            }
        }
        Expr::Like(value, pattern, negated) => match (evaluate(value), evaluate(pattern)) {
            (Scalar::Str(value), Scalar::Str(pattern)) => {
                Scalar::Bool(like_matches(&value, &pattern) ^ *negated)
            }
            _ => Scalar::Null,
        },
        Expr::DictEq(_, _, _)
        | Expr::Window { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::InSubquery(_, _, _) => Scalar::Null,
    }
}

pub(crate) fn eval_materialized(
    expression: &Expr,
    relation: &MaterializedRelation,
    row: usize,
) -> Scalar {
    eval_materialized_values(expression, &relation.columns, &relation.rows[row])
}

pub(crate) fn update_materialized_aggregate(
    state: &mut AggState,
    expression: &Expr,
    relation: &MaterializedRelation,
    row: usize,
) {
    let Expr::Func(_, argument) = expression else {
        unreachable!()
    };
    let value = eval_materialized(argument, relation, row);
    match state {
        AggState::Count(count) => {
            if matches!(argument.as_ref(), Expr::Star) || !matches!(value, Scalar::Null) {
                *count += 1;
            }
        }
        AggState::Sum(sum) => sum.add(&value),
        AggState::Avg { sum, count } => {
            if let Some(number) = value.number() {
                *sum += number;
                *count += 1;
            }
        }
        AggState::Min(current) => {
            if !matches!(value, Scalar::Null)
                && current
                    .as_ref()
                    .is_none_or(|old| cmp(&value, old) == Some(Ordering::Less))
            {
                *current = Some(value);
            }
        }
        AggState::Max(current) => {
            if !matches!(value, Scalar::Null)
                && current
                    .as_ref()
                    .is_none_or(|old| cmp(&value, old) == Some(Ordering::Greater))
            {
                *current = Some(value);
            }
        }
    }
}

pub(crate) fn eval_materialized_group(
    expression: &Expr,
    relation: &MaterializedRelation,
    row: usize,
    aggregates: &[Expr],
    values: &[Scalar],
) -> Scalar {
    if is_agg(expression) {
        let key = format!("{expression:?}");
        return aggregates
            .iter()
            .position(|candidate| format!("{candidate:?}") == key)
            .map_or(Scalar::Null, |index| values[index].clone());
    }
    if !contains_agg(expression) {
        return eval_materialized(expression, relation, row);
    }
    let evaluate = |value: &Expr| eval_materialized_group(value, relation, row, aggregates, values);
    match expression {
        Expr::Binary(operator, left, right) => {
            apply_binary(operator, evaluate(left), evaluate(right))
        }
        Expr::Unary(operator, value) if operator == "not" => evaluate(value)
            .sql_bool()
            .map_or(Scalar::Null, |value| Scalar::Bool(!value)),
        Expr::Call(name, args) => eval_values(name, args.iter().map(evaluate).collect()),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                if evaluate(condition).sql_bool() == Some(true) {
                    return evaluate(value);
                }
            }
            evaluate(fallback)
        }
        Expr::Cast(value, data_type) => cast_value(data_type, evaluate(value)),
        Expr::IsNull(value, negated) => {
            Scalar::Bool(matches!(evaluate(value), Scalar::Null) ^ *negated)
        }
        _ => Scalar::Null,
    }
}

pub(crate) fn execute_materialized(
    query: &Query,
    relation: &MaterializedRelation,
) -> Result<Vec<Vec<Scalar>>, String> {
    if query.from.name != relation.name {
        return Err(format!("unknown CTE {}", query.from.name));
    }
    if !account_query_memory_or_stop(
        relation
            .rows
            .len()
            .saturating_mul(std::mem::size_of::<usize>()),
        "filter selection",
    ) {
        return Ok(Vec::new());
    }
    let mut selected: Vec<_> = (0..relation.rows.len()).collect();
    if let Some(filter) = &query.filter {
        selected.retain(|&row| eval_materialized(filter, relation, row).truthy());
    }
    let aggregate = query.select.iter().any(|item| contains_agg(&item.expr))
        || query.having.as_ref().is_some_and(contains_agg)
        || !query.group_by.is_empty();
    let mut rows = Vec::new();
    if aggregate {
        let mut aggregate_expressions = Vec::new();
        for item in &query.select {
            collect_aggregates(&item.expr, &mut aggregate_expressions);
        }
        if let Some(having) = &query.having {
            collect_aggregates(having, &mut aggregate_expressions);
        }
        let templates: Vec<_> = aggregate_expressions.iter().map(aggregate_state).collect();
        let mut groups = std::collections::HashMap::<Vec<ScalarKey>, (usize, Vec<AggState>)>::new();
        if query.group_by.is_empty() {
            groups.insert(Vec::new(), (0, templates.clone()));
        }
        for row in selected {
            let key: Vec<ScalarKey> = query
                .group_by
                .iter()
                .map(|column| {
                    scalar_group_key(eval_materialized(
                        &Expr::Column(column.clone()),
                        relation,
                        row,
                    ))
                })
                .collect();
            if !groups.contains_key(&key)
                && !account_query_memory_or_stop(
                    key.len().saturating_mul(std::mem::size_of::<ScalarKey>())
                        + templates
                            .len()
                            .saturating_mul(std::mem::size_of::<AggState>())
                        + 64,
                    "materialized hash aggregation",
                )
            {
                return Ok(Vec::new());
            }
            let group = groups
                .entry(key)
                .or_insert_with(|| (row, templates.clone()));
            group.0 = row;
            for (state, expression) in group.1.iter_mut().zip(&aggregate_expressions) {
                update_materialized_aggregate(state, expression, relation, row);
            }
        }
        for (_, (row, states)) in groups {
            let values: Vec<_> = states.iter().map(finish).collect();
            if query.having.as_ref().is_some_and(|having| {
                !eval_materialized_group(having, relation, row, &aggregate_expressions, &values)
                    .truthy()
            }) {
                continue;
            }
            let output: Vec<_> = query
                .select
                .iter()
                .map(|item| {
                    eval_materialized_group(
                        &item.expr,
                        relation,
                        row,
                        &aggregate_expressions,
                        &values,
                    )
                })
                .collect();
            if !account_query_memory_or_stop(
                crate::execution::row_bytes(&output),
                "result materialization",
            ) {
                return Ok(Vec::new());
            }
            rows.push(output);
        }
    } else {
        for row in selected {
            let output: Vec<_> = query
                .select
                .iter()
                .map(|item| eval_materialized(&item.expr, relation, row))
                .collect();
            if !account_query_memory_or_stop(
                crate::execution::row_bytes(&output),
                "result materialization",
            ) {
                return Ok(Vec::new());
            }
            rows.push(output);
        }
    }
    finalize_rows(query, &mut rows);
    Ok(rows)
}

pub(crate) fn execute_materialized_joins(
    query: &Query,
    catalog: &Catalog,
) -> Result<Option<Vec<Vec<Scalar>>>, String> {
    let find_cte = |table: &TableRef| query.ctes.iter().find(|(name, _)| name == &table.name);
    if find_cte(&query.from).is_none()
        && query
            .joins
            .iter()
            .all(|join| find_cte(&join.table).is_none())
    {
        return Ok(None);
    }
    let materialize = |table: &TableRef| -> Result<(Vec<String>, Vec<Vec<Scalar>>), String> {
        if let Some((_, cte_query)) = find_cte(table) {
            return Ok((output_columns(cte_query), execute_rel(cte_query, catalog)?));
        }
        let columns: Vec<_> = table_columns(&table.name)
            .iter()
            .map(|column| (*column).to_string())
            .collect();
        if columns.is_empty() {
            return Err(format!("unknown table {}", table.name));
        }
        let mut rows = Vec::new();
        for row in base_relation_rows(&table.name, catalog) {
            let output: Vec<_> = columns
                .iter()
                .map(|column| relation_scalar(catalog, row, &table.name, column))
                .collect();
            if !account_query_memory_or_stop(
                crate::execution::row_bytes(&output),
                "materialized relation",
            ) {
                return Ok((columns, Vec::new()));
            }
            rows.push(output);
        }
        Ok((columns, rows))
    };
    let (from_columns, mut rows) = materialize(&query.from)?;
    let mut columns: Vec<_> = from_columns
        .iter()
        .map(|column| format!("{}.{}", query.from.alias, column))
        .collect();
    for join in &query.joins {
        let (raw_right_columns, right_rows) = materialize(&join.table)?;
        let right_columns: Vec<_> = raw_right_columns
            .into_iter()
            .map(|column| format!("{}.{}", join.table.alias, column))
            .collect();
        let mut combined_columns = columns.clone();
        combined_columns.extend(right_columns.iter().cloned());
        let left_width = columns.len();
        let right_width = right_columns.len();
        let mut joined = Vec::new();
        let mut matched_right = vec![false; right_rows.len()];
        for left in rows {
            let mut matched = false;
            for (right_index, right) in right_rows.iter().enumerate() {
                let mut combined = left.clone();
                combined.extend(right.iter().cloned());
                let matches = join.kind == JoinKind::Cross
                    || join.on.as_ref().is_some_and(|on| {
                        eval_materialized_values(on, &combined_columns, &combined).truthy()
                    });
                if matches {
                    if !account_query_memory_or_stop(
                        crate::execution::row_bytes(&combined),
                        "materialized join output",
                    ) {
                        return Ok(Some(Vec::new()));
                    }
                    joined.push(combined);
                    matched = true;
                    matched_right[right_index] = true;
                }
            }
            if !matched && matches!(join.kind, JoinKind::Left | JoinKind::Full) {
                let mut combined = left;
                combined.extend(std::iter::repeat_n(Scalar::Null, right_width));
                if !account_query_memory_or_stop(
                    crate::execution::row_bytes(&combined),
                    "materialized join output",
                ) {
                    return Ok(Some(Vec::new()));
                }
                joined.push(combined);
            }
        }
        if matches!(join.kind, JoinKind::Right | JoinKind::Full) {
            for (right, matched) in right_rows.iter().zip(matched_right) {
                if !matched {
                    let mut combined = vec![Scalar::Null; left_width];
                    combined.extend(right.iter().cloned());
                    if !account_query_memory_or_stop(
                        crate::execution::row_bytes(&combined),
                        "materialized join output",
                    ) {
                        return Ok(Some(Vec::new()));
                    }
                    joined.push(combined);
                }
            }
        }
        rows = joined;
        columns = combined_columns;
    }
    let relation = MaterializedRelation {
        name: query.from.name.clone(),
        columns,
        rows,
    };
    Ok(Some(execute_materialized(query, &relation)?))
}

pub(crate) fn execute_rel(query: &Query, catalog: &Catalog) -> Result<Vec<Vec<Scalar>>, String> {
    execute_rel_inner(query, catalog, false)
}

pub(crate) fn execute_rel_inner(
    query: &Query,
    catalog: &Catalog,
    skip_union: bool,
) -> Result<Vec<Vec<Scalar>>, String> {
    if !query.joins.is_empty()
        && !query.ctes.is_empty()
        && let Some(rows) = execute_materialized_joins(query, catalog)?
    {
        return Ok(rows);
    }
    if !query.ctes.is_empty()
        && let Some((name, cte_query)) =
            query.ctes.iter().find(|(name, _)| name == &query.from.name)
    {
        let relation = MaterializedRelation {
            name: name.clone(),
            columns: output_columns(cte_query),
            rows: execute_rel(cte_query, catalog)?,
        };
        return execute_materialized(query, &relation);
    }
    if !skip_union && let Some(right) = &query.union {
        let mut rows = execute_rel_inner(query, catalog, true)?;
        let right_rows = execute_rel(right, catalog)?;
        if rows.first().map(Vec::len) != right_rows.first().map(Vec::len)
            && !rows.is_empty()
            && !right_rows.is_empty()
        {
            return Err("UNION inputs must have the same column count".into());
        }
        rows.extend(right_rows);
        if !query.union_all {
            let mut seen = std::collections::HashSet::new();
            rows.retain(|row| {
                seen.insert(row.iter().map(scalar_group_key_ref).collect::<Vec<_>>())
            });
        }
        return Ok(rows);
    }
    let bindings = bind_query(query)?;
    let scan_filters = pushed_filters(query);
    let residual_filter = residual_filter(query);
    let impossible = filter_always_false(query.filter.as_ref());
    let mut relation = if impossible {
        Vec::new()
    } else {
        base_relation_rows_filtered(&query.from.name, catalog, &bindings, &scan_filters)
    };
    if execution_cancelled() {
        return Ok(Vec::new());
    }
    for join in &query.joins {
        if impossible {
            break;
        }
        relation = apply_join(
            relation,
            join,
            catalog,
            &bindings,
            query.optimizer_enabled,
            &scan_filters,
        );
        if execution_cancelled() {
            return Ok(Vec::new());
        }
    }
    if !impossible && let Some(filter) = &residual_filter {
        relation.retain(|row| {
            !execution_cancelled() && eval_rel(filter, catalog, *row, &bindings).truthy()
        });
    }
    let aggregate = query.select.iter().any(|item| contains_agg(&item.expr))
        || query.having.as_ref().is_some_and(contains_agg)
        || !query.group_by.is_empty();
    let mut rows: Vec<Vec<Scalar>> = Vec::new();
    let has_windows = query.select.iter().any(|item| contains_window(&item.expr));
    if aggregate {
        let mut aggregate_expressions = Vec::new();
        for item in &query.select {
            collect_aggregates(&item.expr, &mut aggregate_expressions);
        }
        if let Some(having) = &query.having {
            collect_aggregates(having, &mut aggregate_expressions);
        }
        let templates: Vec<_> = aggregate_expressions.iter().map(aggregate_state).collect();
        let mut groups = std::collections::HashMap::<Vec<ScalarKey>, RelGroup>::new();
        if query.group_by.is_empty() {
            if !account_query_memory_or_stop(
                templates
                    .len()
                    .saturating_mul(std::mem::size_of::<AggState>())
                    + 64,
                "relational hash aggregation",
            ) {
                return Ok(Vec::new());
            }
            groups.insert(
                Vec::new(),
                RelGroup {
                    row: RelRow::default(),
                    states: templates.clone(),
                },
            );
        }
        let group_columns = query
            .group_by
            .iter()
            .map(|column| resolve_column(column, &bindings))
            .collect::<Result<Vec<_>, _>>()?;
        for row in relation {
            if execution_cancelled() {
                break;
            }
            let key: Vec<ScalarKey> = group_columns
                .iter()
                .map(|(table, column)| relation_group_key(catalog, row, table, column))
                .collect();
            if !groups.contains_key(&key)
                && !account_query_memory_or_stop(
                    key.len().saturating_mul(std::mem::size_of::<ScalarKey>())
                        + templates
                            .len()
                            .saturating_mul(std::mem::size_of::<AggState>())
                        + 64,
                    "relational hash aggregation",
                )
            {
                return Ok(Vec::new());
            }
            let group = groups.entry(key).or_insert_with(|| RelGroup {
                row,
                states: templates.clone(),
            });
            group.row = row;
            for (state, expression) in group.states.iter_mut().zip(&aggregate_expressions) {
                update_rel_aggregate(state, expression, catalog, row, &bindings);
            }
        }
        for group in groups.into_values() {
            let aggregate_values: Vec<_> = group.states.iter().map(finish).collect();
            if query.having.as_ref().is_some_and(|having| {
                !eval_group_expr(
                    having,
                    catalog,
                    group.row,
                    &bindings,
                    &aggregate_expressions,
                    &aggregate_values,
                )
                .truthy()
            }) {
                continue;
            }
            let output: Vec<_> = query
                .select
                .iter()
                .map(|item| {
                    eval_group_expr(
                        &item.expr,
                        catalog,
                        group.row,
                        &bindings,
                        &aggregate_expressions,
                        &aggregate_values,
                    )
                })
                .collect();
            if !account_query_memory_or_stop(
                crate::execution::row_bytes(&output),
                "result materialization",
            ) {
                return Ok(Vec::new());
            }
            rows.push(output);
        }
    } else if has_windows {
        let mut window_expressions = Vec::new();
        for item in &query.select {
            collect_windows(&item.expr, &mut window_expressions);
        }
        let window_values: Vec<_> = window_expressions
            .iter()
            .map(|window| compute_window(window, &relation, catalog, &bindings))
            .collect();
        if execution_cancelled() {
            return Ok(Vec::new());
        }
        for row_index in 0..relation.len() {
            let output: Vec<_> = query
                .select
                .iter()
                .map(|item| {
                    eval_window_expr(
                        &item.expr,
                        row_index,
                        &relation,
                        catalog,
                        &bindings,
                        &window_expressions,
                        &window_values,
                    )
                })
                .collect();
            if !account_query_memory_or_stop(
                crate::execution::row_bytes(&output),
                "window result materialization",
            ) {
                return Ok(Vec::new());
            }
            rows.push(output);
        }
    } else {
        let top_k = query
            .optimizer_enabled
            .then_some(query.limit)
            .flatten()
            .filter(|_| !query.order_by.is_empty())
            .map(|limit| limit.saturating_add(query.offset));
        if let Some(top_k) = top_k {
            account_query_memory(
                top_k.saturating_add(1).saturating_mul(
                    std::mem::size_of::<Vec<Scalar>>()
                        + query
                            .select
                            .len()
                            .saturating_mul(std::mem::size_of::<Scalar>() + 64),
                ),
                "top-k buffer",
            )?;
        }
        let order = query_output_order(query);
        let mut worst_top_k = None;
        for row in relation {
            let output: Vec<_> = query
                .select
                .iter()
                .map(|item| eval_rel(&item.expr, catalog, row, &bindings))
                .collect();
            if let Some(top_k) = top_k {
                if top_k == 0 {
                    continue;
                }
                if rows.len() < top_k {
                    rows.push(output);
                    if rows.len() == top_k {
                        worst_top_k = (0..rows.len()).max_by(|&left, &right| {
                            compare_output_rows(&rows[left], &rows[right], &order)
                        });
                    }
                } else {
                    let worst = worst_top_k.expect("non-empty top-k");
                    if compare_output_rows(&output, &rows[worst], &order) == Ordering::Less {
                        rows[worst] = output;
                        worst_top_k = (0..rows.len()).max_by(|&left, &right| {
                            compare_output_rows(&rows[left], &rows[right], &order)
                        });
                    }
                }
            } else {
                if !account_query_memory_or_stop(
                    crate::execution::row_bytes(&output),
                    "result materialization",
                ) {
                    return Ok(Vec::new());
                }
                rows.push(output);
            }
        }
    }
    finalize_rows(query, &mut rows);
    Ok(rows)
}

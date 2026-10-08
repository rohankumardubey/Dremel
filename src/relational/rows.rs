use super::*;

pub(crate) fn base_relation_rows(table: &str, catalog: &Catalog) -> Vec<RelRow> {
    base_relation_rows_filtered(table, catalog, &Bindings::default(), &[])
}

pub(crate) fn base_relation_rows_filtered(
    table: &str,
    catalog: &Catalog,
    bindings: &Bindings,
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
            "country" => ScalarKey::Dict(catalog.events.raw_key("country", index) as u32),
            "device" => ScalarKey::Dict(catalog.events.raw_key("device", index) as u32),
            "event_type" => ScalarKey::Dict(catalog.events.raw_key("event_type", index) as u32),
            "success" | "campaign_id" => scalar_group_key(catalog.events.scalar(column, index)),
            _ => scalar_group_key(relation_scalar(catalog, row, table, column)),
        };
    }
    scalar_group_key(relation_scalar(catalog, row, table, column))
}

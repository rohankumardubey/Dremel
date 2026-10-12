use super::{ColumnAccess, Expansion};
use crate::query::bind::validate;
use crate::query::nested::collect_paths;
use crate::sql::{JoinKind, Query, contains_agg};
use crate::storage::{Bindings, SqlType, infer_type, resolve_column};
use arrow::datatypes::{DataType, Field};

pub(in crate::query) fn bind_expansions(
    query: &mut Query,
    bindings: &mut Bindings,
) -> Result<Vec<Expansion>, String> {
    if query.joins.len() > 32 {
        return Err("at most 32 correlated UNNEST stages are supported".into());
    }
    let mut expansions = Vec::new();
    for join in &mut query.joins {
        let spec = join
            .unnest
            .as_ref()
            .ok_or("named-table joins require a catalog; only correlated UNNEST is supported")?;
        if !matches!(
            join.kind,
            JoinKind::Cross | JoinKind::Inner | JoinKind::Left
        ) {
            return Err("correlated UNNEST supports CROSS, INNER, and LEFT joins only".into());
        }
        let (relation, column) = resolve_column(&spec.path, bindings)?;
        let kind = bindings
            .arrow_type(&relation, &column)
            .ok_or("missing UNNEST source type")?;
        let element = match kind {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => field.data_type().clone(),
            _ => return Err("UNNEST requires a LIST, LARGE_LIST, or FIXED_SIZE_LIST field".into()),
        };
        let ColumnAccess::Value { path, domain } =
            column_access(&spec.path, bindings, &query.from.name, &expansions)?
        else {
            return Err("UNNEST requires a list field, not an ordinality".into());
        };
        // Source and ON can reference only the parent and preceding stages.
        let mut fields = vec![Field::new(&spec.column, element, true)];
        if let Some(ordinality) = &spec.ordinality {
            fields.push(Field::new(ordinality, DataType::Int64, true));
        }
        bindings.add_relation(&join.table.alias, fields.into())?;
        if let Some(on) = &mut join.on {
            if contains_agg(on)
                || !matches!(infer_type(on, bindings)?, SqlType::Bool | SqlType::Null)
            {
                return Err("UNNEST ON requires a non-aggregate BOOLEAN expression".into());
            }
            validate(on, bindings, false, None)?;
            super::scope::qualify(on, bindings)?;
        }
        expansions.push(Expansion {
            alias: join.table.alias.clone(),
            column: spec.column.clone(),
            ordinality: spec.ordinality.clone(),
            path,
            source_domain: domain,
            kind: join.kind.clone(),
            on: join.on.clone(),
        });
    }
    Ok(expansions)
}

pub(in crate::query) fn column_access(
    name: &str,
    bindings: &Bindings,
    base: &str,
    expansions: &[Expansion],
) -> Result<ColumnAccess, String> {
    let (table, path) = resolve_column(name, bindings)?;
    if table == base {
        return Ok(ColumnAccess::Value { path, domain: 0 });
    }
    let (index, stage) = expansions
        .iter()
        .enumerate()
        .find(|(_, stage)| stage.alias == table)
        .ok_or("unknown UNNEST relation")?;
    let domain = index + 1;
    if stage.ordinality.as_ref() == Some(&path) {
        return Ok(ColumnAccess::Ordinality { domain });
    }
    let suffix = path
        .strip_prefix(&stage.column)
        .filter(|suffix| suffix.is_empty() || suffix.starts_with('.'))
        .ok_or("unknown UNNEST element field")?;
    Ok(ColumnAccess::Value {
        path: format!("{}[]{suffix}", stage.path),
        domain,
    })
}

pub(in crate::query) fn collect_join_paths(
    query: &Query,
    paths: &mut std::collections::BTreeSet<String>,
) {
    for join in &query.joins {
        if let Some(on) = &join.on {
            collect_paths(on, paths);
        }
    }
}

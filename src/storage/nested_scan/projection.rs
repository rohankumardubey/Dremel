use arrow::datatypes::{DataType, Field, Schema};
use std::collections::HashSet;

/// Logical leaf paths omit the physical Parquet LIST wrapper names.
pub(super) fn leaf_paths(schema: &Schema) -> Result<Vec<Vec<String>>, String> {
    let mut leaves = Vec::new();
    unique_names(schema.fields().iter().map(|field| field.as_ref()))?;
    for field in schema.fields() {
        visit(field, vec![field.name().clone()], &mut leaves)?;
    }
    Ok(leaves)
}

fn visit(field: &Field, path: Vec<String>, leaves: &mut Vec<Vec<String>>) -> Result<(), String> {
    match field.data_type() {
        DataType::Struct(fields) => {
            unique_names(fields.iter().map(|field| field.as_ref()))?;
            for field in fields {
                let mut child = path.clone();
                child.push(field.name().clone());
                visit(field, child, leaves)?;
            }
        }
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            visit(field, path, leaves)?;
        }
        DataType::Map(field, _) => visit(field, path, leaves)?,
        DataType::Dictionary(_, value) => {
            visit(
                &Field::new(field.name(), value.as_ref().clone(), true),
                path,
                leaves,
            )?;
        }
        DataType::Union(_, _) | DataType::RunEndEncoded(_, _) => {
            return Err(format!(
                "unsupported Parquet projection type {}",
                field.data_type()
            ));
        }
        _ => leaves.push(path),
    }
    Ok(())
}

fn unique_names<'a>(fields: impl Iterator<Item = &'a Field>) -> Result<(), String> {
    let mut names = HashSet::new();
    for field in fields {
        if !names.insert(field.name()) {
            return Err(format!("ambiguous nested field {}", field.name()));
        }
    }
    Ok(())
}

pub(super) fn select(paths: &[Vec<String>], columns: &[String]) -> Result<Vec<usize>, String> {
    if columns.is_empty() {
        return Ok((0..paths.len()).collect());
    }
    let mut selected = vec![false; paths.len()];
    for name in columns {
        let path: Vec<_> = name.split('.').map(str::to_owned).collect();
        if path.iter().any(String::is_empty) {
            return Err(format!("invalid nested field path {name}"));
        }
        let mut found = false;
        for (index, leaf) in paths.iter().enumerate() {
            if leaf.starts_with(&path) {
                selected[index] = true;
                found = true;
            }
        }
        if !found {
            return Err(format!("unknown nested field path {name}"));
        }
    }
    Ok(selected
        .iter()
        .enumerate()
        .filter_map(|(index, &yes)| yes.then_some(index))
        .collect())
}

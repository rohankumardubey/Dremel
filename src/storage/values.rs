use super::{ColumnarTable, SqlType, primitive};
use crate::types::{Scalar, account_query_memory, execution_cancelled};
use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, StructArray, make_array,
};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::DataType;
use std::collections::HashMap;

pub(crate) struct ValueBatch {
    pub(crate) rows: usize,
    columns: HashMap<String, ColumnValue>,
}

struct ColumnValue {
    array: ArrayRef,
    kind: SqlType,
    children: Vec<(String, ColumnValue)>,
}

impl ValueBatch {
    pub(crate) fn from_table(
        table: &ColumnarTable,
        paths: &HashMap<String, String>,
    ) -> Result<Vec<Self>, String> {
        table
            .batches()
            .iter()
            .map(|batch| {
                let mut columns = HashMap::new();
                for (expression, path) in paths {
                    if execution_cancelled() {
                        return Err("query cancelled during column normalization".into());
                    }
                    let mut segments = path.split('.');
                    let root = segments.next().ok_or("empty column path")?;
                    let index = batch
                        .schema()
                        .fields()
                        .iter()
                        .position(|field| field.name().eq_ignore_ascii_case(root))
                        .ok_or_else(|| format!("unknown column {path}"))?;
                    let mut array = batch.column(index).clone();
                    for segment in segments {
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
                    columns.insert(expression.clone(), ColumnValue::new(path, array)?);
                }
                Ok(Self {
                    rows: batch.num_rows(),
                    columns,
                })
            })
            .collect()
    }

    pub(crate) fn scalar(&self, column: &str, row: usize) -> Scalar {
        self.columns[column].scalar(row)
    }
}

fn with_parent_nulls(array: ArrayRef, parent: Option<&NullBuffer>) -> Result<ArrayRef, String> {
    let Some(parent) = parent else {
        return Ok(array);
    };
    let nulls = NullBuffer::union(array.nulls(), Some(parent));
    if let Some(nulls) = &nulls {
        account_query_memory(nulls.buffer().len(), "nested validity")?;
    }
    let data = array
        .to_data()
        .into_builder()
        .nulls(nulls)
        .build()
        .map_err(|error| error.to_string())?;
    Ok(make_array(data))
}

fn list_range(array: &ArrayRef, row: usize) -> std::ops::Range<usize> {
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        let offsets = list.value_offsets();
        offsets[row] as usize..offsets[row + 1] as usize
    } else if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        let offsets = list.value_offsets();
        offsets[row] as usize..offsets[row + 1] as usize
    } else {
        let list = array
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("bound list");
        let start = list.value_offset(row) as usize;
        start..start + list.value_length() as usize
    }
}

impl ColumnValue {
    fn new(path: &str, array: ArrayRef) -> Result<Self, String> {
        match array.data_type() {
            DataType::Struct(_) => {
                let structure = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("struct array");
                let children = structure
                    .fields()
                    .iter()
                    .zip(structure.columns())
                    .map(|(field, child)| {
                        let child = with_parent_nulls(child.clone(), array.nulls())?;
                        Ok((
                            field.name().clone(),
                            Self::new(&format!("{path}.{}", field.name()), child)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                Ok(Self {
                    array,
                    kind: SqlType::Struct,
                    children,
                })
            }
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                let values = if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
                    list.values().clone()
                } else if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
                    list.values().clone()
                } else {
                    array
                        .as_any()
                        .downcast_ref::<FixedSizeListArray>()
                        .expect("list array")
                        .values()
                        .clone()
                };
                // Sliced lists retain values outside their offset range. Mask those
                // and values hidden beneath null lists before validating leaves.
                account_query_memory(values.len(), "list visibility workspace")?;
                let mut visible = vec![false; values.len()];
                for row in 0..array.len() {
                    if array.is_valid(row) {
                        visible[list_range(&array, row)].fill(true);
                    }
                }
                let validity = NullBuffer::new(BooleanBuffer::from(visible));
                let values = with_parent_nulls(values, Some(&validity))?;
                let child = Self::new(path, values)?;
                Ok(Self {
                    array,
                    kind: SqlType::List,
                    children: vec![(String::new(), child)],
                })
            }
            _ => {
                let (kind, array) = primitive::normalize(path, array)?;
                Ok(Self {
                    array,
                    kind,
                    children: Vec::new(),
                })
            }
        }
    }

    fn scalar(&self, row: usize) -> Scalar {
        if self.array.is_null(row) {
            return Scalar::Null;
        }
        match self.kind {
            SqlType::Struct => Scalar::Struct(
                self.children
                    .iter()
                    .map(|(name, child)| (name.clone(), child.scalar(row)))
                    .collect(),
            ),
            SqlType::List => Scalar::List(
                list_range(&self.array, row)
                    .map(|index| self.children[0].1.scalar(index))
                    .collect(),
            ),
            _ => primitive::scalar(&self.array, self.kind, row),
        }
    }
}

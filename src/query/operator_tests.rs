use super::*;
use arrow::array::{Array, ArrayRef, Int64Array, ListArray, StringArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn table() -> ColumnarTable {
    let key = StructArray::new(
        vec![
            Field::new("number", DataType::Int64, true),
            Field::new("label", DataType::Utf8, true),
        ]
        .into(),
        vec![
            Arc::new(Int64Array::from(vec![
                None,
                None,
                Some(2),
                Some(10),
                Some(2),
                Some(2),
            ])),
            Arc::new(StringArray::from(vec![
                None,
                None,
                Some("x"),
                Some("x"),
                Some("x"),
                Some("x"),
            ])),
        ],
        Some(NullBuffer::from(vec![false, true, true, true, true, true])),
    );
    let lists = ListArray::from_iter_primitive::<Int64Type, _, _>([
        None,
        Some(vec![]),
        Some(vec![None]),
        Some(vec![Some(2)]),
        Some(vec![Some(10)]),
        Some(vec![Some(2), None]),
    ]);
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5])),
        Arc::new(key),
        Arc::new(lists),
    ];
    let schema = Arc::new(Schema::new(
        arrays
            .iter()
            .zip(["id", "key", "items"])
            .map(|(value, name)| Field::new(name, value.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    ColumnarTable::try_new(schema, vec![batch.slice(0, 3), batch.slice(3, 3)]).unwrap()
}

fn run(table: &ColumnarTable, sql: &str) -> QueryResult {
    ColumnarQuery::prepare("records", table.schema().clone(), sql)
        .unwrap()
        .execute(table)
        .unwrap()
}

fn ids(table: &ColumnarTable, suffix: &str) -> Vec<i64> {
    run(
        table,
        &format!("SELECT id, key, items FROM records {suffix}"),
    )
    .rows
    .into_iter()
    .map(|row| match row[0] {
        Scalar::Int(value) => value,
        _ => panic!("id"),
    })
    .collect()
}

#[test]
fn container_sort_is_typed_lexicographic_not_json() {
    let table = table();
    assert_eq!(
        ids(&table, "ORDER BY items ASC NULLS FIRST"),
        vec![0, 1, 2, 3, 5, 4]
    );
    assert_eq!(
        ids(&table, "ORDER BY items DESC NULLS LAST"),
        vec![4, 5, 3, 2, 1, 0]
    );
    assert_eq!(
        ids(&table, "ORDER BY key ASC NULLS FIRST, id"),
        vec![0, 1, 2, 4, 5, 3]
    );
    assert_eq!(
        ids(
            &table,
            "ORDER BY key DESC NULLS LAST, id DESC LIMIT 2 OFFSET 1"
        ),
        vec![5, 4]
    );
    let mut bytes = Vec::new();
    run(&table, "SELECT key FROM records ORDER BY key")
        .write_ndjson(&mut bytes)
        .unwrap();
    assert!(
        String::from_utf8(bytes)
            .unwrap()
            .contains("\"t\":\"struct\"")
    );
}

#[test]
fn container_groups_preserve_outer_null_and_all_null_struct() {
    let table = table();
    let result = run(
        &table,
        "SELECT key, COUNT(*) AS n, SUM(id) FROM records GROUP BY key ORDER BY key NULLS FIRST",
    );
    assert_eq!(
        result
            .rows
            .iter()
            .map(|row| row[1..].to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Scalar::Int(1), Scalar::Int(0)],
            vec![Scalar::Int(1), Scalar::Int(1)],
            vec![Scalar::Int(3), Scalar::Int(11)],
            vec![Scalar::Int(1), Scalar::Int(3)]
        ]
    );
    assert_eq!(result.rows[0][0], Scalar::Null);
    assert!(matches!(result.rows[1][0], Scalar::Struct(_)));
    assert_eq!(
        run(
            &table,
            "SELECT key, COUNT(*) FROM records GROUP BY key HAVING COUNT(*) > 1 AND key = key"
        )
        .rows
        .len(),
        1
    );
    assert_eq!(
        run(&table, "SELECT DISTINCT key FROM records ORDER BY key")
            .rows
            .len(),
        4
    );
    assert_eq!(
        run(&table, "SELECT DISTINCT items FROM records").rows.len(),
        6
    );
}

#[test]
fn container_predicates_obey_outer_three_valued_logic() {
    let table = table();
    let result = run(
        &table,
        "SELECT id, key = key, key != key, items IN (items, NULL), items NOT IN (items, NULL), items BETWEEN items AND items, NULLIF(key, key) FROM records ORDER BY id",
    );
    assert_eq!(result.rows[0][1..], vec![Scalar::Null; 6]);
    for row in &result.rows[1..] {
        assert_eq!(
            row[1..],
            vec![
                Scalar::Bool(true),
                Scalar::Bool(false),
                Scalar::Bool(true),
                Scalar::Bool(false),
                Scalar::Bool(true),
                Scalar::Null
            ]
        );
    }
    assert_eq!(
        ids(&table, "WHERE key = key ORDER BY id"),
        vec![1, 2, 3, 4, 5]
    );
    assert!(
        run(&table, "SELECT id FROM records WHERE items = NULL")
            .rows
            .is_empty()
    );
}

#[test]
fn container_extrema_retain_output_layout_and_skip_outer_nulls() {
    let table = table();
    let output = run(
        &table,
        "SELECT MIN(key), MAX(key), MIN(items), MAX(items), COUNT(items) FROM records",
    );
    assert_eq!(output.rows[0][2], Scalar::List(vec![]));
    assert_eq!(output.rows[0][3], Scalar::List(vec![Scalar::Int(10)]));
    assert_eq!(output.rows[0][4], Scalar::Int(5));
    assert!(matches!(
        output.schema.field(0).data_type(),
        DataType::Struct(_)
    ));
    assert!(matches!(
        output.schema.field(2).data_type(),
        DataType::List(_)
    ));
    assert_eq!(
        run(
            &table,
            "SELECT MIN(key), MAX(items) FROM records WHERE id < 0"
        )
        .rows,
        vec![vec![Scalar::Null, Scalar::Null]]
    );
}

#[test]
fn nested_element_join_predicates_keep_parent_offsets() {
    let lists = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![None, Some(2), Some(10)]),
        Some(vec![]),
        None,
    ]);
    let inner: ArrayRef = Arc::new(lists);
    let outer = ListArray::new(
        Arc::new(Field::new("item", inner.data_type().clone(), true)),
        arrow::buffer::OffsetBuffer::new(vec![0, 3].into()),
        inner,
        None,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "matrix",
        outer.data_type().clone(),
        true,
    )]));
    let table = ColumnarTable::try_new(
        schema.clone(),
        vec![RecordBatch::try_new(schema, vec![Arc::new(outer)]).unwrap()],
    )
    .unwrap();
    let rows = run(&table,"SELECT u.ordinality, v.ordinality FROM records, UNNEST(matrix) WITH ORDINALITY AS u LEFT JOIN UNNEST(matrix) WITH ORDINALITY AS v ON u.value = v.value ORDER BY u.ordinality").rows;
    assert_eq!(
        rows,
        vec![
            vec![Scalar::Int(1), Scalar::Int(1)],
            vec![Scalar::Int(2), Scalar::Int(2)],
            vec![Scalar::Int(3), Scalar::Null]
        ]
    );
    assert_eq!(run(&table,"SELECT u.value, COUNT(*) FROM records, UNNEST(matrix) AS u GROUP BY u.value ORDER BY u.value NULLS FIRST").rows.len(),3);
}

#[test]
fn physical_list_widths_are_comparable_but_leaf_types_are_not_coerced() {
    let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([Some(
        vec![Some(2), None],
    )]));
    let large = arrow::compute::cast(
        &list,
        &DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, true))),
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("small", list.data_type().clone(), true),
        Field::new("large", large.data_type().clone(), false),
    ]));
    let table = ColumnarTable::try_new(
        schema.clone(),
        vec![RecordBatch::try_new(schema, vec![list, large]).unwrap()],
    )
    .unwrap();
    assert_eq!(
        run(&table, "SELECT small = large FROM records").rows,
        vec![vec![Scalar::Bool(true)]]
    );
    let fixture = super::nested_tests::fixture();
    for sql in [
        "SELECT profile = profile.detail FROM records",
        "SELECT samples IN (profile) FROM records",
        "SELECT samples BETWEEN samples AND profile FROM records",
        "SELECT NULLIF(profile, profile.detail) FROM records",
        "SELECT SUM(samples) FROM records",
        "SELECT AVG(profile) FROM records",
        "SELECT CAST(samples AS VARCHAR) FROM records",
    ] {
        assert!(
            ColumnarQuery::prepare("records", fixture.schema().clone(), sql).is_err(),
            "{sql}"
        );
    }
}

#[test]
fn float_container_keys_canonicalize_signed_zero_and_nan_payloads() {
    let values = ListArray::from_iter_primitive::<Float64Type, _, _>([
        Some(vec![Some(-0.0)]),
        Some(vec![Some(0.0)]),
        Some(vec![Some(f64::NAN)]),
        Some(vec![Some(f64::from_bits(0x7ff8_0000_0000_0001))]),
    ]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "items",
        values.data_type().clone(),
        true,
    )]));
    let table = ColumnarTable::try_new(
        schema.clone(),
        vec![RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap()],
    )
    .unwrap();
    let output = run(
        &table,
        "SELECT items, COUNT(*) FROM records GROUP BY items ORDER BY items",
    );
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0][1], Scalar::Int(2));
    assert_eq!(output.rows[1][1], Scalar::Int(2));
    assert_eq!(
        run(&table, "SELECT items = items FROM records").rows,
        vec![vec![Scalar::Bool(true)]; 4]
    );
}

#[test]
fn recursive_keys_agree_with_comparison_and_do_not_encode_json() {
    use crate::execution::scalar::cmp;
    use crate::types::ScalarKey;
    use std::cmp::Ordering;
    let mut seed = 71u64;
    let mut values = Vec::new();
    for _ in 0..96 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        values.push(Scalar::Struct(vec![(
            "a".into(),
            Scalar::List(vec![
                if seed.is_multiple_of(3) {
                    Scalar::Null
                } else {
                    Scalar::Int((seed % 17) as i64)
                },
                Scalar::Str(format!("quoted\"\\{}", seed % 7)),
            ]),
        )]));
    }
    for left in &values {
        for right in &values {
            assert_eq!(
                cmp(left, right) == Some(Ordering::Equal),
                ScalarKey::from_scalar(left) == ScalarKey::from_scalar(right)
            );
            assert_eq!(cmp(left, right), cmp(right, left).map(Ordering::reverse));
        }
    }
    values.sort_by(|left, right| cmp(left, right).unwrap());
    for triple in values.windows(3) {
        assert!(cmp(&triple[0], &triple[2]) != Some(Ordering::Greater));
    }
    assert!(matches!(
        ScalarKey::from_scalar(&values[0]),
        ScalarKey::Struct(_)
    ));
}

#[test]
fn retained_nested_keys_and_extrema_obey_workspace_budget() {
    let strings =
        StringArray::from_iter_values((0..2048).map(|id| format!("{id:04}{}", "x".repeat(1024))));
    let array: ArrayRef = Arc::new(StructArray::new(
        vec![Field::new("text", DataType::Utf8, false)].into(),
        vec![Arc::new(strings)],
        None,
    ));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        array.data_type().clone(),
        true,
    )]));
    let table = ColumnarTable::try_new(
        schema.clone(),
        vec![RecordBatch::try_new(schema, vec![array]).unwrap()],
    )
    .unwrap();
    for sql in [
        "SELECT value, COUNT(*) FROM records GROUP BY value",
        "SELECT DISTINCT value FROM records",
        "SELECT value FROM records ORDER BY value",
    ] {
        let prepared = ColumnarQuery::prepare("records", table.schema().clone(), sql).unwrap();
        assert!(
            prepared
                .execute_with_memory_limit(&table, 1)
                .err()
                .unwrap()
                .contains("RESOURCE_EXHAUSTED"),
            "{sql}"
        );
        assert_eq!(prepared.execute(&table).unwrap().rows.len(), 2048);
    }
}

#[test]
fn container_min_max_budget_retained_values_and_skip_unreferenced_siblings() {
    let value: ArrayRef = Arc::new(StructArray::new(
        vec![Field::new("text", DataType::Utf8, false)].into(),
        vec![Arc::new(StringArray::from(vec![
            "x".repeat(2 * 1024 * 1024),
        ]))],
        None,
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", value.data_type().clone(), true),
        Field::new("hidden", DataType::UInt64, false),
    ]));
    let table = ColumnarTable::try_new(
        schema.clone(),
        vec![
            RecordBatch::try_new(
                schema,
                vec![
                    value,
                    Arc::new(arrow::array::UInt64Array::from(vec![u64::MAX])),
                ],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    for sql in [
        "SELECT MIN(value) FROM records",
        "SELECT MAX(value) FROM records",
    ] {
        let prepared = ColumnarQuery::prepare("records", table.schema().clone(), sql).unwrap();
        assert!(!prepared.explain().contains("columns=[hidden"));
        assert!(
            prepared
                .execute_with_memory_limit(&table, 1)
                .err()
                .unwrap()
                .contains("aggregate retained values")
        );
        assert_eq!(prepared.execute(&table).unwrap().rows.len(), 1);
    }
}

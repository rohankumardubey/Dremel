use crate::query::ColumnarQuery;
use crate::storage::ColumnarTable;
use crate::types::Scalar;
use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Int32Array, Int64Array, LargeListArray, ListArray,
    StructArray, UInt64Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn table(values: ArrayRef) -> ColumnarTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("items", values.data_type().clone(), true),
    ]));
    let ids = Arc::new(Int64Array::from_iter_values(0..values.len() as i64));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, values]).unwrap();
    let middle = batch.num_rows() / 2;
    ColumnarTable::try_new(
        schema,
        vec![
            batch.slice(0, middle),
            batch.slice(middle, batch.num_rows() - middle),
        ],
    )
    .unwrap()
}

fn fixture() -> ColumnarTable {
    table(Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
        [
            None,
            Some(vec![]),
            Some(vec![None]),
            Some(vec![Some(7), None, Some(9)]),
        ],
    )))
}

fn run(table: &ColumnarTable, sql: &str) -> Vec<Vec<Scalar>> {
    ColumnarQuery::prepare("records", table.schema().clone(), sql)
        .unwrap()
        .execute(table)
        .unwrap()
        .rows
}

#[test]
fn correlated_inner_left_and_one_based_ordinality() {
    let table = fixture();
    let inner = run(
        &table,
        "SELECT id, u.value, u.ordinality FROM records r CROSS JOIN LATERAL UNNEST(r.items) WITH ORDINALITY AS u",
    );
    assert_eq!(
        inner,
        vec![
            vec![Scalar::Int(2), Scalar::Null, Scalar::Int(1)],
            vec![Scalar::Int(3), Scalar::Int(7), Scalar::Int(1)],
            vec![Scalar::Int(3), Scalar::Null, Scalar::Int(2)],
            vec![Scalar::Int(3), Scalar::Int(9), Scalar::Int(3)]
        ]
    );
    let left = run(
        &table,
        "SELECT id, v, pos FROM records LEFT JOIN UNNEST(items) WITH ORDINALITY AS u(v,pos) ON TRUE",
    );
    assert_eq!(left.len(), 6);
    assert_eq!(left[0], vec![Scalar::Int(0), Scalar::Null, Scalar::Null]);
    assert_eq!(left[1], vec![Scalar::Int(1), Scalar::Null, Scalar::Null]);
    assert_eq!(&left[2..], &inner);
    assert_eq!(
        run(
            &table,
            "SELECT COUNT(*), COUNT(u.value), SUM(u.value), AVG(u.value) FROM records, UNNEST(items) AS u"
        ),
        vec![vec![
            Scalar::Int(4),
            Scalar::Int(2),
            Scalar::Int(16),
            Scalar::Float(8.0)
        ]]
    );
}

#[test]
fn on_filter_and_where_have_distinct_outer_join_semantics() {
    let table = fixture();
    assert_eq!(
        run(
            &table,
            "SELECT id, v, pos FROM records LEFT JOIN UNNEST(items) WITH ORDINALITY AS u(v,pos) ON v > 8"
        ),
        vec![
            vec![Scalar::Int(0), Scalar::Null, Scalar::Null],
            vec![Scalar::Int(1), Scalar::Null, Scalar::Null],
            vec![Scalar::Int(2), Scalar::Null, Scalar::Null],
            vec![Scalar::Int(3), Scalar::Int(9), Scalar::Int(3)]
        ]
    );
    assert_eq!(
        run(
            &table,
            "SELECT id, u.value, u.ordinality FROM records LEFT JOIN UNNEST(items) WITH ORDINALITY AS u ON TRUE WHERE u.value > 8"
        ),
        vec![vec![Scalar::Int(3), Scalar::Int(9), Scalar::Int(3)]]
    );
    assert_eq!(
        run(
            &table,
            "SELECT id, v FROM records INNER JOIN UNNEST(items) AS u(v) ON v > id"
        ),
        vec![
            vec![Scalar::Int(3), Scalar::Int(7)],
            vec![Scalar::Int(3), Scalar::Int(9)]
        ]
    );
    assert_eq!(
        run(
            &table,
            "SELECT id, COUNT(*), COUNT(u.value) FROM records LEFT JOIN UNNEST(items) AS u ON FALSE GROUP BY id ORDER BY id"
        ),
        (0..4)
            .map(|id| vec![Scalar::Int(id), Scalar::Int(1), Scalar::Int(0)])
            .collect::<Vec<_>>()
    );
}

#[test]
fn groups_having_distinct_sort_offset_and_empty_aggregates() {
    let table = fixture();
    assert_eq!(
        run(
            &table,
            "SELECT u.value, COUNT(*) AS n FROM records, UNNEST(items) AS u GROUP BY u.value HAVING COUNT(*) >= 1 ORDER BY value NULLS FIRST"
        ),
        vec![
            vec![Scalar::Null, Scalar::Int(2)],
            vec![Scalar::Int(7), Scalar::Int(1)],
            vec![Scalar::Int(9), Scalar::Int(1)]
        ]
    );
    assert_eq!(
        run(
            &table,
            "SELECT DISTINCT u.value FROM records, UNNEST(items) AS u ORDER BY value DESC NULLS LAST LIMIT 2 OFFSET 1"
        ),
        vec![vec![Scalar::Int(7)], vec![Scalar::Null]]
    );
    assert_eq!(
        run(
            &table,
            "SELECT COUNT(*), SUM(u.value) FROM records, UNNEST(items) AS u WHERE id < 2"
        ),
        vec![vec![Scalar::Int(0), Scalar::Null]]
    );
    assert_eq!(
        run(
            &table,
            "SELECT id, u.value FROM records, UNNEST(items) AS u LIMIT 1 OFFSET 1"
        ),
        vec![vec![Scalar::Int(3), Scalar::Int(7)]]
    );
    assert!(run(&table, "SELECT id FROM records, UNNEST(items) AS u LIMIT 0").is_empty());
    assert_eq!(
        run(&table, "SELECT * FROM records, UNNEST(items) AS u LIMIT 1")[0],
        vec![
            Scalar::Int(2),
            Scalar::List(vec![Scalar::Null]),
            Scalar::Null
        ]
    );
}

#[test]
fn independent_lists_form_cartesian_products_and_on_names_keep_scope() {
    let table = fixture();
    assert_eq!(
        run(
            &table,
            "SELECT COUNT(*) FROM records, UNNEST(items) AS u, UNNEST(items) AS v"
        ),
        vec![vec![Scalar::Int(10)]]
    );
    // A later relation's value must not retroactively make the earlier ON ambiguous.
    assert_eq!(
        run(
            &table,
            "SELECT COUNT(*) FROM records JOIN UNNEST(items) AS u ON value > 7 CROSS JOIN UNNEST(items) AS v"
        ),
        vec![vec![Scalar::Int(3)]]
    );
    assert_eq!(
        run(
            &table,
            "SELECT u.value, v.value FROM records, UNNEST(items) AS u, UNNEST(items) AS v WHERE u.value = v.value ORDER BY u.value"
        ),
        vec![
            vec![Scalar::Int(7), Scalar::Int(7)],
            vec![Scalar::Int(9), Scalar::Int(9)]
        ]
    );
}

#[test]
fn chained_list_structs_mask_null_ancestors_and_skip_unselected_siblings() {
    let tags = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(99)]),
        Some(vec![Some(1), None]),
        None,
        Some(vec![]),
    ]);
    let fields = vec![
        Field::new("tags", tags.data_type().clone(), true),
        Field::new("bad", DataType::UInt64, false),
    ];
    let children: Vec<ArrayRef> = vec![
        Arc::new(tags),
        Arc::new(UInt64Array::from(vec![u64::MAX; 4])),
    ];
    let structure = StructArray::new(
        fields.into(),
        children,
        Some(NullBuffer::from(vec![false, true, true, true])),
    );
    let items = ListArray::new(
        Arc::new(Field::new("item", structure.data_type().clone(), true)),
        OffsetBuffer::new(vec![0i32, 1, 4].into()),
        Arc::new(structure),
        None,
    );
    let table = table(Arc::new(items));
    assert_eq!(
        run(
            &table,
            "SELECT id, u.ordinality, v.value, v.ordinality FROM records, UNNEST(items) WITH ORDINALITY AS u LEFT JOIN UNNEST(u.value.tags) WITH ORDINALITY AS v ON TRUE"
        ),
        vec![
            vec![Scalar::Int(0), Scalar::Int(1), Scalar::Null, Scalar::Null],
            vec![
                Scalar::Int(1),
                Scalar::Int(1),
                Scalar::Int(1),
                Scalar::Int(1)
            ],
            vec![Scalar::Int(1), Scalar::Int(1), Scalar::Null, Scalar::Int(2)],
            vec![Scalar::Int(1), Scalar::Int(2), Scalar::Null, Scalar::Null],
            vec![Scalar::Int(1), Scalar::Int(3), Scalar::Null, Scalar::Null]
        ]
    );
    // No output values: counting must not validate the unrelated overflowing leaf.
    assert_eq!(
        run(&table, "SELECT COUNT(*) FROM records, UNNEST(items) AS u"),
        vec![vec![Scalar::Int(4)]]
    );
}

#[test]
fn sliced_large_fixed_and_list_of_lists_preserve_element_domains() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![99, 1, 2, 88]));
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    let large = LargeListArray::new(
        field.clone(),
        OffsetBuffer::new(vec![0i64, 1, 3, 4].into()),
        values.clone(),
        None,
    )
    .slice(1, 1);
    assert_eq!(
        run(
            &table(Arc::new(large)),
            "SELECT u.value, u.ordinality FROM records, UNNEST(items) WITH ORDINALITY AS u"
        ),
        vec![
            vec![Scalar::Int(1), Scalar::Int(1)],
            vec![Scalar::Int(2), Scalar::Int(2)]
        ]
    );
    let fixed =
        FixedSizeListArray::new(field, 2, values, Some(NullBuffer::from(vec![false, true])))
            .slice(1, 1);
    assert_eq!(
        run(
            &table(Arc::new(fixed)),
            "SELECT u.value FROM records, UNNEST(items) AS u"
        ),
        vec![vec![Scalar::Int(2)], vec![Scalar::Int(88)]]
    );
    let inner = ListArray::from_iter_primitive::<Int32Type, _, _>([
        None,
        Some(vec![]),
        Some(vec![Some(4), None]),
    ]);
    let outer = ListArray::new(
        Arc::new(Field::new("item", inner.data_type().clone(), true)),
        OffsetBuffer::new(vec![0i32, 3].into()),
        Arc::new(inner),
        None,
    );
    let nested = table(Arc::new(outer));
    assert_eq!(
        run(&nested, "SELECT a.value FROM records, UNNEST(items) AS a"),
        vec![
            vec![Scalar::Null],
            vec![Scalar::List(vec![])],
            vec![Scalar::List(vec![Scalar::Int(4), Scalar::Null])]
        ]
    );
    assert_eq!(
        run(
            &nested,
            "SELECT b.value FROM records, UNNEST(items) AS a, UNNEST(a.value) AS b"
        ),
        vec![vec![Scalar::Int(4)], vec![Scalar::Null]]
    );
}

#[test]
fn rejects_invalid_sources_aliases_forward_references_and_shapes() {
    let table = fixture();
    for sql in [
        "SELECT id FROM records, UNNEST(id) AS u",
        "SELECT id FROM records, UNNEST(v.value) AS u, UNNEST(items) AS v",
        "SELECT id FROM records, UNNEST(items) AS records",
        "SELECT id FROM records, UNNEST(items) AS u, UNNEST(items) AS u",
        "SELECT value FROM records, UNNEST(items) AS u, UNNEST(items) AS v",
        "SELECT id FROM records RIGHT JOIN UNNEST(items) AS u ON TRUE",
        "SELECT id FROM records FULL JOIN UNNEST(items) AS u ON TRUE",
        "SELECT id FROM records JOIN UNNEST(items) AS u ON SUM(u.value) > 0",
        "SELECT id FROM records JOIN UNNEST(items) AS u ON u.value",
        "SELECT id FROM records JOIN UNNEST(items) AS u ON v.value > 0 CROSS JOIN UNNEST(items) AS v",
        "SELECT id FROM records, UNNEST(items,id) AS u",
        "SELECT id FROM records, UNNEST(items) WITH ORDINALITY AS u(v)",
        "SELECT id FROM records, UNNEST(items) AS u(v,p)",
        "SELECT id FROM records, UNNEST(items) AS u(a.b)",
        "SELECT id FROM records CROSS JOIN LATERAL records r",
        "SELECT id FROM records, UNNEST(items)",
    ] {
        assert!(
            ColumnarQuery::prepare("records", table.schema().clone(), sql).is_err(),
            "accepted {sql}"
        );
    }
}

#[test]
fn deterministic_generated_lists_match_independent_expansion_oracle() {
    let lists: Vec<Option<Vec<Option<i32>>>> = (0..257)
        .map(|row| {
            if row % 11 == 0 {
                None
            } else {
                Some(
                    (0..row % 7)
                        .map(|index| {
                            if (row + index) % 5 == 0 {
                                None
                            } else {
                                Some(row * 7 + index)
                            }
                        })
                        .collect(),
                )
            }
        })
        .collect();
    let table = table(Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
        lists.clone(),
    )));
    let mut expected = Vec::new();
    for (row, list) in lists.iter().enumerate() {
        let matches: Vec<_> = list
            .iter()
            .flatten()
            .enumerate()
            .filter(|(_, value)| value.is_some_and(|v| v > 900))
            .collect();
        if matches.is_empty() {
            expected.push(vec![Scalar::Int(row as i64), Scalar::Null, Scalar::Null]);
        }
        for (index, value) in matches {
            expected.push(vec![
                Scalar::Int(row as i64),
                Scalar::Int(value.unwrap() as i64),
                Scalar::Int(index as i64 + 1),
            ]);
        }
    }
    assert_eq!(
        run(
            &table,
            "SELECT id, u.value, u.ordinality FROM records LEFT JOIN UNNEST(items) WITH ORDINALITY AS u ON u.value > 900"
        ),
        expected
    );
}

#[test]
fn expansion_workspace_and_results_obey_query_budget() {
    let list = ListArray::from_iter_primitive::<Int32Type, _, _>([Some(
        (0..100_000).map(Some).collect::<Vec<_>>(),
    )]);
    let table = table(Arc::new(list));
    let prepared = ColumnarQuery::prepare(
        "records",
        table.schema().clone(),
        "SELECT u.value FROM records, UNNEST(items) AS u",
    )
    .unwrap();
    assert!(
        prepared
            .execute_with_memory_limit(&table, 1)
            .err()
            .unwrap()
            .contains("RESOURCE_EXHAUSTED")
    );
    assert!(prepared.explain().contains("CorrelatedUnnestExec"));
    assert!(prepared.explain().contains("parent_copy=false"));
}

#[test]
fn null_parent_structs_hide_list_elements_and_valid_all_null_elements_count() {
    let nested = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(999)]),
        Some(vec![Some(4), None]),
        Some(vec![]),
    ]);
    let parent = StructArray::new(
        vec![Field::new("entries", nested.data_type().clone(), true)].into(),
        vec![Arc::new(nested)],
        Some(NullBuffer::from(vec![false, true, true])),
    );
    let data = table(Arc::new(parent));
    assert_eq!(
        run(
            &data,
            "SELECT id, u.value, u.ordinality FROM records LEFT JOIN UNNEST(items.entries) WITH ORDINALITY AS u ON TRUE"
        ),
        vec![
            vec![Scalar::Int(0), Scalar::Null, Scalar::Null],
            vec![Scalar::Int(1), Scalar::Int(4), Scalar::Int(1)],
            vec![Scalar::Int(1), Scalar::Null, Scalar::Int(2)],
            vec![Scalar::Int(2), Scalar::Null, Scalar::Null]
        ]
    );
    let element = StructArray::new(
        vec![Field::new("n", DataType::Int32, true)].into(),
        vec![Arc::new(Int32Array::from(vec![None, None]))],
        Some(NullBuffer::from(vec![true, false])),
    );
    let list = ListArray::new(
        Arc::new(Field::new("element", element.data_type().clone(), true)),
        OffsetBuffer::new(vec![0i32, 2].into()),
        Arc::new(element),
        None,
    );
    assert_eq!(
        run(
            &table(Arc::new(list)),
            "SELECT COUNT(*), COUNT(u.value), COUNT(u.value.n) FROM records, UNNEST(items) AS u"
        ),
        vec![vec![Scalar::Int(2), Scalar::Int(1), Scalar::Int(0)]]
    );
}

#[test]
fn deadline_failure_does_not_poison_later_execution_and_limits_stage_depth() {
    use crate::types::{ExecutionControl, set_execution_control};
    use std::sync::atomic::AtomicBool;
    let data = fixture();
    let prepared = ColumnarQuery::prepare(
        "records",
        data.schema().clone(),
        "SELECT COUNT(*) FROM records, UNNEST(items) AS u",
    )
    .unwrap();
    set_execution_control(Some(Arc::new(ExecutionControl {
        cancelled: AtomicBool::new(false),
        deadline: Some(std::time::Instant::now()),
    })));
    let stopped = prepared.execute(&data);
    set_execution_control(None);
    assert!(stopped.err().unwrap().contains("cancelled"));
    assert_eq!(
        prepared.execute(&data).unwrap().rows,
        vec![vec![Scalar::Int(4)]]
    );
    let sql = format!(
        "SELECT COUNT(*) FROM records{}",
        (0..33)
            .map(|index| format!(", UNNEST(items) AS u{index}"))
            .collect::<String>()
    );
    assert!(
        ColumnarQuery::prepare("records", data.schema().clone(), &sql)
            .err()
            .unwrap()
            .contains("32")
    );
    let parsed = crate::sql::Parser::new("SELECT COUNT(*) FROM events, UNNEST(event_id) AS u")
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        crate::storage::query_bindings(&parsed)
            .err()
            .unwrap()
            .contains("--table")
    );
}

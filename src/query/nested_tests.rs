use super::*;
use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Int32Array, Int64Array, LargeListArray, ListArray,
    StringArray, StructArray, UInt64Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn fixture() -> ColumnarTable {
    let detail = StructArray::new(
        vec![
            Field::new("Zip", DataType::Int32, false),
            Field::new("city", DataType::Utf8, true),
        ]
        .into(),
        vec![
            Arc::new(Int32Array::from(vec![999, 10, 20, 30])),
            Arc::new(StringArray::from(vec!["hidden", "A", "B", "C"])),
        ],
        Some(NullBuffer::from(vec![true, true, false, true])),
    );
    let profile = StructArray::new(
        vec![
            Field::new("detail", detail.data_type().clone(), true),
            Field::new("city", DataType::Utf8, true),
        ]
        .into(),
        vec![
            Arc::new(detail),
            Arc::new(StringArray::from(vec!["hidden", "NY", "LA", "IN"])),
        ],
        Some(NullBuffer::from(vec![false, true, true, true])),
    );
    let samples = ListArray::from_iter_primitive::<Int32Type, _, _>([
        None,
        Some(vec![]),
        Some(vec![None]),
        Some(vec![Some(1), None, Some(2)]),
    ]);
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![0, 1, 2, 3])),
        Arc::new(profile),
        Arc::new(samples),
    ];
    let schema = Arc::new(Schema::new(
        arrays
            .iter()
            .zip(["id", "profile", "samples"])
            .map(|(array, name)| Field::new(name, array.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    ColumnarTable::try_new(schema, vec![batch.slice(0, 2), batch.slice(2, 2)]).unwrap()
}

fn run(table: &ColumnarTable, sql: &str) -> QueryResult {
    ColumnarQuery::prepare("records", table.schema().clone(), sql)
        .unwrap()
        .execute(table)
        .unwrap()
}

fn single(array: ArrayRef) -> ColumnarTable {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        array.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
    ColumnarTable::try_new(schema, vec![batch]).unwrap()
}

#[test]
fn resolves_deep_case_insensitive_paths_and_parent_nulls() {
    let table = fixture();
    let result = run(
        &table,
        "SELECT r.profile.detail.zip AS zip, profile.city, profile.detail.city FROM records r ORDER BY zip ASC NULLS FIRST",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![Scalar::Null, Scalar::Null, Scalar::Null],
            vec![Scalar::Null, Scalar::Str("LA".into()), Scalar::Null],
            vec![
                Scalar::Int(10),
                Scalar::Str("NY".into()),
                Scalar::Str("A".into())
            ],
            vec![
                Scalar::Int(30),
                Scalar::Str("IN".into()),
                Scalar::Str("C".into())
            ],
        ]
    );
    assert_eq!(result.schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(
        run(
            &table,
            "SELECT profile.city FROM records profile WHERE profile.city = 'NY'"
        )
        .rows,
        vec![vec![Scalar::Str("NY".into())]]
    );
    assert_eq!(run(&table, "SELECT profile.detail.zip, SUM(id) FROM records GROUP BY profile.detail.zip HAVING COUNT(*) > 0 ORDER BY profile.detail.zip ASC NULLS FIRST").rows,
        vec![vec![Scalar::Null, Scalar::Int(2)], vec![Scalar::Int(10), Scalar::Int(1)], vec![Scalar::Int(30), Scalar::Int(3)]]);
}

#[test]
fn projects_typed_containers_and_nested_null_expressions() {
    let table = fixture();
    let result = run(
        &table,
        "SELECT profile, samples, CARDINALITY(samples), profile IS NULL, profile.detail IS NULL, CASE WHEN id > 1 THEN samples ELSE NULL END, COALESCE(NULL, profile), id FROM records ORDER BY id",
    );
    assert_eq!(result.rows.len(), 4);
    assert_eq!(result.rows[0][0], Scalar::Null);
    assert_eq!(result.rows[1][1], Scalar::List(vec![]));
    assert_eq!(result.rows[2][1], Scalar::List(vec![Scalar::Null]));
    assert_eq!(
        result.rows[3][1],
        Scalar::List(vec![Scalar::Int(1), Scalar::Null, Scalar::Int(2)])
    );
    assert_eq!(
        result
            .rows
            .iter()
            .map(|row| row[2].clone())
            .collect::<Vec<_>>(),
        vec![Scalar::Null, Scalar::Int(0), Scalar::Int(1), Scalar::Int(3)]
    );
    for row in &result.rows {
        assert_eq!(row[0], row[6]);
    }
    let DataType::Struct(fields) = result.schema.field(0).data_type() else {
        panic!("struct result schema")
    };
    let DataType::Struct(detail) = fields[0].data_type() else {
        panic!("nested struct schema")
    };
    assert_eq!(detail[0].data_type(), &DataType::Int64);
    let mut encoded = Vec::new();
    result.write_ndjson(&mut encoded).unwrap();
    let rows = String::from_utf8(encoded)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows[3][0]["t"], "struct");
    assert_eq!(rows[3][0]["v"]["detail"]["v"]["Zip"]["v"], 30);
    assert_eq!(rows[1][1]["t"], "list");
    assert_eq!(
        run(
            &table,
            "SELECT COUNT(profile), COUNT(profile.detail), COUNT(samples) FROM records"
        )
        .rows,
        vec![vec![Scalar::Int(3), Scalar::Int(2), Scalar::Int(3)]]
    );
    assert_eq!(
        run(&table, "SELECT * FROM records LIMIT 1").rows[0],
        vec![Scalar::Int(0), Scalar::Null, Scalar::Null]
    );
}

#[test]
fn preserves_sliced_large_and_fixed_lists() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(99),
        Some(1),
        None,
        Some(2),
        Some(88),
    ]));
    let element = Arc::new(Field::new("item", DataType::Int32, true));
    let large = LargeListArray::new(
        element.clone(),
        OffsetBuffer::new(vec![0i64, 1, 4, 5].into()),
        values.clone(),
        None,
    )
    .slice(1, 1);
    assert_eq!(
        run(
            &single(Arc::new(large)),
            "SELECT value, CARDINALITY(value) FROM records"
        )
        .rows,
        vec![vec![
            Scalar::List(vec![Scalar::Int(1), Scalar::Null, Scalar::Int(2)]),
            Scalar::Int(3)
        ]]
    );
    let fixed = FixedSizeListArray::new(
        element,
        2,
        Arc::new(Int32Array::from(vec![99, 88, 1, 2, 77, 66])),
        Some(NullBuffer::from(vec![true, true, false])),
    )
    .slice(1, 2);
    assert_eq!(
        run(
            &single(Arc::new(fixed)),
            "SELECT value, CARDINALITY(value) FROM records"
        )
        .rows,
        vec![
            vec![
                Scalar::List(vec![Scalar::Int(1), Scalar::Int(2)]),
                Scalar::Int(2)
            ],
            vec![Scalar::Null, Scalar::Null]
        ]
    );
}

#[test]
fn masks_hidden_invalid_values_before_normalization() {
    let structure = StructArray::new(
        vec![Field::new("large", DataType::UInt64, false)].into(),
        vec![Arc::new(UInt64Array::from(vec![u64::MAX, 42]))],
        Some(NullBuffer::from(vec![false, true])),
    );
    let table = single(Arc::new(structure));
    assert_eq!(
        run(&table, "SELECT value.large FROM records").rows,
        vec![vec![Scalar::Null], vec![Scalar::Int(42)]]
    );
    let list = ListArray::new(
        Arc::new(Field::new("item", DataType::UInt64, true)),
        OffsetBuffer::new(vec![0, 1, 2].into()),
        Arc::new(UInt64Array::from(vec![u64::MAX, 42])),
        Some(NullBuffer::from(vec![false, true])),
    );
    assert_eq!(
        run(&single(Arc::new(list)), "SELECT value FROM records").rows,
        vec![
            vec![Scalar::Null],
            vec![Scalar::List(vec![Scalar::Int(42)])]
        ]
    );
}

#[test]
fn rejects_unsupported_nested_operations_at_prepare_time() {
    let table = fixture();
    for sql in [
        "SELECT profile.missing FROM records",
        "SELECT items.price FROM records",
        "SELECT profile FROM records ORDER BY profile",
        "SELECT DISTINCT samples FROM records",
        "SELECT profile, COUNT(*) FROM records GROUP BY profile",
        "SELECT MIN(profile) FROM records",
        "SELECT CAST(samples AS VARCHAR) FROM records",
        "SELECT samples = samples FROM records",
        "SELECT CONCAT(profile) FROM records",
        "SELECT NULLIF(profile, profile) FROM records",
        "SELECT CARDINALITY(profile) FROM records",
        "SELECT CASE WHEN id > 0 THEN profile ELSE samples END FROM records",
        "SELECT COALESCE(profile, profile.detail) FROM records",
    ] {
        assert!(
            ColumnarQuery::prepare("records", table.schema().clone(), sql).is_err(),
            "{sql}"
        );
    }
    let fields = vec![
        Field::new("city", DataType::Int32, true),
        Field::new("City", DataType::Int32, true),
    ];
    let schema = Arc::new(Schema::new(vec![Field::new(
        "profile",
        DataType::Struct(fields.into()),
        true,
    )]));
    assert!(ColumnarQuery::prepare("records", schema, "SELECT profile.city FROM records").is_err());
    let schema = Arc::new(Schema::new(vec![
        Field::new("city", DataType::Int32, true),
        Field::new(
            "profile",
            DataType::Struct(vec![Field::new("city", DataType::Int32, true)].into()),
            true,
        ),
    ]));
    assert!(
        ColumnarQuery::prepare(
            "records",
            schema,
            "SELECT profile.city FROM records profile"
        )
        .err()
        .unwrap()
        .contains("ambiguous")
    );
}

#[test]
fn budgets_recursive_nested_result_buffers() {
    let children = StringArray::from_iter_values(std::iter::repeat_n("x".repeat(1024), 2048));
    let structure = StructArray::new(
        vec![Field::new("text", DataType::Utf8, true)].into(),
        vec![Arc::new(children)],
        None,
    );
    let table = single(Arc::new(structure));
    let query = ColumnarQuery::prepare(
        "records",
        table.schema().clone(),
        "SELECT value FROM records",
    )
    .unwrap();
    assert!(
        query
            .execute_with_memory_limit(&table, 1)
            .err()
            .unwrap()
            .contains("RESOURCE_EXHAUSTED")
    );
    assert_eq!(query.execute(&table).unwrap().rows.len(), 2048);
}

#[test]
fn distinguishes_null_structs_from_valid_all_null_structs() {
    let structure = StructArray::new(
        vec![Field::new("child", DataType::Int32, true)].into(),
        vec![Arc::new(Int32Array::from(vec![None, None]))],
        Some(NullBuffer::from(vec![false, true])),
    );
    let table = single(Arc::new(structure));
    assert_eq!(
        run(&table, "SELECT value, value IS NULL FROM records").rows,
        vec![
            vec![Scalar::Null, Scalar::Bool(true)],
            vec![
                Scalar::Struct(vec![("child".into(), Scalar::Null)]),
                Scalar::Bool(false)
            ]
        ]
    );
    assert_eq!(
        run(
            &table,
            "SELECT COUNT(value), COUNT(value.child) FROM records"
        )
        .rows,
        vec![vec![Scalar::Int(1), Scalar::Int(0)]]
    );
}

#[test]
fn validates_only_selected_siblings_and_rejects_malformed_paths() {
    use arrow::array::{BinaryArray, TimestampMicrosecondArray};
    let structure = StructArray::new(
        vec![
            Field::new("good", DataType::Int64, false),
            Field::new("unsupported", DataType::Binary, true),
        ]
        .into(),
        vec![
            Arc::new(Int64Array::from(vec![42])),
            Arc::new(BinaryArray::from(vec![Some(b"data".as_slice())])),
        ],
        None,
    );
    let table = single(Arc::new(structure));
    assert_eq!(
        run(&table, "SELECT value.good FROM records").rows,
        vec![vec![Scalar::Int(42)]]
    );
    for sql in [
        "SELECT value FROM records",
        "SELECT value. FROM records",
        "SELECT value..good FROM records",
        "SELECT value.good FROM records GROUP BY value.",
    ] {
        assert!(
            ColumnarQuery::prepare("records", table.schema().clone(), sql).is_err(),
            "{sql}"
        );
    }
    let structure = StructArray::new(
        vec![Field::new(
            "time",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            false,
        )]
        .into(),
        vec![Arc::new(TimestampMicrosecondArray::from(vec![
            1, 1_000_000,
        ]))],
        Some(NullBuffer::from(vec![false, true])),
    );
    assert_eq!(
        run(
            &single(Arc::new(structure)),
            "SELECT value.time FROM records"
        )
        .rows,
        vec![vec![Scalar::Null], vec![Scalar::Int(1)]]
    );
}

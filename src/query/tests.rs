use super::*;
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int32Array, Int64Array, ListArray,
    StringArray, StringDictionaryBuilder, TimestampMicrosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Int8Type, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn table() -> ColumnarTable {
    let fields = vec![
        Field::new("reading_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
        Field::new("value", DataType::Int32, true),
        Field::new("accepted", DataType::Boolean, true),
    ];
    let schema = Arc::new(Schema::new(fields));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
        Arc::new(StringArray::from(vec![
            Some("US"),
            None,
            Some("US"),
            Some("IN"),
        ])),
        Arc::new(Int32Array::from(vec![Some(10), None, Some(30), Some(40)])),
        Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            Some(true),
        ])),
    ];
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    ColumnarTable::try_new(schema, vec![batch.slice(0, 2), batch.slice(2, 2)]).unwrap()
}

fn query(sql: &str) -> QueryResult {
    let table = table();
    ColumnarQuery::prepare("readings", table.schema().clone(), sql)
        .unwrap()
        .execute(&table)
        .unwrap()
}

#[test]
fn queries_arbitrary_nullable_columns_across_batches() {
    let result = query(
        "SELECT r.reading_id, COALESCE(r.value, 0) AS amount FROM readings r WHERE r.accepted OR r.value IS NULL ORDER BY reading_id DESC LIMIT 2 OFFSET 1",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![Scalar::Int(2), Scalar::Int(0)],
            vec![Scalar::Int(1), Scalar::Int(10)]
        ]
    );
    assert_eq!(result.schema.field(1).name(), "amount");
    assert_eq!(result.schema.field(1).data_type(), &DataType::Int64);
}

#[test]
fn groups_nulls_and_merges_aggregates_across_batches() {
    let result = query(
        "SELECT region, COUNT(*) AS rows, COUNT(value) AS present, SUM(value) AS total, AVG(value) AS mean FROM readings GROUP BY region HAVING COUNT(*) >= 1 ORDER BY region ASC NULLS FIRST",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![
                Scalar::Null,
                Scalar::Int(1),
                Scalar::Int(0),
                Scalar::Null,
                Scalar::Null
            ],
            vec![
                Scalar::Str("IN".into()),
                Scalar::Int(1),
                Scalar::Int(1),
                Scalar::Int(40),
                Scalar::Float(40.0)
            ],
            vec![
                Scalar::Str("US".into()),
                Scalar::Int(2),
                Scalar::Int(2),
                Scalar::Int(40),
                Scalar::Float(20.0)
            ],
        ]
    );
    assert_eq!(query("SELECT COUNT(*), SUM(value), MIN(region), MAX(value) FROM readings WHERE reading_id < 0").rows,
        vec![vec![Scalar::Int(0), Scalar::Null, Scalar::Null, Scalar::Null]]);
}

#[test]
fn integer_sum_preserves_precision_and_reports_overflow_as_null() {
    for (values, expected) in [
        (
            vec![9_007_199_254_740_993, 2],
            Scalar::Int(9_007_199_254_740_995),
        ),
        (vec![i64::MAX, 1], Scalar::Null),
    ] {
        let table = one_column("value", Arc::new(Int64Array::from(values)));
        let result = ColumnarQuery::prepare(
            "readings",
            table.schema().clone(),
            "SELECT SUM(value) FROM readings",
        )
        .unwrap()
        .execute(&table)
        .unwrap();
        assert_eq!(result.rows, vec![vec![expected]]);
    }
}

#[test]
fn expands_star_and_preserves_distinct_nulls() {
    let result = query("SELECT * FROM readings LIMIT 1");
    assert_eq!(result.schema.fields().len(), 4);
    assert_eq!(
        result.rows[0],
        vec![
            Scalar::Int(1),
            Scalar::Str("US".into()),
            Scalar::Int(10),
            Scalar::Bool(true)
        ]
    );
    assert_eq!(
        query("SELECT DISTINCT region FROM readings ORDER BY region ASC NULLS LAST").rows,
        vec![
            vec![Scalar::Str("IN".into())],
            vec![Scalar::Str("US".into())],
            vec![Scalar::Null]
        ]
    );
}

#[test]
fn rejects_unknown_names_types_functions_and_query_shapes() {
    let table = table();
    for (sql, message) in [
        ("SELECT reading_id FROM missing", "unknown table"),
        ("SELECT missing FROM readings", "unknown column"),
        ("SELECT reading_id FROM readings WHERE value", "BOOLEAN"),
        ("SELECT region + 1 FROM readings", "incompatible"),
        (
            "SELECT missing_function(value) FROM readings",
            "unsupported function",
        ),
        ("SELECT SUM(region) FROM readings", "numeric"),
        ("SELECT SUM(*) FROM readings", "unsupported"),
        (
            "SELECT region, COUNT(*) FROM readings GROUP BY region HAVING value > 0",
            "must be grouped",
        ),
        (
            "SELECT reading_id FROM readings HAVING reading_id > 0",
            "HAVING requires",
        ),
        (
            "SELECT CAST(value AS DECIMAL(10,3)) FROM readings",
            "DECIMAL(18,2)",
        ),
        (
            "SELECT reading_id FROM readings ORDER BY missing",
            "ORDER BY",
        ),
        (
            "SELECT reading_id FROM readings WHERE COUNT(*) > 0",
            "not allowed in WHERE",
        ),
        ("SELECT SUM(COUNT(value)) FROM readings", "nested aggregate"),
        (
            "SELECT reading_id FROM readings UNION SELECT reading_id FROM readings",
            "catalog",
        ),
        ("SELECT ROW_NUMBER() OVER () FROM readings", "windows"),
    ] {
        let error = ColumnarQuery::prepare("readings", table.schema().clone(), sql)
            .err()
            .unwrap();
        assert!(error.contains(message), "{sql}: {error}");
    }
}

fn one_column(name: &str, array: ArrayRef) -> ColumnarTable {
    let schema = Arc::new(Schema::new(vec![Field::new(
        name,
        array.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
    ColumnarTable::try_new(schema, vec![batch]).unwrap()
}

#[test]
fn converts_dictionary_decimal_date_and_timestamp_columns() {
    let mut builder = StringDictionaryBuilder::<Int8Type>::new();
    builder.append("US").unwrap();
    builder.append_null();
    builder.append("IN").unwrap();
    for (array, expected) in [
        (
            Arc::new(builder.finish()) as ArrayRef,
            vec![
                Scalar::Str("US".into()),
                Scalar::Null,
                Scalar::Str("IN".into()),
            ],
        ),
        (
            Arc::new(
                Decimal128Array::from(vec![Some(123), None])
                    .with_precision_and_scale(18, 2)
                    .unwrap(),
            ),
            vec![Scalar::Decimal(123), Scalar::Null],
        ),
        (
            Arc::new(Date32Array::from(vec![Some(0), Some(-1), None])),
            vec![
                Scalar::Str("1970-01-01".into()),
                Scalar::Str("1969-12-31".into()),
                Scalar::Null,
            ],
        ),
        (
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_000_000),
                Some(-1_000_000),
                None,
            ])),
            vec![Scalar::Int(1), Scalar::Int(-1), Scalar::Null],
        ),
    ] {
        let table = one_column("value", array);
        let result = ColumnarQuery::prepare(
            "readings",
            table.schema().clone(),
            "SELECT value FROM readings",
        )
        .unwrap()
        .execute(&table)
        .unwrap();
        assert_eq!(
            result.rows,
            expected
                .into_iter()
                .map(|value| vec![value])
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn rejects_lossy_values_and_stale_prepared_schemas() {
    for array in [
        Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef,
        Arc::new(TimestampMicrosecondArray::from(vec![1])),
        Arc::new(
            Decimal128Array::from(vec![i128::from(i64::MAX) + 1])
                .with_precision_and_scale(20, 2)
                .unwrap(),
        ),
    ] {
        let table = one_column("value", array);
        assert!(
            ColumnarQuery::prepare(
                "readings",
                table.schema().clone(),
                "SELECT value FROM readings"
            )
            .unwrap()
            .execute(&table)
            .is_err()
        );
    }
    let first = table();
    let query = ColumnarQuery::prepare(
        "readings",
        first.schema().clone(),
        "SELECT value FROM readings",
    )
    .unwrap();
    let second = one_column("value", Arc::new(StringArray::from(vec!["changed"])));
    assert!(
        query
            .execute(&second)
            .err()
            .unwrap()
            .contains("schema changed")
    );
}

#[test]
fn query_budget_failure_does_not_poison_the_next_execution() {
    let table = one_column(
        "value",
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            "x".repeat(1024),
            2048,
        ))),
    );
    let query = ColumnarQuery::prepare(
        "readings",
        table.schema().clone(),
        "SELECT value FROM readings",
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
fn binds_case_insensitive_names_and_rejects_ambiguous_schema() {
    let table = one_column("VALUE", Arc::new(Int32Array::from(vec![42])));
    assert_eq!(
        ColumnarQuery::prepare(
            "Readings",
            table.schema().clone(),
            "SELECT value FROM READINGS"
        )
        .unwrap()
        .execute(&table)
        .unwrap()
        .rows,
        vec![vec![Scalar::Int(42)]]
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("Value", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
    ]));
    assert!(
        ColumnarQuery::prepare("readings", schema, "SELECT value FROM readings")
            .err()
            .unwrap()
            .contains("ambiguous schema")
    );
}

#[test]
fn unreferenced_nested_fields_are_retained_but_not_evaluated() {
    let list = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(1)]),
        Some(vec![]),
        None,
    ]);
    let table = one_column("samples", Arc::new(list));
    let count = ColumnarQuery::prepare(
        "readings",
        table.schema().clone(),
        "SELECT COUNT(*) FROM readings",
    )
    .unwrap();
    assert_eq!(
        count.execute(&table).unwrap().rows,
        vec![vec![Scalar::Int(3)]]
    );
    assert!(
        ColumnarQuery::prepare(
            "readings",
            table.schema().clone(),
            "SELECT samples FROM readings"
        )
        .err()
        .unwrap()
        .contains("nested")
    );
}

#[test]
fn generic_queries_round_trip_through_official_file_readers() {
    use arrow::ipc::writer::FileWriter;
    use parquet::arrow::ArrowWriter;
    use std::fs::File;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let source = table();
    for extension in ["arrow", "parquet"] {
        let path = std::env::temp_dir().join(format!(
            "dremel-schema-{}-{}.{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
            extension
        ));
        let file = File::create(&path).unwrap();
        if extension == "arrow" {
            let mut writer = FileWriter::try_new(file, source.schema()).unwrap();
            for batch in source.batches() {
                writer.write(batch).unwrap();
            }
            writer.finish().unwrap();
        } else {
            let mut writer = ArrowWriter::try_new(file, source.schema().clone(), None).unwrap();
            for batch in source.batches() {
                writer.write(batch).unwrap();
            }
            writer.close().unwrap();
        }
        let loaded = ColumnarTable::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let prepared = ColumnarQuery::prepare("readings", loaded.schema().clone(), "SELECT region, SUM(value) AS total FROM readings GROUP BY region ORDER BY region ASC NULLS FIRST").unwrap();
        let result = prepared.execute(&loaded).unwrap();
        assert_eq!(
            result.rows,
            vec![
                vec![Scalar::Null, Scalar::Null],
                vec![Scalar::Str("IN".into()), Scalar::Int(40)],
                vec![Scalar::Str("US".into()), Scalar::Int(40)]
            ]
        );
        let mut json = Vec::new();
        result.write_ndjson(&mut json).unwrap();
        assert_eq!(String::from_utf8(json).unwrap().lines().count(), 3);
    }
}

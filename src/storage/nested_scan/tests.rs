use super::*;
use arrow::array::{Array, ArrayRef, Int64Array, ListArray, StringArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Fixture(std::path::PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn fixture() -> Fixture {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!(
        "dremel-nested-scan-{}-{}.parquet",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let profile = StructArray::from(vec![
        (
            Arc::new(Field::new("city", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![
                Some("NY"),
                None,
                Some("hidden"),
                Some("LA"),
            ])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("noise", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
        ),
    ]);
    let profile = StructArray::new(
        profile.fields().clone(),
        profile.columns().to_vec(),
        Some(NullBuffer::from(vec![true, true, false, true])),
    );
    let item = Arc::new(Field::new("element", DataType::Int64, true));
    // null, empty, [null, 7], [8]; the null list carries no values.
    let list = ListArray::new(
        item,
        OffsetBuffer::new(vec![0i32, 0, 0, 2, 3].into()),
        Arc::new(Int64Array::from(vec![None, Some(7), Some(8)])),
        Some(NullBuffer::from(vec![false, true, true, true])),
    );
    let batch = RecordBatch::try_from_iter(vec![
        ("profile", Arc::new(profile) as ArrayRef),
        ("values", Arc::new(list) as ArrayRef),
    ])
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Fixture(path)
}

#[test]
fn preserves_parent_validity_list_nulls_empty_lists_and_elements() {
    let file = fixture();
    let mut scan = ParquetScan::open(
        &file.0,
        ParquetScanOptions {
            columns: vec!["profile.city".into(), "values".into()],
            batch_size: 4,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(scan.plan().selected_leaf_columns, vec![0, 2]);
    assert_eq!(scan.plan().max_definition_levels, vec![2, 3]);
    assert_eq!(scan.plan().max_repetition_levels, vec![0, 1]);
    let batch = scan.next().unwrap().unwrap();
    let profile = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(profile.num_columns(), 1);
    assert!(profile.is_null(2));
    assert!(!profile.is_null(1));
    assert!(profile.column(0).is_null(1));
    let list = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(list.is_null(0));
    assert!(!list.is_null(1));
    assert_eq!(list.value_length(1), 0);
    assert_eq!(list.value_length(2), 2);
    assert!(list.value(2).is_null(0));
    assert!(scan.next().is_none());
    assert_eq!(scan.rows_read, 4);
}

#[test]
fn selects_row_groups_in_file_order_and_batches_without_collecting() {
    let file = fixture();
    let mut scan = ParquetScan::open(
        &file.0,
        ParquetScanOptions {
            row_groups: Some(vec![1]),
            batch_size: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(scan.plan().selected_rows, 2);
    assert_eq!(
        scan.by_ref()
            .map(|batch| batch.unwrap().num_rows())
            .sum::<usize>(),
        2
    );
    assert_eq!(scan.batches_read, 2);
    assert!(scan.peak_decoded_batch_bytes > 0);
    let mut empty = ParquetScan::open(
        &file.0,
        ParquetScanOptions {
            row_groups: Some(vec![]),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(empty.plan().selected_compressed_bytes, 0);
    assert!(empty.next().is_none());
    let order = ParquetScan::open(
        &file.0,
        ParquetScanOptions {
            row_groups: Some(vec![1, 0]),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(order.plan().selected_row_groups, vec![0, 1]);
}

#[test]
fn rejects_invalid_projection_groups_batch_size_and_corrupt_input() {
    let file = fixture();
    for options in [
        ParquetScanOptions {
            columns: vec!["profile.missing".into()],
            ..Default::default()
        },
        ParquetScanOptions {
            columns: vec!["profile..city".into()],
            ..Default::default()
        },
        ParquetScanOptions {
            row_groups: Some(vec![2]),
            ..Default::default()
        },
        ParquetScanOptions {
            row_groups: Some(vec![0, 0]),
            ..Default::default()
        },
        ParquetScanOptions {
            batch_size: 0,
            ..Default::default()
        },
    ] {
        assert!(ParquetScan::open(&file.0, options).is_err());
    }
    assert!(ParquetScan::open("Cargo.toml", Default::default()).is_err());
}

#[test]
fn list_struct_paths_are_logical_and_overlap_is_deduplicated() {
    let fields = vec![
        Arc::new(Field::new("price", DataType::Int64, true)),
        Arc::new(Field::new("label", DataType::Utf8, true)),
    ];
    let schema = Schema::new(vec![Field::new(
        "items",
        DataType::List(Arc::new(Field::new(
            "element",
            DataType::Struct(fields.into()),
            true,
        ))),
        true,
    )]);
    let paths = projection::leaf_paths(&schema).unwrap();
    assert_eq!(paths, vec![vec!["items", "price"], vec!["items", "label"]]);
    assert_eq!(
        projection::select(
            &paths,
            &["items.price".into(), "items".into(), "items.price".into()]
        )
        .unwrap(),
        vec![0, 1]
    );
    assert!(projection::select(&paths, &["items.element.price".into()]).is_err());
    let duplicate = Schema::new(vec![Field::new(
        "profile",
        DataType::Struct(
            vec![
                Arc::new(Field::new("name", DataType::Int64, true)),
                Arc::new(Field::new("name", DataType::Utf8, true)),
            ]
            .into(),
        ),
        true,
    )]);
    assert!(
        projection::leaf_paths(&duplicate)
            .unwrap_err()
            .contains("ambiguous")
    );
}

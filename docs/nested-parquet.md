# Nested Parquet scans

`dremel scan` reads projected Parquet columns in Arrow batches using the
official Apache Parquet Rust implementation. It supports nested STRUCT,
LIST, LARGE_LIST, and repeated lists without assembling every record into
scalar rows. Definition and repetition levels are decoded by the Apache
reader, not by the engine's separate educational shredding example.

## CLI

```bash
./target/release/dremel scan --data records.parquet \
  --columns record_id,profile.city,items.price \
  --row-groups 0,2 --batch-size 4096 --output selected.arrow
```

Paths are case sensitive. STRUCT children use dot-separated names. LIST
elements are traversed implicitly: `items.price` selects `price` inside each
element of a list of structs. `items` selects all descendant leaves. Repeated
and overlapping paths are deduplicated; results retain file-schema order,
not selection order. Literal dots inside field names cannot be escaped in a
projection path. Unknown paths and duplicate field names are rejected.

Omit `--columns` to read every leaf. Row-group indexes are zero based; omit
`--row-groups` to read every group or use `--row-groups none` to select no rows.
Explicit groups are read in file order, regardless of argument order. Invalid
or duplicate indexes and a zero batch size are errors.

The optional output is an Arrow IPC file. Existing destinations are never
overwritten. Rust writes a staging file beside the destination and publishes
it only after decoding and IPC finalization succeed, using an atomic hard link.
The destination filesystem must support hard links. Normal errors clean up
the staging file; forced process termination may leave a staging file behind.
This is not a crash-recovery or transactional storage protocol.

Without `--output`, each batch is decoded and discarded. The command always
prints a JSON summary after successful completion. It does not emit SQL's
typed JSON rows and does not accept SQL or query-memory flags.

## Typed API

```rust
use dremel::{ParquetScan, ParquetScanOptions};

let scan = ParquetScan::open("records.parquet", ParquetScanOptions {
    columns: vec!["profile.city".into(), "items.price".into()],
    row_groups: Some(vec![0, 2]),
    batch_size: 4096,
})?;

let projected_schema = scan.plan().schema.clone();
for batch in scan {
    let batch = batch?;
    // Consume the typed arrays, then drop the batch before reading the next.
    println!("{} columns, {} rows", batch.num_columns(), batch.num_rows());
}
```

Selecting `profile.city` returns a STRUCT containing `city`, not a flattened
nullable string. A null parent struct remains distinct from a present struct
whose child is null. Lists retain their offsets, validity, null elements,
empty collections, and repeated-container boundaries. No rows are exploded.
An unselected sibling is not decoded by the Parquet reader.

Keeping returned batches keeps their Arrow buffers alive. Batch size limits
rows, not bytes: a single row with a huge repeated field can require a large
batch. Decoder buffers, dictionaries, metadata, and retained results also use
memory. There is no hard memory budget or parallel scan scheduler on this path.

## Metrics and correctness

The plan exposes selected leaf indexes and paths, selected row groups and
rows, compressed column-chunk sizes, and each selected leaf's maximum
definition/repetition levels. These maxima describe the file schema, not a
dump of the per-value encoded level streams. Selected bytes are metadata
sizes, not observed filesystem reads or total I/O including the footer.

Runtime metrics count decoded rows and batches and estimate peak decoded
batch bytes. The reader verifies that the selected row count was fully decoded
before completing. CLI exports are only published after that check.

The regression suite covers optional parents, null/empty lists, null list
elements, lists of structs, lists of lists, required fields, LARGE_LIST,
row-group boundaries, empty scans/files, invalid selections, truncated input,
and damaged pages. A damaged unselected sibling must not break a projected
scan. A damaged selected column must fail without publishing an IPC export.

## Benchmarks

```bash
.venv/bin/python benchmarks/scripts/run_nested_benchmark.py \
  --rows 100000 --warmup 3 --iterations 20
```

The benchmark generates a deterministic nested Parquet fixture with official
PyArrow, checks exported Rust and C++ schemas and values against PyArrow and
the original full fixture, then interleaves timed scans. Both tools use one
thread, identical leaf/row-group selections, and the same row batch size.
Timing includes metadata and selected-column decoding, excludes process
startup and IPC export, and uses a warmed filesystem cache. The C++ scan
adapter is a standalone official-reader control, not nested SQL in the
reference engine. Peak batch estimates differ in their accounting conventions
and are not an apples-to-apples RSS ranking.

Results and an interactive report are written under `results/nested/`; the
terminal prints the report URL. The full runner enables this suite by default;
set `NESTED_PARQUET=0` to disable it. Benchmarks remain manual in CI.

Nested SQL field binding, UNNEST, nested joins/aggregates, automatic partition
pruning, and a complete query memory budget remain separate work.

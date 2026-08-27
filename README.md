# Dremel Bench

Two small columnar SQL engines, one in Rust and one in C++, built to answer the
same queries over equivalent data. The repository is a practical test bed for
query execution, optimization, and concurrent workload scheduling rather than
a general-purpose database.

The design borrows the columnar layout and nested-record ideas described in the
[Dremel paper](https://research.google/pubs/dremel-interactive-analysis-of-web-scale-datasets-2/).
It is an independent implementation and is not Google Dremel or BigQuery.

## What is included

| Area | Implementation |
| --- | --- |
| Engines | Rust 1.97.1 (Rust 2024) and LLVM Clang 22.1.8 (C++26) |
| Storage | DREMCOL1, Arrow IPC, and Apache Parquet projection with row-group pruning |
| Execution | Bounded Parquet and result streaming, spillable aggregation, batched scans, joins and windows |
| Optimizer | Scan filters, transitive predicates, pruning, contradiction elimination, selectivity-aware join ordering and Top-K |
| Workloads | 64 baseline, 16 hardening, 68 SQL, 12 optimizer, 15 Parquet, 5 spill, 5 result streaming and 5 concurrency cases |
| Validation | Cross-engine typed results, SQLite differential tests and plan assertions |

The toolchains are pinned so a later compiler update does not silently change
the comparison. Rust's `2024` label is the language edition, not the compiler
release year.

## Quick start

Requirements:

- Python 3.11 or newer
- Rust 1.97.1 with `rustfmt` and `clippy`
- CMake 3.20 or newer
- LLVM Clang 22 with C++26 support
- Apache Arrow C++ and Parquet 25.0.1
- PyArrow 25.0.1 from `requirements.txt`

On macOS, install the native dependencies and Python package with:

```bash
brew install llvm cmake apache-arrow
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Run a small end-to-end check:

```bash
DATASET_ROWS=20000 WARMUP=1 ITERATIONS=3 ./benchmark.sh
```

This generates the dataset, builds and tests both engines, validates their
results, and runs every enabled workload. Generated data, build products, and
reports stay under ignored directories.

The default benchmark uses one million events, three warmups, and twenty timed
iterations:

```bash
./benchmark.sh
```

Results are written to `results/` as JSON, CSV, environment metadata, and a
plain-text summary. Raw nanosecond samples are retained in the JSON reports.

## Benchmark configuration

The runner is configured through environment variables:

```bash
DATASET_ROWS=5000000 \
DATASET_SEED=12345 \
BENCH_THREADS=8 \
BATCH_SIZE=8192 \
WARMUP=5 \
ITERATIONS=30 \
TIE_THRESHOLD_PCT=0 \
LTO=1 \
NATIVE=1 \
./benchmark.sh
```

`EXTENDED`, `SQL_V1`, `OPTIMIZER`, `CONCURRENCY`, `STORAGE`,
`MEMORY_BOUNDED`, `PARQUET`, `SPILL`, and `RESULT_STREAMING` default to `1`.
Set any of them to `0` to skip that suite. The storage suite verifies equal
typed results and benchmarks full-file load plus in-memory execution for
DREMCOL1, Arrow IPC, Parquet Snappy, and Parquet Zstd. On Linux,
`BENCH_CPUSET=0-7` pins both servers through `taskset`.

Every completed benchmark run writes a self-contained interactive report to
`results/benchmark-report.html`, prints a clickable `file://` URL, and opens it
in the default browser when the terminal is interactive. Set `REPORT_OPEN=0`
to prevent automatic opening or `REPORT_OPEN=1` to request it explicitly. The
report can also be regenerated from existing JSON results without rerunning the
benchmarks:

```bash
python3 scripts/generate_benchmark_report.py --results-dir results --open
```

## Running an individual query

After a release build and dataset generation:

```bash
./dremel-rs/target/release/dremel-rs query \
  --data data/events.dremel --threads 4 --batch-size 4096 \
  --sql "SELECT country, COUNT(*) AS count FROM events GROUP BY country"

./dremel-cpp/build/dremel-cpp query \
  --data data/events.dremel --threads 4 --batch-size 4096 \
  --sql "SELECT country, COUNT(*) AS count FROM events GROUP BY country" \
  --explain
```

Use an interoperable file by changing `--data` in either command:

```bash
--data data/events.arrow
--data data/events-snappy.parquet
--data data/events-zstd.parquet
```

Add `--direct-parquet` when using a Parquet file to defer decoding until query
execution. The scan reads only referenced columns and skips row groups whose
official Parquet min/max/null statistics cannot satisfy supported predicates:

```bash
./dremel-rs/target/release/dremel-rs query \
  --data data/events-snappy.parquet --direct-parquet \
  --sql "SELECT SUM(bytes) FROM events WHERE event_id <= 100000" --stats
```

Use `--streaming-parquet` instead to pass projected Parquet record batches
directly into scans, aggregates, and eligible dimension joins. Inner and left
joins from `events` to the `users.user_id` or `campaigns.campaign_id` primary
key stream each fact batch through a cached dimension index. Global Top-K,
`DISTINCT`, and grouped `COUNT`, `SUM`, `MIN`, and `MAX` are merged across
batches. `--batch-size` sets the maximum decoded batch size, and `--stats`
reports the batch count, peak decoded batch bytes, and whether the query used
the materialized fallback. Other join shapes, `AVG` over joins, windows, CTEs,
subqueries, `UNION`, and `HAVING` use that fallback.

```bash
./dremel-cpp/build/dremel-cpp query \
  --data data/events-snappy.parquet --streaming-parquet --batch-size 4096 \
  --sql "SELECT country, COUNT(*) FROM events GROUP BY country" --stats
```

`users` and `campaigns` are loaded from matching Arrow or Parquet files when a
query uses those tables. The files are generated deterministically by official
PyArrow and read through the official Rust and C++ Arrow/Parquet libraries.

Use `--stats` for scan and execution counters. `--memory-limit-mb` limits the
loaded or materialized table and each decoded streaming batch, while
`--query-memory-limit-mb` places a hard cap on accounted query workspace. Hash
aggregation, joins, distinct sets, windows, intermediate relations, scan
selections, and result rows participate in the cap.
Optimized `ORDER BY ... LIMIT` queries retain only `limit + offset` rows. A
query that cannot stay inside the cap returns `RESOURCE_EXHAUSTED`; `0` keeps
the query cap disabled. `--max-result-rows` provides a separate result
cardinality limit. See
[SQL support](docs/sql-support.md) for the implemented language surface.

Add `--spill-dir` with a nonzero query memory limit to let eligible
high-cardinality hash aggregations partition row references to temporary files.
The current spill path supports grouped event queries with ordered, limited
output, including `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`. Query stats report
peak accounted memory, partitions, files, bytes written/read, and passes. Each
query uses a unique child directory and removes it after success, failure, or
cancellation.

```bash
./dremel-rs/target/release/dremel-rs query \
  --data data/events.dremel --query-memory-limit-mb 4 \
  --spill-dir /tmp/dremel-spill \
  --sql "SELECT event_id, COUNT(*) AS cnt FROM events GROUP BY event_id ORDER BY event_id DESC LIMIT 100" \
  --stats
```

Add `--stream-results` to emit typed NDJSON as rows are produced instead of
holding the complete result in memory. The bounded path supports a single
`events` scan with scalar projection, filtering, `LIMIT`, and `OFFSET`. It can
also consume official Arrow-backed Parquet record batches with
`--streaming-parquet`. Query stats report emitted rows, bytes, batches, and
peak accounted memory.

```bash
./dremel-cpp/build/dremel-cpp query \
  --data data/events.dremel --query-memory-limit-mb 1 --stream-results \
  --sql "SELECT event_id, country, score FROM events WHERE event_id <= 100000" \
  --stats > result.ndjson
```

Each output line is one JSON array containing typed scalar objects. Streaming
can expose earlier rows before a later execution or output error, so consumers
must treat a successful process exit as the completion signal.

## How the comparison works

Both engines load the same versioned binary column store and execute matching
physical algorithms. Each query is prepared before its primary timer begins;
parsing and planning are reported separately as end-to-end latency. Engine
order alternates on every iteration, and only one timed query runs at a time.

The headline comparison uses per-query median execution latency. Differences
inside `TIE_THRESHOLD_PCT` are ties, and the workload summary is the geometric
mean of the Rust/C++ ratios. Results are checked before they are counted:
unordered rows are canonicalized, ordered rows remain in order, and floating
point values use absolute and relative tolerances of `1e-9`.

This controls the workload and engine design; it does not make the compilers,
standard libraries, allocators, or language runtimes identical. A result from
one machine describes these two implementations on that machine, not either
language in general.

## Tests

```bash
cargo fmt --manifest-path dremel-rs/Cargo.toml --check
cargo test --manifest-path dremel-rs/Cargo.toml
cargo clippy --manifest-path dremel-rs/Cargo.toml --all-targets --all-features -- -D warnings

cmake -S dremel-cpp -B dremel-cpp/build -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_PREFIX_PATH="$(brew --prefix apache-arrow)" \
  -DDREMEL_CXX_STANDARD=26
cmake --build dremel-cpp/build -j
ctest --test-dir dremel-cpp/build --output-on-failure

python3 scripts/differential_test.py
python3 scripts/test_resource_limits.py
python3 scripts/test_memory_limits.py
python3 scripts/test_streaming_results.py
```

CI runs the same checks on macOS with the pinned toolchains. The `Dockerfile`
provides a Linux correctness environment; do not mix Docker measurements with
native host measurements.

The default Arrow and Parquet path remains an eager full-file control. Direct
Parquet mode materializes only projected columns and selected row groups.
Streaming Parquet keeps event scans, aggregates, and primary-key dimension
joins batch bounded while retaining the same projection and pruning rules.
Page-index pruning, streaming right/full/non-key joins, external merge sort,
distributed exchange, durable spill recovery, transactions, and database wire
protocols remain outside the current scope.

The main benchmark also runs `benchmark/memory/manifest.json` with a 256 MiB
query workspace cap. Set `MEMORY_BOUNDED=0` to skip that suite or change the
cap with `QUERY_MEMORY_LIMIT_MB`.
The spill suite uses a 4 MiB cap by default. Set `SPILL=0` to skip it or change
the cap with `SPILL_MEMORY_LIMIT_MB`. The result-streaming suite uses a 1 MiB
cap. Set `RESULT_STREAMING=0` to skip it or change the cap with
`RESULT_STREAM_MEMORY_LIMIT_MB`.

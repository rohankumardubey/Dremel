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
| Storage | DREMCOL1, Arrow IPC, and direct Apache Parquet projection with row-group pruning |
| Execution | Batched scans, selection vectors, partitioned aggregation, joins and windows |
| Optimizer | Scan filters, transitive predicates, pruning, contradiction elimination, selectivity-aware join ordering and Top-K |
| Workloads | 64 baseline, 16 hardening, 68 SQL, 12 optimizer, 9 Parquet and 5 concurrency cases |
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

`EXTENDED`, `SQL_V1`, `OPTIMIZER`, `CONCURRENCY`, `STORAGE`, and
`PARQUET_DIRECT` default to `1`.
Set any of them to `0` to skip that suite. The storage suite verifies equal
typed results and benchmarks full-file load plus in-memory execution for
DREMCOL1, Arrow IPC, Parquet Snappy, and Parquet Zstd. On Linux,
`BENCH_CPUSET=0-7` pins both servers through `taskset`.

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

`users` and `campaigns` are loaded from matching Arrow or Parquet files when a
query uses those tables. The files are generated deterministically by official
PyArrow and read through the official Rust and C++ Arrow/Parquet libraries.

Use `--stats` for scan and execution counters. `--memory-limit-mb` limits the
loaded table, while `--query-memory-limit-mb` places a hard cap on accounted
query workspace. Hash aggregation, joins, distinct sets, windows, intermediate
relations, scan selections, and result rows participate in the cap.
Optimized `ORDER BY ... LIMIT` queries retain only `limit + offset` rows. A
query that cannot stay inside the cap returns `RESOURCE_EXHAUSTED`; `0` keeps
the query cap disabled. `--max-result-rows` provides a separate result
cardinality limit. See
[SQL support](docs/sql-support.md) for the implemented language surface.

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
```

CI runs the same checks on macOS with the pinned toolchains. The `Dockerfile`
provides a Linux correctness environment; do not mix Docker measurements with
native host measurements.

The default Arrow and Parquet path remains an eager full-file control. Direct
Parquet mode performs query-time column projection and row-group pruning, then
decodes selected row groups into the existing in-memory execution operators.
Page-index pruning and a fully streaming Parquet-to-operator pipeline remain
future work. Distributed exchange, durable spill/recovery, transactions, and
database wire protocols are also outside the current scope.

The main benchmark also runs `benchmark/memory/manifest.json` with a 256 MiB
query workspace cap. Set `MEMORY_BOUNDED=0` to skip that suite or change the
cap with `QUERY_MEMORY_LIMIT_MB`.

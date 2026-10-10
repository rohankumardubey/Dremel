# Dremel

[![CI](https://github.com/rohankumardubey/Dremel/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/rohankumardubey/Dremel/actions/workflows/ci.yml)

An experimental columnar SQL engine written in Rust. It executes analytical
queries over a fixed `events`, `users`, and `campaigns` schema, with Apache
Arrow and Parquet support. A matching C++ implementation lives under
`benchmarks/` for correctness checks and performance comparisons.

The project is inspired by the [Dremel research paper](https://research.google.com/pubs/archive/36632.pdf),
but is an independent implementation. It is not Google Dremel or BigQuery,
and it is not production-ready.

## What works today

| Area | Current support |
| --- | --- |
| SQL | Joins, aggregates, CTEs, subqueries, unions, window functions, ordering, and Top-K |
| Storage | Native DREMCOL1 files, Arrow IPC, and Apache Parquet |
| Parquet scans | Projected-column reads, row-group pruning, batch streaming, and eligible dimension joins |
| Execution | Parallel batched scans, query memory limits, spillable aggregation, external sort, and streamed results |
| Planning | Predicate pushdown, projection pruning, constant folding, join selection and ordering, and runtime filters |
| Validation | Cross-engine result checks, SQLite differential tests, plan assertions, and workload benchmarks |

See [SQL support](docs/sql-support.md) for exact syntax, execution-mode
restrictions, and unsupported features.

Native DREMCOL1 events and eager Arrow IPC and Parquet reads use typed Arrow
columns. Native string columns retain dictionary encoding; eager interoperable
reads retain additional fields for all three built-in tables. Use `--table NAME`
to query flat Arrow or Parquet files with your own table and field names;
the binder derives their types from the schema.

## Quick start

You need Rust with `rustup` and Python 3.11 or newer. The repository pins Rust
1.97.1; Cargo uses that toolchain through `rust-toolchain.toml`.

```bash
cargo build --release
python3 benchmarks/scripts/generate_data.py --rows 20000
python3 benchmarks/scripts/build_column_store.py
./target/release/dremel query \
  --data data/events.dremel \
  --sql "SELECT country, COUNT(*) AS events FROM events GROUP BY country ORDER BY events DESC"
```

The CLI prints one typed JSON array per result row. Add `--explain` to inspect
the physical plan, or `--stats` for scan and execution counters.

## Querying Parquet

The native sample above needs no Python packages. To generate Arrow IPC and
Parquet versions of the same data, install the pinned PyArrow dependency:

```bash
python3 -m venv .venv
.venv/bin/pip install -r benchmarks/requirements.txt
.venv/bin/python benchmarks/scripts/build_interoperable_data.py
```

The default Parquet path loads the file before execution. Use
`--direct-parquet` to decode selected columns and row groups per query, or
`--streaming-parquet` to pass projected batches into supported operators:

```bash
./target/release/dremel query \
  --data data/events-snappy.parquet --streaming-parquet \
  --batch-size 4096 \
  --sql "SELECT country, COUNT(*) FROM events GROUP BY country" \
  --stats
```

The two flags are mutually exclusive. Some query shapes fall back to
materialization in streaming mode; `--stats` reports when that happens. See
[Parquet execution](docs/sql-support.md#parquet-execution) for the supported
streaming shapes and pruning rules.

## Query your own data

Named-table queries support flat primitive columns, nullable values, scalar
expressions, aggregates, grouping, HAVING, DISTINCT, ordering, and limits:

```bash
./target/release/dremel query \
  --data measurements.parquet --table measurements \
  --sql "SELECT region, COUNT(*) AS n FROM measurements GROUP BY region ORDER BY region"
```

Replace the file and field names with those in your dataset. Named-table
execution currently loads the file eagerly and runs on one thread. Joins,
nested fields, and streaming scans require further work for this mode. See
[named-table SQL](docs/sql-support.md#named-table-sql) for supported types and
the Rust API.

## Scan nested Parquet

Read nested STRUCT/LIST data without flattening records or decoding unrelated
leaf columns:

```bash
./target/release/dremel scan --data records.parquet \
  --columns profile.city,items.price --batch-size 4096 \
  --output selected.arrow
```

The output retains parent nulls, null lists, empty lists, and null elements.
Omit `--output` to scan without exporting data; the command prints a JSON scan
summary. See [nested Parquet scans](docs/nested-parquet.md) for the Rust API,
row-group selection, metrics, and benchmarks. This is a storage scan, not
nested SQL or UNNEST.

## Memory and result streaming

`--query-memory-limit-mb` caps accounted query workspace, while
`--memory-limit-mb` separately caps the loaded table or decoded Parquet batch.
Eligible grouped queries can spill to disk when a spill directory is provided.
Result streaming emits typed NDJSON without retaining the full result:

```bash
./target/release/dremel query \
  --data data/events.dremel \
  --query-memory-limit-mb 2 \
  --spill-dir /tmp/dremel-sort \
  --stream-results \
  --sql "SELECT event_id, score FROM events ORDER BY score DESC, event_id ASC" \
  > sorted.ndjson
```

This ordered query uses external merge sort. Streaming output is complete only
when the process exits successfully. Memory caps are operator-accounting
limits, not process RSS limits; unsupported or over-budget queries return an
error. Details and eligibility rules are in
[Memory-bounded execution](docs/sql-support.md#memory-bounded-execution).

## Benchmarks

The benchmark suite runs the Rust engine against the C++ reference on the
same generated data and validates results before comparing latency. It records
raw samples, machine and toolchain metadata, and a self-contained interactive
HTML report at `results/benchmark-report.html`. The runner prints the report
location after each completed run.

The comparison requires CMake, an LLVM Clang toolchain with C++26 support,
Apache Arrow C++ and Parquet, and PyArrow 25.0.1. On macOS:

```bash
brew install llvm cmake apache-arrow
python3 -m venv .venv
.venv/bin/pip install -r benchmarks/requirements.txt
DATASET_ROWS=20000 WARMUP=1 ITERATIONS=3 ./benchmark.sh
```

The runner requires Rust 1.97.1 and targets Clang 22.1.8 in C++26 mode. It
prints the actual compiler and standard library versions used for each run.

The default run uses one million events, three warmups, and twenty measured
iterations:

```bash
./benchmark.sh
```

Common settings are `DATASET_ROWS`, `BENCH_THREADS`, `BATCH_SIZE`, `WARMUP`,
`ITERATIONS`, `LTO`, and `NATIVE`. Set `REPORT_OPEN=0` to suppress automatic
browser opening. Existing results can be rendered again without rerunning the
engines:

```bash
python3 benchmarks/scripts/generate_benchmark_report.py --results-dir results --open
```

Results from one machine describe these implementations on that machine, not
Rust and C++ in general. Parsing and planning are tracked separately from the
primary execution timer. The summary uses per-query median latency and a
geometric mean of Rust/C++ ratios; query order alternates between engines.
For a meaningful comparison, run both on the same controlled host rather than
comparing measurements from unrelated CI machines.
`benchmarks/Dockerfile` provides a Linux correctness environment, but Docker
measurements should not be mixed with native macOS measurements.

## Development

Run the Rust checks locally:

```bash
cargo fmt --check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

CI runs these checks for pull requests and pushes to `main`. The cross-engine
smoke benchmark is available through a manual GitHub Actions run. The C++
reference can also be built and tested directly:

```bash
cmake -S benchmarks/cpp -B benchmarks/cpp/build -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_COMPILER="$(brew --prefix llvm)/bin/clang++" \
  -DCMAKE_PREFIX_PATH="$(brew --prefix apache-arrow)" \
  -DDREMEL_CXX_STANDARD=26
cmake --build benchmarks/cpp/build -j
ctest --test-dir benchmarks/cpp/build --output-on-failure
```

The reference CLI is `benchmarks/cpp/build/dremel-cpp`; it accepts the same
`query --data ... --sql ...` shape as the Rust CLI.

## Project status

This is a single-node research engine with flat named-table SQL. It does not
yet implement the paper's multi-level serving tree or generic nested-field SQL
execution.
The Rust storage boundary can read arbitrary Arrow IPC and Parquet schemas
through `ColumnarTable`. Eager Arrow and Parquet queries and native DREMCOL1
queries use typed columnar storage. Named-table queries derive flat column
types from the Arrow schema. Built-in workloads still use specialized
operators; a catalog for multiple user-defined tables and generic nested
execution are pending. Nested Parquet scans retain typed containers and support
leaf projection, but are not yet connected to SQL expression evaluation.
There is a repetition/definition-level round-trip example, but it is not
connected to the query engine. Distributed storage and exchange, fault
tolerance, transactions, database wire protocols, and durable spill recovery
are also outside the current scope.

The next architectural steps are generic nested Parquet scans, partitioned
partial aggregation, and a coordinator-to-leaf execution protocol. Each step
needs correctness and resource testing before a production-readiness claim
would be justified.

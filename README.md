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

This is a single-node, fixed-schema research engine. It does not yet implement
the paper's multi-level serving tree or generic nested-field SQL execution.
There is a repetition/definition-level round-trip example, but it is not
connected to the query engine. Distributed storage and exchange, fault
tolerance, transactions, database wire protocols, and durable spill recovery
are also outside the current scope.

The next architectural steps are generic nested Parquet scans, partitioned
partial aggregation, and a coordinator-to-leaf execution protocol. Each step
needs correctness and resource testing before a production-readiness claim
would be justified.

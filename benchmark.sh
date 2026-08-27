#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

DATASET_ROWS="${DATASET_ROWS:-1000000}"
DATASET_SEED="${DATASET_SEED:-0xD3E3A5E1}"
BENCH_THREADS="${BENCH_THREADS:-4}"
BATCH_SIZE="${BATCH_SIZE:-4096}"
WARMUP="${WARMUP:-3}"
ITERATIONS="${ITERATIONS:-20}"
TIE_THRESHOLD_PCT="${TIE_THRESHOLD_PCT:-1.0}"
LTO="${LTO:-0}"
NATIVE="${NATIVE:-0}"
BENCH_CPUSET="${BENCH_CPUSET:-}"
CPP_STANDARD="${CPP_STANDARD:-26}"
EXTENDED="${EXTENDED:-1}"
SQL_V1="${SQL_V1:-1}"
OPTIMIZER="${OPTIMIZER:-1}"
CONCURRENCY="${CONCURRENCY:-1}"
STORAGE="${STORAGE:-1}"
MEMORY_BOUNDED="${MEMORY_BOUNDED:-1}"
PARQUET="${PARQUET:-${PARQUET_DIRECT:-1}}"
SPILL="${SPILL:-1}"
RESULT_STREAMING="${RESULT_STREAMING:-1}"
QUERY_MEMORY_LIMIT_MB="${QUERY_MEMORY_LIMIT_MB:-256}"
SPILL_MEMORY_LIMIT_MB="${SPILL_MEMORY_LIMIT_MB:-4}"
RESULT_STREAM_MEMORY_LIMIT_MB="${RESULT_STREAM_MEMORY_LIMIT_MB:-1}"
REPORT_OPEN="${REPORT_OPEN:-auto}"
export LTO NATIVE BENCH_CPUSET CPP_STANDARD

if [[ "$REPORT_OPEN" != auto && "$REPORT_OPEN" != 0 && "$REPORT_OPEN" != 1 ]]; then
  echo "ERROR: REPORT_OPEN must be auto, 0, or 1" >&2
  exit 1
fi

if [[ -n "${PYTHON_BIN:-}" ]]; then
  PYTHON="$PYTHON_BIN"
elif [[ -x .venv/bin/python ]]; then
  PYTHON=".venv/bin/python"
else
  python3 -m venv .venv
  PYTHON=".venv/bin/python"
fi
if ! "$PYTHON" -c 'import pyarrow; assert pyarrow.__version__ == "25.0.1"' 2>/dev/null; then
  "$PYTHON" -m pip install -r requirements.txt
fi

mkdir -p results
"$PYTHON" scripts/create_workload.py
"$PYTHON" scripts/create_extended_workload.py
"$PYTHON" scripts/create_sql_v1_workload.py
"$PYTHON" scripts/create_optimizer_workload.py
"$PYTHON" scripts/create_parquet_workload.py
"$PYTHON" scripts/create_spill_workload.py
"$PYTHON" scripts/create_streaming_workload.py
"$PYTHON" scripts/create_concurrency_workload.py
"$PYTHON" scripts/generate_data.py --rows "$DATASET_ROWS" --seed "$DATASET_SEED"
"$PYTHON" scripts/build_column_store.py
"$PYTHON" scripts/build_interoperable_data.py

echo "Rust toolchain: $(rustc --version)"
echo "Cargo: $(cargo --version)"
if [[ "$(rustc --version)" != rustc\ 1.97.1* ]]; then
  echo "ERROR: Rust 1.97.1 is required (rust-toolchain.toml should select it)." >&2
  exit 1
fi

if [[ -x /opt/homebrew/opt/llvm/bin/clang++ ]]; then
  CXX_BIN="/opt/homebrew/opt/llvm/bin/clang++"
elif command -v clang++-22 >/dev/null 2>&1; then
  CXX_BIN="clang++-22"
elif command -v g++-16 >/dev/null 2>&1; then
  CXX_BIN="g++-16"
elif command -v clang++ >/dev/null 2>&1; then
  CXX_BIN="clang++"
else
  echo "ERROR: no C++${CPP_STANDARD} compiler found" >&2
  exit 1
fi
export CXX="$CXX_BIN"
export CXX_VERSION="$($CXX_BIN --version | head -n 1)"
export CXX_STDLIB_VERSION="$(printf '#include <version>\n' | "$CXX_BIN" -std="c++${CPP_STANDARD}" -dM -E -x c++ - | awk '$2 == "_LIBCPP_VERSION" { print $3 }')"
echo "C++ compiler: $CXX_VERSION"
if [[ "$CXX_VERSION" != *"clang version 22.1.8"* ]]; then
  echo "WARNING: LLVM Clang 22.1.8 unavailable; using $CXX_VERSION"
fi
echo "C++ standard: C++${CPP_STANDARD}"
echo "C++ standard library: libc++ ${CXX_STDLIB_VERSION:-unknown}"
echo "LTO: $([[ "$LTO" == 1 ]] && echo enabled || echo disabled)"
echo "Native tuning: $([[ "$NATIVE" == 1 ]] && echo enabled || echo disabled)"
if [[ -n "$BENCH_CPUSET" && "$(uname -s)" == Linux && -x "$(command -v taskset || true)" ]]; then
  echo "CPU affinity: taskset -c $BENCH_CPUSET (applied equally to both servers)"
else
  echo "CPU affinity: unsupported / disabled"
fi

RUSTFLAGS_VALUE=""
CXX_FLAGS=""
if [[ "$NATIVE" == 1 ]]; then
  RUSTFLAGS_VALUE="-C target-cpu=native"
  CXX_FLAGS="-march=native"
fi
if [[ "$LTO" == 1 ]]; then
  export CARGO_PROFILE_RELEASE_LTO=true
  CXX_LTO=ON
else
  unset CARGO_PROFILE_RELEASE_LTO || true
  CXX_LTO=OFF
fi

RUSTFLAGS="$RUSTFLAGS_VALUE" cargo build --manifest-path dremel-rs/Cargo.toml --release
cargo fmt --manifest-path dremel-rs/Cargo.toml --check
cargo test --manifest-path dremel-rs/Cargo.toml
cargo clippy --manifest-path dremel-rs/Cargo.toml --all-targets --all-features -- -D warnings

CMAKE_PREFIX_ARGS=()
if command -v brew >/dev/null 2>&1 && brew --prefix apache-arrow >/dev/null 2>&1; then
  CMAKE_PREFIX_ARGS+=("-DCMAKE_PREFIX_PATH=$(brew --prefix apache-arrow)")
fi
cmake --fresh -S dremel-cpp -B dremel-cpp/build -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_COMPILER="$CXX_BIN" -DCMAKE_CXX_FLAGS_RELEASE="-O3 -DNDEBUG $CXX_FLAGS" \
  -DCMAKE_INTERPROCEDURAL_OPTIMIZATION="$CXX_LTO" -DDREMEL_CXX_STANDARD="$CPP_STANDARD" \
  "${CMAKE_PREFIX_ARGS[@]}"
cmake --build dremel-cpp/build -j
ctest --test-dir dremel-cpp/build --output-on-failure
"$PYTHON" scripts/differential_test.py
"$PYTHON" scripts/test_resource_limits.py
"$PYTHON" scripts/test_memory_limits.py
"$PYTHON" scripts/test_streaming_results.py

"$PYTHON" scripts/run_benchmark.py --data data/events.dremel --threads "$BENCH_THREADS" --batch-size "$BATCH_SIZE" \
  --warmup "$WARMUP" --iterations "$ITERATIONS" --tie-threshold "$TIE_THRESHOLD_PCT"

if [[ "$EXTENDED" == 1 ]]; then
  "$PYTHON" scripts/run_benchmark.py --data data/events.dremel --threads "$BENCH_THREADS" --batch-size "$BATCH_SIZE" \
    --warmup "$WARMUP" --iterations "$ITERATIONS" --tie-threshold "$TIE_THRESHOLD_PCT" \
    --manifest benchmark/extended/manifest.json --results-dir results/extended
fi

if [[ "$SQL_V1" == 1 ]]; then
  "$PYTHON" scripts/run_benchmark.py --data data/events.dremel --threads "$BENCH_THREADS" --batch-size "$BATCH_SIZE" \
    --warmup "$WARMUP" --iterations "$ITERATIONS" --tie-threshold "$TIE_THRESHOLD_PCT" \
    --manifest benchmark/sql-v1/manifest.json --results-dir results/sql-v1
fi

if [[ "$OPTIMIZER" == 1 ]]; then
  "$PYTHON" scripts/run_optimizer_benchmark.py --data data/events.dremel --threads "$BENCH_THREADS" \
    --batch-size "$BATCH_SIZE" --warmup "$WARMUP" --iterations "$ITERATIONS"
fi

if [[ "$CONCURRENCY" == 1 ]]; then
  "$PYTHON" scripts/run_concurrency_benchmark.py --data data/events.dremel --threads "$BENCH_THREADS" \
    --batch-size "$BATCH_SIZE"
fi

if [[ "$STORAGE" == 1 ]]; then
  for storage_case in \
    "dremcol1:data/events.dremel" \
    "arrow-ipc:data/events.arrow" \
    "parquet-snappy:data/events-snappy.parquet" \
    "parquet-zstd:data/events-zstd.parquet"; do
    storage_name="${storage_case%%:*}"
    storage_file="${storage_case#*:}"
    "$PYTHON" scripts/run_benchmark.py --data "$storage_file" \
      --threads "$BENCH_THREADS" --batch-size "$BATCH_SIZE" \
      --warmup "$WARMUP" --iterations "$ITERATIONS" \
      --tie-threshold "$TIE_THRESHOLD_PCT" \
      --manifest benchmark/storage/manifest.json \
      --results-dir "results/storage/$storage_name"
  done
  "$PYTHON" scripts/summarize_storage_benchmark.py
fi

if [[ "$PARQUET" == 1 ]]; then
  "$PYTHON" scripts/run_parquet_benchmark.py \
    --data data/events-snappy.parquet --threads "$BENCH_THREADS" \
    --batch-size "$BATCH_SIZE" --warmup "$WARMUP" \
    --iterations "$ITERATIONS"
fi

if [[ "$MEMORY_BOUNDED" == 1 ]]; then
  "$PYTHON" scripts/run_benchmark.py --data data/events.dremel \
    --threads "$BENCH_THREADS" --batch-size "$BATCH_SIZE" \
    --warmup "$WARMUP" --iterations "$ITERATIONS" \
    --tie-threshold "$TIE_THRESHOLD_PCT" \
    --query-memory-limit-mb "$QUERY_MEMORY_LIMIT_MB" \
    --manifest benchmark/memory/manifest.json \
    --results-dir results/memory
fi

if [[ "$SPILL" == 1 ]]; then
  "$PYTHON" scripts/run_spill_benchmark.py --data data/events.dremel \
    --threads "$BENCH_THREADS" --batch-size "$BATCH_SIZE" \
    --memory-limit-mb "$SPILL_MEMORY_LIMIT_MB" --warmup "$WARMUP" \
    --iterations "$ITERATIONS" --tie-threshold "$TIE_THRESHOLD_PCT"
fi

if [[ "$RESULT_STREAMING" == 1 ]]; then
  "$PYTHON" scripts/run_streaming_benchmark.py --data data/events.dremel \
    --batch-size "$BATCH_SIZE" --memory-limit-mb "$RESULT_STREAM_MEMORY_LIMIT_MB" \
    --warmup "$WARMUP" --iterations "$ITERATIONS" \
    --tie-threshold "$TIE_THRESHOLD_PCT"
fi

REPORT_ARGS=(--results-dir results --include baseline)
if [[ "$EXTENDED" == 1 ]]; then REPORT_ARGS+=(--include extended); fi
if [[ "$SQL_V1" == 1 ]]; then REPORT_ARGS+=(--include sql); fi
if [[ "$OPTIMIZER" == 1 ]]; then REPORT_ARGS+=(--include optimizer); fi
if [[ "$CONCURRENCY" == 1 ]]; then REPORT_ARGS+=(--include concurrency); fi
if [[ "$STORAGE" == 1 ]]; then REPORT_ARGS+=(--include storage); fi
if [[ "$PARQUET" == 1 ]]; then REPORT_ARGS+=(--include parquet); fi
if [[ "$MEMORY_BOUNDED" == 1 ]]; then REPORT_ARGS+=(--include memory); fi
if [[ "$SPILL" == 1 ]]; then REPORT_ARGS+=(--include spill); fi
if [[ "$RESULT_STREAMING" == 1 ]]; then REPORT_ARGS+=(--include streaming); fi

if [[ "$REPORT_OPEN" == 1 || ( "$REPORT_OPEN" == auto && -t 1 ) ]]; then
  REPORT_ARGS+=(--open)
fi

REPORT_URI="$("$PYTHON" scripts/generate_benchmark_report.py "${REPORT_ARGS[@]}")"
echo
echo "Interactive benchmark report:"
echo "$REPORT_URI"
if [[ -t 1 ]]; then
  printf '\033]8;;%s\033\\Open interactive benchmark report\033]8;;\033\\\n' "$REPORT_URI"
fi

#!/usr/bin/env python3
"""Benchmark official streaming and materialized Parquet execution."""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path

from run_benchmark import ROOT, Server, compare_rows, result_hash, stats


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rust", default=str(ROOT / "target/release/dremel")
    )
    parser.add_argument("--cpp", default=str(ROOT / "benchmarks/cpp/build/dremel-cpp"))
    parser.add_argument(
        "--data", default=str(ROOT / "data/events-snappy.parquet")
    )
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=4096)
    parser.add_argument("--warmup", type=int, default=int(os.getenv("WARMUP", "3")))
    parser.add_argument(
        "--iterations", type=int, default=int(os.getenv("ITERATIONS", "20"))
    )
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument(
        "--manifest", type=Path, default=ROOT / "benchmarks/workloads/parquet/manifest.json"
    )
    parser.add_argument(
        "--results-dir", type=Path, default=ROOT / "results/parquet"
    )
    args = parser.parse_args()
    if args.warmup < 0 or args.iterations < 1:
        parser.error("warmup cannot be negative and iterations must be positive")
    manifest = json.loads(args.manifest.read_text())
    tail = [
        "bench-server",
        "--data",
        args.data,
        "--threads",
        str(args.threads),
        "--batch-size",
        str(args.batch_size),
    ]
    servers: list[Server] = []
    try:
        rust_streaming = Server(
            "Rust streaming", [args.rust, *tail, "--streaming-parquet"]
        )
        cpp_streaming = Server(
            "C++ streaming", [args.cpp, *tail, "--streaming-parquet"]
        )
        rust_materialized = Server(
            "Rust materialized", [args.rust, *tail, "--direct-parquet"]
        )
        cpp_materialized = Server(
            "C++ materialized", [args.cpp, *tail, "--direct-parquet"]
        )
        rust_eager = Server("Rust eager", [args.rust, *tail])
        cpp_eager = Server("C++ eager", [args.cpp, *tail])
        servers = [
            rust_streaming,
            cpp_streaming,
            rust_materialized,
            cpp_materialized,
            rust_eager,
            cpp_eager,
        ]
        startup_rss_kib = {
            "rust_streaming": rust_streaming.rss_kib(),
            "cpp_streaming": cpp_streaming.rss_kib(),
            "rust_materialized": rust_materialized.rss_kib(),
            "cpp_materialized": cpp_materialized.rss_kib(),
            "rust_eager": rust_eager.rss_kib(),
            "cpp_eager": cpp_eager.rss_kib(),
        }
        peak_rss_kib = dict(startup_rss_kib)
        output = []
        for item in manifest:
            query_id = item["query_id"]
            sql = (
                (args.manifest.parent / item["file"])
                .read_text()
                .strip()
                .replace("\n", " ")
            )
            for server in servers:
                server.prepare(query_id, sql)
            plans = {
                "rust_streaming": rust_streaming.explain(query_id),
                "cpp_streaming": cpp_streaming.explain(query_id),
                "rust_materialized": rust_materialized.explain(query_id),
                "cpp_materialized": cpp_materialized.explain(query_id),
            }
            if plans["rust_streaming"] != plans["cpp_streaming"]:
                raise RuntimeError(f"{query_id}: streaming plans differ: {plans}")
            if plans["rust_materialized"] != plans["cpp_materialized"]:
                raise RuntimeError(f"{query_id}: materialized plans differ: {plans}")
            operators = [line.split("(", 1)[0] for line in plans["rust_streaming"]]
            if "ParquetScanExec" not in operators or "ParquetStreamExec" not in operators:
                raise RuntimeError(f"{query_id}: missing streaming Parquet operators")
            results = {}
            scans = {}
            for label, server in (
                ("rust_streaming", rust_streaming),
                ("cpp_streaming", cpp_streaming),
                ("rust_materialized", rust_materialized),
                ("cpp_materialized", cpp_materialized),
                ("rust_eager", rust_eager),
                ("cpp_eager", cpp_eager),
            ):
                _, results[label] = server.execute(query_id, True)
                scans[label] = dict(server.last_scan_metrics)
                peak_rss_kib[label] = max(peak_rss_kib[label], server.rss_kib())
            ordered = "ORDER BY" in sql.upper()
            reference = results["rust_eager"]
            for label, rows in results.items():
                correct, detail = compare_rows(reference, rows, ordered)
                if not correct:
                    raise RuntimeError(f"{query_id}: {label} differs: {detail}")
            comparable = (
                "total_rows",
                "rows_read",
                "total_row_groups",
                "row_groups_read",
                "total_columns",
                "columns_read",
                "compressed_bytes_read",
            )
            for left, right in (
                ("rust_streaming", "cpp_streaming"),
                ("rust_materialized", "cpp_materialized"),
                ("rust_streaming", "rust_materialized"),
            ):
                if any(scans[left][key] != scans[right][key] for key in comparable):
                    raise RuntimeError(f"{query_id}: scan metrics differ: {scans}")
            scan = scans["rust_streaming"]
            materialized_scan = scans["rust_materialized"]
            if any(
                scans[engine]["streaming_fallback"]
                != item["expected_streaming_fallback"]
                for engine in ("rust_streaming", "cpp_streaming")
            ):
                raise RuntimeError(f"{query_id}: unexpected streaming fallback: {scan}")
            for left, right in (
                ("rust_streaming", "cpp_streaming"),
                ("rust_materialized", "cpp_materialized"),
            ):
                if scans[left]["batches_read"] != scans[right]["batches_read"]:
                    raise RuntimeError(f"{query_id}: Parquet batch counts differ: {scans}")
            for language in ("rust", "cpp"):
                streamed = scans[f"{language}_streaming"]
                materialized = scans[f"{language}_materialized"]
                if (
                    not streamed["streaming_fallback"]
                    and streamed["batches_read"] > 1
                    and streamed["peak_decoded_batch_bytes"]
                    >= materialized["peak_decoded_batch_bytes"]
                ):
                    raise RuntimeError(
                        f"{query_id}: {language} streaming did not bound decoded memory: {scans}"
                    )
            if scan["columns_read"] != item["expected_columns_read"]:
                raise RuntimeError(
                    f"{query_id}: expected {item['expected_columns_read']} columns, got {scan}"
                )
            if scan["row_groups_read"] > item["maximum_row_groups_read"]:
                raise RuntimeError(f"{query_id}: row-group pruning failed: {scan}")
            record = {
                "query_id": query_id,
                "category": item["category"],
                "correct": True,
                "result_hash": result_hash(reference, ordered),
                "row_count": len(reference),
                "scan": scan,
                "materialized_scan": materialized_scan,
                "plans": plans,
            }
            if not args.validate_only:
                timing = {}
                for mode, rust_server, cpp_server in (
                    ("streaming", rust_streaming, cpp_streaming),
                    ("materialized", rust_materialized, cpp_materialized),
                    ("eager", rust_eager, cpp_eager),
                ):
                    for iteration in range(args.warmup):
                        order = (
                            (cpp_server, rust_server)
                            if iteration % 2 == 0
                            else (rust_server, cpp_server)
                        )
                        for server in order:
                            server.execute(query_id)
                    samples = {"rust": [], "cpp": []}
                    for iteration in range(args.iterations):
                        order = (
                            (("cpp", cpp_server), ("rust", rust_server))
                            if iteration % 2 == 0
                            else (("rust", rust_server), ("cpp", cpp_server))
                        )
                        for language, server in order:
                            samples[language].append(server.execute(query_id)[0])
                    timing[f"rust_{mode}"] = stats(samples["rust"])
                    timing[f"cpp_{mode}"] = stats(samples["cpp"])
                    peak_rss_kib[f"rust_{mode}"] = max(
                        peak_rss_kib[f"rust_{mode}"], rust_server.rss_kib()
                    )
                    peak_rss_kib[f"cpp_{mode}"] = max(
                        peak_rss_kib[f"cpp_{mode}"], cpp_server.rss_kib()
                    )
                record["timing"] = timing
            output.append(record)
            print(
                f"{query_id} MATCH columns={scan['columns_read']}/{scan['total_columns']} "
                f"row_groups={scan['row_groups_read']}/{scan['total_row_groups']} "
                f"batches={scan['batches_read']} fallback={scan['streaming_fallback']}",
                flush=True,
            )
        document = {
            "warmup": args.warmup,
            "iterations": args.iterations,
            "validate_only": args.validate_only,
            "startup_load_ns": {
                "rust_streaming": rust_streaming.load_ns,
                "cpp_streaming": cpp_streaming.load_ns,
                "rust_materialized": rust_materialized.load_ns,
                "cpp_materialized": cpp_materialized.load_ns,
                "rust_eager": rust_eager.load_ns,
                "cpp_eager": cpp_eager.load_ns,
            },
            "startup_rss_kib": startup_rss_kib,
            "peak_rss_kib_observed": peak_rss_kib,
            "queries": output,
        }
        out = args.results_dir.resolve()
        out.mkdir(parents=True, exist_ok=True)
        (out / "parquet.json").write_text(json.dumps(document, indent=2) + "\n")
        flat = []
        for row in output:
            timing = row.get("timing", {})
            flat.append(
                {
                    "query_id": row["query_id"],
                    "category": row["category"],
                    "correct": row["correct"],
                    "columns_read": row["scan"]["columns_read"],
                    "total_columns": row["scan"]["total_columns"],
                    "row_groups_read": row["scan"]["row_groups_read"],
                    "total_row_groups": row["scan"]["total_row_groups"],
                    "rows_read": row["scan"]["rows_read"],
                    "total_rows": row["scan"]["total_rows"],
                    "compressed_bytes_read": row["scan"]["compressed_bytes_read"],
                    "batches_read": row["scan"]["batches_read"],
                    "peak_decoded_batch_bytes": row["scan"][
                        "peak_decoded_batch_bytes"
                    ],
                    "streaming_fallback": row["scan"]["streaming_fallback"],
                    "rust_streaming_median_ms": timing.get("rust_streaming", {}).get(
                        "median_ns", 0
                    )
                    / 1e6,
                    "cpp_streaming_median_ms": timing.get("cpp_streaming", {}).get(
                        "median_ns", 0
                    )
                    / 1e6,
                    "rust_materialized_median_ms": timing.get(
                        "rust_materialized", {}
                    ).get("median_ns", 0)
                    / 1e6,
                    "cpp_materialized_median_ms": timing.get(
                        "cpp_materialized", {}
                    ).get(
                        "median_ns", 0
                    )
                    / 1e6,
                    "rust_eager_in_memory_median_ms": timing.get(
                        "rust_eager", {}
                    ).get("median_ns", 0)
                    / 1e6,
                    "cpp_eager_in_memory_median_ms": timing.get(
                        "cpp_eager", {}
                    ).get("median_ns", 0)
                    / 1e6,
                }
            )
        with (out / "parquet.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=flat[0].keys())
            writer.writeheader()
            writer.writerows(flat)
        report = [
            "STREAMING PARQUET BENCHMARK",
            f"Queries: {len(output)} / {len(output)} correct",
            "Streaming and materialized latency include Parquet metadata, selected-column decoding, and execution.",
            "Eager latency is the in-memory control after its one-time full-file startup load.",
            f"Startup load ms: Rust streaming {rust_streaming.load_ns / 1e6:.3f}, materialized {rust_materialized.load_ns / 1e6:.3f}, eager {rust_eager.load_ns / 1e6:.3f}; "
            f"C++ streaming {cpp_streaming.load_ns / 1e6:.3f}, materialized {cpp_materialized.load_ns / 1e6:.3f}, eager {cpp_eager.load_ns / 1e6:.3f}",
            f"Observed peak RSS KiB: Rust streaming {peak_rss_kib['rust_streaming']}, materialized {peak_rss_kib['rust_materialized']}, eager {peak_rss_kib['rust_eager']}; "
            f"C++ streaming {peak_rss_kib['cpp_streaming']}, materialized {peak_rss_kib['cpp_materialized']}, eager {peak_rss_kib['cpp_eager']}",
        ]
        for row in flat:
            report.append(
                f"{row['query_id']} {row['category']}: columns {row['columns_read']}/{row['total_columns']}, "
                f"row groups {row['row_groups_read']}/{row['total_row_groups']}, "
                f"rows {row['rows_read']}/{row['total_rows']}, "
                f"compressed bytes {row['compressed_bytes_read']}; "
                f"batches {row['batches_read']}, peak batch bytes {row['peak_decoded_batch_bytes']}, "
                f"fallback {row['streaming_fallback']}; "
                f"Rust streaming {row['rust_streaming_median_ms']:.3f} ms "
                f"(materialized {row['rust_materialized_median_ms']:.3f} ms, eager in-memory {row['rust_eager_in_memory_median_ms']:.3f} ms), "
                f"C++ streaming {row['cpp_streaming_median_ms']:.3f} ms "
                f"(materialized {row['cpp_materialized_median_ms']:.3f} ms, eager in-memory {row['cpp_eager_in_memory_median_ms']:.3f} ms)"
            )
        (out / "report.txt").write_text("\n".join(report) + "\n")
        print(f"Streaming Parquet correctness: {len(output)} / {len(output)} PASS")
        return 0
    finally:
        for server in reversed(servers):
            server.close()


if __name__ == "__main__":
    raise SystemExit(main())

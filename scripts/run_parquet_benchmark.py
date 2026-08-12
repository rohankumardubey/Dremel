#!/usr/bin/env python3
"""Benchmark official direct Parquet scans in the Rust and C++ engines."""

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
        "--rust", default=str(ROOT / "dremel-rs/target/release/dremel-rs")
    )
    parser.add_argument("--cpp", default=str(ROOT / "dremel-cpp/build/dremel-cpp"))
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
        "--manifest", type=Path, default=ROOT / "benchmark/parquet/manifest.json"
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
        rust_direct = Server("Rust direct", [args.rust, *tail, "--direct-parquet"])
        cpp_direct = Server("C++ direct", [args.cpp, *tail, "--direct-parquet"])
        rust_eager = Server("Rust eager", [args.rust, *tail])
        cpp_eager = Server("C++ eager", [args.cpp, *tail])
        servers = [rust_direct, cpp_direct, rust_eager, cpp_eager]
        startup_rss_kib = {
            "rust_direct": rust_direct.rss_kib(),
            "cpp_direct": cpp_direct.rss_kib(),
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
                "rust_direct": rust_direct.explain(query_id),
                "cpp_direct": cpp_direct.explain(query_id),
            }
            if plans["rust_direct"] != plans["cpp_direct"]:
                raise RuntimeError(f"{query_id}: direct plans differ: {plans}")
            if "ParquetScanExec" not in [line.split("(", 1)[0] for line in plans["rust_direct"]]:
                raise RuntimeError(f"{query_id}: missing ParquetScanExec")
            results = {}
            scans = {}
            for label, server in (
                ("rust_direct", rust_direct),
                ("cpp_direct", cpp_direct),
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
            if scans["rust_direct"] != scans["cpp_direct"]:
                raise RuntimeError(f"{query_id}: direct scan metrics differ: {scans}")
            scan = scans["rust_direct"]
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
                "plans": plans,
            }
            if not args.validate_only:
                timing = {}
                for mode, rust_server, cpp_server in (
                    ("direct", rust_direct, cpp_direct),
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
                f"row_groups={scan['row_groups_read']}/{scan['total_row_groups']}",
                flush=True,
            )
        document = {
            "warmup": args.warmup,
            "iterations": args.iterations,
            "validate_only": args.validate_only,
            "startup_load_ns": {
                "rust_direct": rust_direct.load_ns,
                "cpp_direct": cpp_direct.load_ns,
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
                    "rust_direct_median_ms": timing.get("rust_direct", {}).get(
                        "median_ns", 0
                    )
                    / 1e6,
                    "cpp_direct_median_ms": timing.get("cpp_direct", {}).get(
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
            "DIRECT PARQUET BENCHMARK",
            f"Queries: {len(output)} / {len(output)} correct",
            "Direct latency includes Parquet metadata, selected-column decoding, and execution.",
            "Eager latency is the in-memory control after its one-time full-file startup load.",
            f"Startup load ms: Rust direct {rust_direct.load_ns / 1e6:.3f}, eager {rust_eager.load_ns / 1e6:.3f}; "
            f"C++ direct {cpp_direct.load_ns / 1e6:.3f}, eager {cpp_eager.load_ns / 1e6:.3f}",
            f"Observed peak RSS KiB: Rust direct {peak_rss_kib['rust_direct']}, eager {peak_rss_kib['rust_eager']}; "
            f"C++ direct {peak_rss_kib['cpp_direct']}, eager {peak_rss_kib['cpp_eager']}",
        ]
        for row in flat:
            report.append(
                f"{row['query_id']} {row['category']}: columns {row['columns_read']}/{row['total_columns']}, "
                f"row groups {row['row_groups_read']}/{row['total_row_groups']}, "
                f"rows {row['rows_read']}/{row['total_rows']}, "
                f"compressed bytes {row['compressed_bytes_read']}; "
                f"Rust direct {row['rust_direct_median_ms']:.3f} ms "
                f"(eager in-memory {row['rust_eager_in_memory_median_ms']:.3f} ms), "
                f"C++ direct {row['cpp_direct_median_ms']:.3f} ms "
                f"(eager in-memory {row['cpp_eager_in_memory_median_ms']:.3f} ms)"
            )
        (out / "report.txt").write_text("\n".join(report) + "\n")
        print(f"Direct Parquet correctness: {len(output)} / {len(output)} PASS")
        return 0
    finally:
        for server in reversed(servers):
            server.close()


if __name__ == "__main__":
    raise SystemExit(main())

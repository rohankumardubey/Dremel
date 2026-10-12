#!/usr/bin/env python3
"""Correlated list correctness and prepared Rust versus pre-expanded controls."""
from __future__ import annotations

import argparse
import json
import platform
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa

from run_benchmark import ROOT, Server, compare_rows, result_hash, stats
from run_nested_benchmark import fixture, file_hash
from run_nested_sql_benchmark import write_inputs
from unnest_oracle import containers, flat_tables


def expected_rows(item, source, records):
    if "oracle" in item:
        return containers(records, item["oracle"])
    with sqlite3.connect(":memory:") as db:
        schema = source.schema
        db.execute("CREATE TABLE flat (" + ",".join(f"{field.name} {'TEXT' if pa.types.is_string(field.type) else 'INTEGER'}" for field in schema) + ")")
        db.executemany("INSERT INTO flat VALUES (" + ",".join("?" for _ in schema) + ")", [tuple(row.values()) for row in source.to_pylist()])
        return [[None if value is None else {"t": kind, "v": value} for value, kind in zip(row, item["types"], strict=True)]
            for row in db.execute(item["flat_sql"])]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rust", default=str(ROOT / "target/release/dremel"))
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--results-dir", type=Path, default=ROOT / "results")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    if args.rows < 25 or args.warmup < 0 or args.iterations < 1:
        parser.error("rows >= 25, warmup >= 0, iterations > 0 required")
    out = args.results_dir.resolve() / "unnest"
    out.mkdir(parents=True, exist_ok=True)
    source = fixture(args.rows)
    fixed = pa.array([[row, None, row + 1] for row in range(args.rows)], type=pa.list_(pa.int32(), 3))
    source = source.append_column("fixed_values", fixed)
    records = source.to_pylist()
    paths = write_inputs(out, source)
    flat = flat_tables(records)
    controls = {name: write_inputs(out, table, f"flat-{name}") for name, table in flat.items()}
    manifest = json.loads((ROOT / "benchmarks/workloads/unnest/manifest.json").read_text())
    expected = {item["query_id"]: expected_rows(item, flat.get(item.get("source")), records) for item in manifest}
    results, startup = [], {}
    for file_format, path in paths.items():
        servers = []
        try:
            nested = Server("Rust UNNEST", [args.rust, "bench-server", "--data", str(path), "--table", "records"])
            servers.append(nested)
            control_servers = {}
            startup[file_format] = {"rust_nested": nested.load_ns, "rust_flat": {}}
            for name, files in controls.items():
                control = Server(f"Rust flat {name}", [args.rust, "bench-server", "--data", str(files[file_format]), "--table", "flat"])
                servers.append(control)
                control_servers[name] = control
                startup[file_format]["rust_flat"][name] = control.load_ns
            for item in manifest:
                qid, sql = item["query_id"], item["sql"]
                prepared = nested.command(f"PREPARE\t{qid}\t{sql}")
                plan = nested.explain(qid)
                count = sql.upper().count("UNNEST(")
                assert sum("CorrelatedUnnestExec" in str(line) for line in plan) == count, (qid, plan)
                assert all("parent_copy=false" in str(line) for line in plan if "CorrelatedUnnestExec" in str(line)), (qid, plan)
                pairs = [("rust_nested", nested)]
                if "source" in item:
                    control = control_servers[item["source"]]
                    control.prepare(qid, item["flat_sql"])
                    pairs.append(("rust_flat", control))
                for label, server in pairs:
                    _, actual = server.execute(qid, True)
                    correct, detail = compare_rows(expected[qid], actual, "ORDER BY" in sql or "LIMIT" in sql)
                    if not correct:
                        raise RuntimeError(f"{file_format} {qid} {label}: {detail}")
                timing = {}
                if not args.validate_only:
                    samples = {label: [] for label, _ in pairs}
                    for iteration in range(args.warmup + args.iterations):
                        for label, server in pairs[::(-1 if iteration % 2 else 1)]:
                            elapsed, _ = server.execute(qid)
                            if iteration >= args.warmup:
                                samples[label].append(elapsed)
                    timing = {label: stats(values) for label, values in samples.items()}
                results.append({"query_id": qid, "category": item["category"], "format": file_format, "sql": sql,
                    "flat_sql": item.get("flat_sql"), "prepare_ns": int(prepared[2]), "row_count": len(expected[qid]),
                    "result_hash": result_hash(expected[qid], "ORDER BY" in sql or "LIMIT" in sql), "correct": True,
                    "control_rows": flat[item["source"]].num_rows if "source" in item else None,
                    "oracle": "PyArrow typed elements" if "oracle" in item else "SQLite over independent PyArrow row expansion", "timing": timing})
                print(f"{file_format} {qid} MATCH {results[-1]['oracle']}", flush=True)
        finally:
            for server in reversed(servers):
                server.close()
        for sql in ["SELECT record_id FROM records, UNNEST(record_id) AS u",
                "SELECT record_id FROM records, UNNEST(v.value) AS u, UNNEST(samples) AS v",
                "SELECT record_id FROM records RIGHT JOIN UNNEST(samples) AS u ON TRUE"]:
            rejected = subprocess.run([args.rust, "query", "--data", str(path), "--table", "records", "--sql", sql], capture_output=True, text=True)
            assert rejected.returncode != 0 and not rejected.stdout, (sql, rejected)
        sql = "SELECT u.value FROM records, UNNEST(samples) AS u"
        for flags in [["--max-result-rows", "1"], ["--query-memory-limit-mb", "1"]]:
            rejected = subprocess.run([args.rust, "query", "--data", str(path), "--table", "records", "--sql", sql, *flags], capture_output=True, text=True)
            if flags[0] == "--max-result-rows" or args.rows >= 20000:
                assert rejected.returncode != 0 and "RESOURCE_EXHAUSTED" in rejected.stderr and not rejected.stdout, (flags, rejected)
    files = {**paths, **{f"flat_{name}_{kind}": path for name, control_paths in controls.items() for kind, path in control_paths.items()}}
    document = {"rows": args.rows, "warmup": args.warmup, "iterations": args.iterations, "validate_only": args.validate_only,
        "generated_at": datetime.now(timezone.utc).isoformat(), "machine": platform.platform(), "pyarrow": pa.__version__,
        "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(), "rust_binary_sha256": file_hash(Path(args.rust)),
        "files": {name: {"path": str(path), "sha256": file_hash(path)} for name, path in files.items()}, "startup_load_ns": startup,
        "subtitle": "Correlated list offsets, outer-join preservation, ordinalities, and chained repeated fields",
        "summary_note": "Rust UNNEST checked against independent PyArrow expansion and SQLite",
        "scope": "Prepared serial Rust execution includes selected leaf normalization, list visibility, correlated offset walking, operators, and result materialization. Eager file loading, preparation, and JSON/IPC encoding are excluded. Flat controls have equivalent expanded scalar rows and matching formats, primitive widths, writer batch and row-group sizes; Python expansion and loading are outside control timing. Flat row counts differ from parent row counts. Ratios measure UNNEST overhead against pre-expanded data, not end-to-end flattening cost or a language winner. Count-only flat queries can use metadata, while UNNEST must visit offsets. Typed container projections have no flat timing control. C++ has no generic UNNEST path; no Rust/C++ comparison is claimed. No SQL Parquet projection, pruning, or streaming.",
        "queries": results}
    (out / "unnest.json").write_text(json.dumps(document, indent=2) + "\n")
    print(f"UNNEST correctness: {len(results)} / {len(results)} PASS")
    subprocess.run([sys.executable, str(ROOT / "benchmarks/scripts/generate_benchmark_report.py"), "--results-dir", str(args.results_dir),
        "--include", "unnest", "--output", str(out / "benchmark-report.html")], check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

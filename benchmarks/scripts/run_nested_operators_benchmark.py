#!/usr/bin/env python3
"""Nested operators checked independently and measured after preparation."""
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
from run_nested_sql_benchmark import typed, write_inputs
from nested_operators_oracle import expected, rank_records


def source_table(rows):
    source = fixture(rows)
    records = source.to_pylist()
    for field in ("profile", "samples"):
        source = source.append_column(field + "2", pa.array(
            [records[(index + 2) % len(records)][field] for index in range(rows)], type=source.schema.field(field).type))
    patterns = [None, {"number": None, "label": None}, {"number": 2, "label": "x"},
                {"number": 10, "label": "x"}, {"number": 2, "label": "x"}]
    return source.append_column("key", pa.array([patterns[index % 5] for index in range(rows)],
        type=pa.struct([pa.field("number", pa.int64()), pa.field("label", pa.string())])))


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
    out = args.results_dir.resolve() / "nested-operators"
    out.mkdir(parents=True, exist_ok=True)
    source = source_table(args.rows)
    records = source.to_pylist()
    paths = write_inputs(out, source)
    ranks = rank_records(records)
    flat = pa.Table.from_pylist(ranks, schema=pa.schema([pa.field(name, pa.int64()) for name in ranks[0]]))
    controls = write_inputs(out, flat, "rank-control")
    manifest = json.loads((ROOT / "benchmarks/workloads/nested-operators/manifest.json").read_text())
    answers = {item["query_id"]: expected(item, records) for item in manifest}
    # A separate SQLite check catches SQL outer-null mistakes in the Python oracle.
    with sqlite3.connect(":memory:") as db:
        db.execute("CREATE TABLE flat (record_id INTEGER, profile INTEGER, profile2 INTEGER, samples INTEGER, samples2 INTEGER)")
        db.executemany("INSERT INTO flat VALUES (?,?,?,?,?)", [tuple(row.values()) for row in ranks])
        for item in manifest:
            if "flat_sql" in item:
                actual = [[typed(bool(value) if index > 0 and value is not None else value) for index, value in enumerate(row)]
                          for row in db.execute(item["flat_sql"])]
                assert compare_rows(answers[item["query_id"]], actual, True)[0], item["query_id"]
    results, startup = [], {}
    for file_format, path in paths.items():
        servers = []
        try:
            nested = Server("Rust nested operators", [args.rust, "bench-server", "--data", str(path), "--table", "records"])
            servers.append(nested)
            control = Server("Rust rank control", [args.rust, "bench-server", "--data", str(controls[file_format]), "--table", "flat"])
            servers.append(control)
            startup[file_format] = {"rust_nested": nested.load_ns, "rust_flat": control.load_ns}
            for item in manifest:
                qid, sql = item["query_id"], item["sql"]
                prepared = nested.command(f"PREPARE\t{qid}\t{sql}")
                plan = nested.explain(qid)
                assert any("NestedValueExec(keys=typed" in str(line) for line in plan), (qid, plan)
                assert all("wide" not in str(line) for line in plan if "ArrowScanExec" in str(line)), (qid, plan)
                pairs = [("rust_nested", nested)]
                if "flat_sql" in item:
                    control.prepare(qid, item["flat_sql"])
                    pairs.append(("rust_flat", control))
                for label, server in pairs:
                    _, actual = server.execute(qid, True)
                    correct, detail = compare_rows(answers[qid], actual, "ORDER BY" in sql)
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
                    "flat_sql": item.get("flat_sql"), "prepare_ns": int(prepared[2]), "row_count": len(answers[qid]),
                    "result_hash": result_hash(answers[qid], "ORDER BY" in sql), "correct": True,
                    "oracle": "PyArrow values + independent structural oracle" + (" + SQLite rank SQL" if "flat_sql" in item else ""),
                    "observed_rss_kib": {label: server.rss_kib() for label, server in pairs},
                    "timing": timing})
                print(f"{file_format} {qid} MATCH {item['category']}", flush=True)
        finally:
            for server in reversed(servers):
                server.close()
        for sql in ["SELECT profile = profile.detail FROM records", "SELECT samples + samples FROM records", "SELECT AVG(items) FROM records"]:
            result = subprocess.run([args.rust, "query", "--data", str(path), "--table", "records", "--sql", sql], capture_output=True, text=True)
            assert result.returncode != 0 and not result.stdout, (sql, result)
        for sql, flags in [
            ("SELECT DISTINCT required_profile FROM records", ["--max-result-rows", "1"]),
            ("SELECT COUNT(*) FROM records GROUP BY required_profile", ["--query-memory-limit-mb", "1"]),
        ]:
            if flags[0] == "--query-memory-limit-mb" and args.rows < 20000:
                continue
            result = subprocess.run([args.rust, "query", "--data", str(path), "--table", "records", "--sql", sql, *flags], capture_output=True, text=True)
            assert result.returncode != 0 and "RESOURCE_EXHAUSTED" in result.stderr and not result.stdout, (sql, result)
    files = {**paths, **{f"rank_{kind}": path for kind, path in controls.items()}}
    document = {"rows": args.rows, "warmup": args.warmup, "iterations": args.iterations, "validate_only": args.validate_only,
        "generated_at": datetime.now(timezone.utc).isoformat(), "machine": platform.platform(), "pyarrow": pa.__version__,
        "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(), "rust_binary_sha256": file_hash(Path(args.rust)),
        "files": {name: {"path": str(path), "sha256": file_hash(path)} for name, path in files.items()}, "startup_load_ns": startup,
        "subtitle": "Typed container keys, structural predicates, nested aggregates, ordering, and correlated joins",
        "summary_note": "Rust structural operators checked independently over PyArrow values",
        "control_label": "Rust rank control",
        "scope": "Prepared serial execution includes selected column normalization, typed nested expression values, structural keys/comparison, operators, and result materialization. Input loading, preparation, and NDJSON encoding are excluded. Optional scalar controls use precomputed order-preserving integer ranks for equivalent scalar results, not identical data layouts. Python rank preprocessing and loading are excluded; ratios isolate nested evaluation overhead, not an end-to-end flattening comparison. Container-valued outputs and nested joins have no scalar timing control. Memory accounting is estimated; transient values are not fully bounded. Parent records and unreferenced wide siblings are not flattened. Input is eager; no generic table joins, SQL Parquet projection, spill, or distributed execution. C++ has no generic nested SQL path, so this is not a language comparison.",
        "queries": results}
    (out / "nested-operators.json").write_text(json.dumps(document, indent=2) + "\n")
    print(f"Nested operator correctness: {len(results)} / {len(results)} PASS")
    subprocess.run([sys.executable, str(ROOT / "benchmarks/scripts/generate_benchmark_report.py"), "--results-dir", str(args.results_dir),
        "--include", "nested-operators", "--output", str(out / "benchmark-report.html")], check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

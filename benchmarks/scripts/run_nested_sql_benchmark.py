#!/usr/bin/env python3
"""Validate nested SQL independently and measure prepared Rust execution."""
from __future__ import annotations

import argparse
import json
import platform
import re
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from run_benchmark import ROOT, Server, compare_rows, result_hash, stats
from run_nested_benchmark import fixture, file_hash


def typed(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return {"t": "b", "v": value}
    if isinstance(value, int):
        return {"t": "i", "v": value}
    if isinstance(value, str):
        return {"t": "s", "v": value}
    if isinstance(value, list):
        return {"t": "list", "v": [typed(item) for item in value]}
    if isinstance(value, dict):
        return {"t": "struct", "v": {key: typed(item) for key, item in value.items()}}
    raise TypeError(f"unsupported oracle value {value!r}")


def flat_records(records: list[dict]) -> list[dict]:
    result = []
    for row in records:
        profile = row["profile"]
        detail = None if profile is None else profile["detail"]
        result.append({"record_id": row["record_id"], "city": None if profile is None else profile["city"],
            "zip": None if detail is None else detail["zip"],
            "profile_present": None if profile is None else True,
            "detail_present": None if detail is None else True,
            "samples_present": None if row["samples"] is None else True,
            "list_size": None if row["samples"] is None else len(row["samples"])})
    return result


def flat_sql(sql: str) -> str:
    paths = {"profile.detail.zip": "zip", "profile.city": "city", "profile.detail": "detail_present",
             "profile": "profile_present", "samples": "samples_present"}
    sql = re.sub(r"CARDINALITY\(samples\)", "list_size", sql)
    return re.sub(r"\b(?:" + "|".join(map(re.escape, paths)) + r")\b", lambda match: paths[match[0]], sql)


def container_expected(kind: str, records: list[dict]) -> list:
    if kind == "empty":
        return []
    result = []
    for row in records[:25]:
        if kind == "struct":
            values = [row["record_id"], row["profile"]]
        elif kind == "lists":
            values = [row[name] for name in ("record_id", "items", "samples", "matrix", "large_values")]
        elif kind == "case":
            values = [row["record_id"], row["samples"] if row["record_id"] > 2 else None, row["profile"]]
        elif kind == "star":
            values = list(row.values())
        else:
            raise ValueError(kind)
        result.append([typed(value) for value in values])
    return result


def write_inputs(out: Path, source: pa.Table, name: str = "records") -> dict:
    paths = {"arrow": out / f"{name}.arrow", "parquet": out / f"{name}.parquet"}
    with ipc.new_file(paths["arrow"], source.schema) as writer:
        writer.write_table(source, max_chunksize=4096)
    pq.write_table(source, paths["parquet"], row_group_size=4096, compression="snappy")
    for path in paths.values():
        loaded = ipc.open_file(path).read_all() if path.suffix == ".arrow" else pq.read_table(path)
        assert loaded.to_pylist() == source.to_pylist(), (path, "official round trip differs")
    return paths


def main() -> int:
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
    out = args.results_dir.resolve() / "nested-sql"
    out.mkdir(parents=True, exist_ok=True)
    source = fixture(args.rows)
    records = source.to_pylist()
    paths = write_inputs(out, source)
    flattened = flat_records(records)
    flat_schema = pa.schema([pa.field("record_id", pa.int64()), pa.field("city", pa.string()), pa.field("zip", pa.int32()),
        pa.field("profile_present", pa.bool_()), pa.field("detail_present", pa.bool_()), pa.field("samples_present", pa.bool_()), pa.field("list_size", pa.int64())])
    flat = pa.Table.from_pylist(flattened, schema=flat_schema)
    flat_paths = write_inputs(out, flat, "flat")
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE records (record_id INTEGER, city TEXT, zip INTEGER, profile_present BOOLEAN, detail_present BOOLEAN, samples_present BOOLEAN, list_size INTEGER)")
    connection.executemany("INSERT INTO records VALUES (?, ?, ?, ?, ?, ?, ?)", [tuple(row.values()) for row in flattened])
    manifest = json.loads((ROOT / "benchmarks/workloads/nested-sql/manifest.json").read_text())
    expected = {}
    for item in manifest:
        if "oracle" in item:
            expected[item["query_id"]] = container_expected(item["oracle"], records)
        else:
            expected[item["query_id"]] = [[None if value is None else {"t": kind, "v": bool(value) if kind == "b" else value}
                for value, kind in zip(row, item["types"], strict=True)] for row in connection.execute(flat_sql(item["sql"]))]
    connection.close()
    output, startup = [], {}
    for file_format, path in paths.items():
        servers = []
        try:
            nested = Server("Rust nested", [args.rust, "bench-server", "--data", str(path), "--table", "records", "--threads", "1"])
            servers.append(nested)
            control = Server("Rust flat", [args.rust, "bench-server", "--data", str(flat_paths[file_format]), "--table", "records", "--threads", "1"])
            servers.append(control)
            startup[file_format] = {"rust_nested": nested.load_ns, "rust_flat": control.load_ns}
            for item in manifest:
                qid, sql = item["query_id"], item["sql"]
                prepared = nested.command(f"PREPARE\t{qid}\t{sql}")
                pairs = [("rust_nested", nested)]
                if "oracle" not in item:
                    control.prepare(qid, flat_sql(sql))
                    pairs.append(("rust_flat", control))
                for label, server in pairs:
                    _, actual = server.execute(qid, True)
                    correct, detail = compare_rows(expected[qid], actual, "ORDER BY" in sql)
                    if not correct:
                        raise RuntimeError(f"{file_format} {qid} {label}: {detail}")
                # Plan assertions catch accidental leaf-name stripping.
                if "profile.detail.zip" in sql:
                    plan = nested.explain(qid)
                    assert any("profile.detail.zip" in str(line) for line in plan), (qid, plan)
                timing = {}
                if not args.validate_only:
                    samples = {label: [] for label, _ in pairs}
                    for iteration in range(args.warmup + args.iterations):
                        for label, server in pairs[::(-1 if iteration % 2 else 1)]:
                            elapsed, _ = server.execute(qid)
                            if iteration >= args.warmup:
                                samples[label].append(elapsed)
                    timing = {label: stats(values) for label, values in samples.items()}
                output.append({"query_id": qid, "format": file_format, "category": item["category"], "correct": True,
                    "sql": sql, "flat_sql": flat_sql(sql) if "oracle" not in item else None,
                    "prepare_ns": int(prepared[2]), "row_count": len(expected[qid]),
                    "result_hash": result_hash(expected[qid], "ORDER BY" in sql),
                    "oracle": "PyArrow typed values" if "oracle" in item else "SQLite over null-aware scalar paths",
                    "timing": timing})
                print(f"{file_format} {qid} MATCH {output[-1]['oracle']}", flush=True)
        finally:
            for server in reversed(servers):
                server.close()
        for sql in ["SELECT samples.price FROM records", "SELECT profile = profile.detail FROM records",
                    "SELECT SUM(samples) FROM records", "SELECT AVG(profile) FROM records",
                    "SELECT COALESCE(profile, profile.detail) FROM records"]:
            result = subprocess.run([args.rust, "query", "--data", str(path), "--table", "records", "--sql", sql], capture_output=True, text=True)
            assert result.returncode != 0 and not result.stdout, (sql, result)
        truncated = out / f"truncated.{file_format}"
        truncated.write_bytes(path.read_bytes()[:100])
        result = subprocess.run([args.rust, "query", "--data", str(truncated), "--table", "records", "--sql", "SELECT profile FROM records"], capture_output=True, text=True)
        assert result.returncode != 0 and not result.stdout, (truncated, result)
        truncated.unlink()
        for flags, sql, expected_error in [
            (["--max-result-rows", "1"], "SELECT record_id, profile FROM records", "RESOURCE_EXHAUSTED"),
            (["--query-memory-limit-mb", "1"], "SELECT items FROM records", "RESOURCE_EXHAUSTED"),
        ]:
            result = subprocess.run([args.rust, "query", "--data", str(path), "--table", "records", "--sql", sql, *flags], capture_output=True, text=True)
            # Small fixtures can fit the memory cap; require rejection only at
            # production benchmark size, but result-row limits always apply.
            if flags[0] == "--max-result-rows" or args.rows >= 20000:
                assert result.returncode != 0 and expected_error in result.stderr and not result.stdout, (flags, result)
    document = {"rows": args.rows, "warmup": args.warmup, "iterations": args.iterations,
        "validate_only": args.validate_only, "generated_at": datetime.now(timezone.utc).isoformat(),
        "machine": platform.platform(), "pyarrow": pa.__version__, "rust_binary_sha256": file_hash(Path(args.rust)),
        "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "files": {kind: {"path": str(path), "sha256": file_hash(path)} for kind, path in {**paths, **{f"flat_{kind}": path for kind, path in flat_paths.items()}}.items()},
        "startup_load_ns": startup,
        "scope": "Prepared serial Rust execution, including path validity propagation, selected leaf normalization, operators, and result materialization. File loading, preparation, NDJSON encoding, and IPC are outside timed execution. Flat controls use the same format, writer batch/row-group sizes, primitive leaf widths, and logical scalar values with ancestor nulls applied. Container presence is represented by nullable flags in the flat control. Whole-container projections have no flat timing control. C++ has no generic nested SQL path; this is not a Rust/C++ comparison. Inputs are eager, with no SQL Parquet projection or pruning.",
        "queries": output}
    (out / "nested-sql.json").write_text(json.dumps(document, indent=2) + "\n")
    print(f"Nested SQL correctness: {len(output)} / {len(output)} PASS")
    subprocess.run([sys.executable, str(ROOT / "benchmarks/scripts/generate_benchmark_report.py"), "--results-dir", str(args.results_dir),
        "--include", "nested-sql", "--output", str(out / "benchmark-report.html")], check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

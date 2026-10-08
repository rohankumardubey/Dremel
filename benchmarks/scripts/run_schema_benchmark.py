#!/usr/bin/env python3
"""Validate named-table SQL and time prepared execution against built-in controls."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from run_benchmark import ROOT, Server, compare_rows, result_hash, stats


NAMES = {
    "reading_id": "event_id",
    "region": "country",
    "payload": "bytes",
    "temperature": "score",
    "optional_ref": "campaign_id",
    "accepted": "success",
    "readings": "events",
}


def builtin_sql(sql: str) -> str:
    return re.sub(r"\b(?:" + "|".join(NAMES) + r")\b", lambda match: NAMES[match[0]], sql)


def sqlite_result(connection: sqlite3.Connection, sql: str, types: list[str]) -> list:
    return [
        [None if value is None else {"t": kind, "v": bool(value) if kind == "b" else value}
         for value, kind in zip(row, types, strict=True)]
        for row in connection.execute(sql)
    ]


def file_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rust", default=str(ROOT / "target/release/dremel"))
    parser.add_argument("--cpp", default=str(ROOT / "benchmarks/cpp/build/dremel-cpp"))
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    parser.add_argument("--results-dir", type=Path, default=ROOT / "results")
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    if args.warmup < 0 or args.iterations < 1:
        parser.error("warmup cannot be negative; iterations must be positive")
    out = args.results_dir.resolve() / "schema"
    out.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((ROOT / "benchmarks/workloads/schema/manifest.json").read_text())
    source = ipc.open_file(args.data_dir / "events.arrow").read_all()
    names = [name for name in NAMES if name != "readings"]
    renamed = source.select([NAMES[name] for name in names]).rename_columns(names)
    paths = {"arrow": out / "readings.arrow", "parquet": out / "readings.parquet"}
    with ipc.new_file(paths["arrow"], renamed.schema) as writer:
        writer.write_table(renamed, max_chunksize=65_536)
    pq.write_table(renamed, paths["parquet"], row_group_size=65_536, compression="snappy")
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE readings (reading_id INTEGER, region TEXT, payload INTEGER, temperature REAL, optional_ref INTEGER, accepted BOOLEAN)")
    for batch in renamed.to_batches(max_chunksize=65_536):
        columns = batch.to_pydict()
        connection.executemany("INSERT INTO readings VALUES (?, ?, ?, ?, ?, ?)", zip(*(columns[name] for name in names), strict=True))
    expected = {item["query_id"]: sqlite_result(connection, item["sql"], item["types"]) for item in manifest}
    connection.close()
    output = []
    startup = {}
    for file_format, path in paths.items():
        command = [args.rust, "query", "--data", str(path), "--table", "readings"]
        default = subprocess.run(command, capture_output=True, text=True, check=True)
        if json.loads(default.stdout) != [{"t": "i", "v": renamed.num_rows}]:
            raise RuntimeError(f"{file_format}: default named-table count differs")
        rejected = subprocess.run([*command, "--streaming-parquet"], capture_output=True, text=True)
        if rejected.returncode == 0 or "unsupported" not in rejected.stderr or rejected.stdout:
            raise RuntimeError(f"{file_format}: unsupported-mode rejection failed")
        original = args.data_dir / ("events.arrow" if file_format == "arrow" else "events-snappy.parquet")
        servers = []
        try:
            named = Server("Rust named", [args.rust, "bench-server", "--data", str(path), "--table", "readings", "--threads", "1"])
            servers.append(named)
            rust = Server("Rust built-in", [args.rust, "bench-server", "--data", str(original), "--threads", "1"])
            servers.append(rust)
            cpp = Server("C++ built-in", [args.cpp, "bench-server", "--data", str(original), "--threads", "1"])
            servers.append(cpp)
            startup[file_format] = {label: server.load_ns for label, server in zip(("rust_named", "rust_builtin", "cpp_builtin"), servers, strict=True)}
            for item in manifest:
                qid, sql = item["query_id"], item["sql"]
                prepared = named.command(f"PREPARE\t{qid}\t{sql}")
                for server in (rust, cpp):
                    server.prepare(qid, builtin_sql(sql))
                ordered = "ORDER BY" in sql
                for server in servers:
                    _, rows = server.execute(qid, True)
                    correct, detail = compare_rows(expected[qid], rows, ordered)
                    if not correct:
                        raise RuntimeError(f"{file_format} {qid} {server.name}: {detail}")
                timing = {}
                if not args.validate_only:
                    for iteration in range(args.warmup):
                        for server in servers[::(-1 if iteration % 2 else 1)]:
                            server.execute(qid)
                    samples = {label: [] for label in ("rust_named", "rust_builtin", "cpp_builtin")}
                    for iteration in range(args.iterations):
                        pairs = list(zip(samples, servers, strict=True))
                        if iteration % 2:
                            pairs.reverse()
                        for label, server in pairs:
                            samples[label].append(server.execute(qid)[0])
                    timing = {label: stats(values) for label, values in samples.items()}
                output.append({"query_id": qid, "format": file_format, "category": item["category"], "correct": True,
                    "sql": sql, "builtin_sql": builtin_sql(sql), "prepare_ns": int(prepared[2]),
                    "result_hash": result_hash(expected[qid], ordered), "row_count": len(expected[qid]), "timing": timing})
                print(f"{file_format} {qid} MATCH SQLite/Rust named/Rust built-in/C++ built-in", flush=True)
        finally:
            for server in reversed(servers):
                server.close()
    document = {"rows": renamed.num_rows, "warmup": args.warmup, "iterations": args.iterations,
        "validate_only": args.validate_only, "startup_load_ns": startup,
        "files": {kind: {"path": str(path), "sha256": file_sha256(path)} for kind, path in paths.items()},
        "scope": "Prepared in-memory execution; renamed fields preserve source values. Named-table Rust is serial; built-in controls use one thread. Startup and prepare are measured separately.",
        "queries": output}
    (out / "schema.json").write_text(json.dumps(document, indent=2) + "\n")
    print(f"Schema-driven SQL correctness: {len(output)} / {len(output)} PASS")
    subprocess.run([sys.executable, str(ROOT / "benchmarks/scripts/generate_benchmark_report.py"),
        "--results-dir", str(args.results_dir), "--include", "schema", "--output", str(out / "benchmark-report.html")], check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

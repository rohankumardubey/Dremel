#!/usr/bin/env python3
"""Compare streaming nested leaf scans through official Rust/C++ Parquet readers."""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from run_benchmark import ROOT, stats


def fixture(rows: int) -> pa.Table:
    profile = pa.struct([
        pa.field("city", pa.string()),
        pa.field("detail", pa.struct([pa.field("zip", pa.int32()), pa.field("padding", pa.string())])),
        pa.field("noise", pa.string()),
    ])
    item = pa.struct([pa.field("price", pa.int64()), pa.field("name", pa.string()), pa.field("tags", pa.list_(pa.string()))])
    schema = pa.schema([
        pa.field("record_id", pa.int64(), nullable=False), pa.field("profile", profile),
        pa.field("items", pa.list_(item)), pa.field("samples", pa.list_(pa.int64())),
        pa.field("matrix", pa.list_(pa.list_(pa.int32()))), pa.field("wide", pa.string()),
        pa.field("large_values", pa.large_list(pa.int64())),
        pa.field("required_profile", pa.struct([pa.field("code", pa.int64(), nullable=False), pa.field("label", pa.string())]), nullable=False),
    ])
    patterns = [
        {"profile": None, "items": None, "samples": None, "matrix": None, "large_values": None},
        {"profile": {"city": None, "detail": None, "noise": "n"}, "items": [], "samples": [], "matrix": [], "large_values": []},
        {"profile": {"city": "NY", "detail": {"zip": None, "padding": "p"}, "noise": "n"}, "items": [None], "samples": [None], "matrix": [None, []], "large_values": [None]},
        {"profile": {"city": "LA", "detail": {"zip": 90210, "padding": "p"}, "noise": "n"}, "items": [{"price": None, "name": None, "tags": None}], "samples": [1, None, 2], "matrix": [[None, 1], [2]], "large_values": [1, None, 2]},
        {"profile": {"city": "IN", "detail": None, "noise": "n"}, "items": [{"price": 12, "name": "a", "tags": []}, None, {"price": 7, "name": "b", "tags": [None, "x"]}], "samples": [3, 4], "matrix": [[], [3, None]], "large_values": [3, 4]},
    ]
    batches = []
    for offset in range(0, rows, 4096):
        records = [{**patterns[index % len(patterns)], "record_id": index,
                    "wide": hashlib.sha256(str(index).encode()).hexdigest() * 4,
                    "required_profile": {"code": index, "label": None if index % 3 == 0 else "required"}}
                   for index in range(offset, min(rows, offset + 4096))]
        batches.append(pa.RecordBatch.from_pylist(records, schema=schema))
    return pa.Table.from_batches(batches, schema=schema)


CASES = [
    ("N001", "full_nested_scan", [], None),
    ("N002", "struct_leaf", ["profile.city"], None),
    ("N003", "deep_struct_leaf", ["profile.detail.zip"], None),
    ("N004", "list_struct_leaf", ["items.price"], None),
    ("N005", "repeated_nested_list", ["items.tags", "matrix"], None),
    ("N006", "whole_list_and_nullable_elements", ["items", "samples", "large_values"], None),
    ("N007", "mixed_required_optional", ["record_id", "required_profile.code", "profile.city", "samples"], [0]),
    ("N008", "row_group_selection", ["items.price", "profile.detail.zip"], [1]),
    ("N009", "empty_selection", ["items.price"], []),
    ("N010", "overlapping_paths", ["profile", "profile.city", "profile"], [0]),
]


def invoke(command: list[str]) -> dict:
    process = subprocess.run(command, capture_output=True, text=True, check=True)
    return json.loads(process.stdout)


def file_hash(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rust", default=str(ROOT / "target/release/dremel"))
    parser.add_argument("--cpp", default=str(ROOT / "benchmarks/cpp/build/dremel-nested-scan"))
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--batch-size", type=int, default=4096)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--results-dir", type=Path, default=ROOT / "results")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    if args.rows < 10 or args.batch_size < 1 or args.warmup < 0 or args.iterations < 1:
        parser.error("rows >= 10, batch size/iterations > 0, warmup >= 0 required")
    out = args.results_dir.resolve() / "nested"
    out.mkdir(parents=True, exist_ok=True)
    path = out / "nested.parquet"
    source = fixture(args.rows)
    pq.write_table(source, path, row_group_size=max(1, (args.rows + 3) // 4), compression="snappy")
    file = pq.ParquetFile(path)
    results = []
    keys = ["rows", "selected_rows", "selected_leaf_columns", "total_leaf_columns", "row_groups", "total_row_groups", "selected_compressed_bytes", "max_definition_levels", "max_repetition_levels"]
    with tempfile.TemporaryDirectory(prefix="dremel-nested-validation-") as temp:
        for qid, category, columns, groups in CASES:
            rust = [args.rust, "scan", "--data", str(path), "--batch-size", str(args.batch_size)]
            if columns: rust += ["--columns", ",".join(columns)]
            if groups is not None: rust += ["--row-groups", ",".join(map(str, groups)) or "none"]
            r_output = Path(temp) / f"{qid}-rust.arrow"
            r_stats = invoke([*rust, "--output", str(r_output)])
            leaves = r_stats["selected_leaf_columns"]
            # Fixture LIST wrappers are named list/element. Resolve the requested
            # logical paths independently so a wrong Rust leaf index cannot
            # become its own correctness oracle.
            logical = [file.schema.column(index).path.replace(".list.element", "") for index in range(file.metadata.num_columns)]
            expected_leaves = [index for index, name in enumerate(logical) if not columns or any(name == column or name.startswith(column + ".") for column in columns)]
            assert leaves == expected_leaves, (qid, leaves, expected_leaves)
            physical = [file.schema.column(index).path for index in expected_leaves]
            selected_groups = list(range(file.metadata.num_row_groups)) if groups is None else sorted(groups)
            assert r_stats["row_groups"] == selected_groups
            assert r_stats["max_definition_levels"] == [file.schema.column(index).max_definition_level for index in expected_leaves]
            assert r_stats["max_repetition_levels"] == [file.schema.column(index).max_repetition_level for index in expected_leaves]
            expected_bytes = sum(file.metadata.row_group(group).column(index).total_compressed_size for group in selected_groups for index in expected_leaves)
            assert r_stats["selected_compressed_bytes"] == expected_bytes
            expected = file.read_row_groups(selected_groups, columns=physical, use_threads=False)
            # Independent full-file source checks prove container/element fidelity,
            # not just agreement between two projected readers.
            if qid == "N001": assert expected.to_pylist() == source.to_pylist()
            cpp = [args.cpp, "--data", str(path), "--batch-size", str(args.batch_size),
                   "--leaves", ",".join(map(str, leaves)), "--row-groups", ",".join(map(str, selected_groups)) or "none"]
            c_output = Path(temp) / f"{qid}-cpp.arrow"
            c_stats = invoke([*cpp, "--output", str(c_output)])
            for label, output in [("Rust", r_output), ("C++", c_output)]:
                actual = ipc.open_file(output).read_all()
                assert actual.schema.equals(expected.schema, check_metadata=False), (qid, label, actual.schema, expected.schema)
                assert actual.to_pylist() == expected.to_pylist(), (qid, label, "nested values differ")
            assert all(r_stats[key] == c_stats[key] for key in keys), (qid, r_stats, c_stats)
            assert r_stats["rows"] == expected.num_rows
            if qid == "N002": assert len(leaves) == 1 and r_stats["selected_compressed_bytes"] < path.stat().st_size
            timing = {}
            if not args.validate_only:
                samples = {"rust": [], "cpp": []}
                for iteration in range(args.warmup + args.iterations):
                    pairs = [("rust", rust), ("cpp", cpp)]
                    if iteration % 2: pairs.reverse()
                    for label, command in pairs:
                        result = invoke(command)
                        assert all(result[key] == r_stats[key] for key in keys), (qid, label, "timed scan selection differs")
                        if iteration >= args.warmup: samples[label].append(result["elapsed_ns"])
                timing = {label: stats(values) for label, values in samples.items()}
            results.append({"query_id": qid, "category": category, "columns": columns, "correct": True,
                            "rust_stats": r_stats, "cpp_stats": c_stats, "timing": timing})
            print(f"{qid} MATCH Rust/C++/PyArrow (nested values, schemas, levels, selection)", flush=True)
        # Both tools reject invalid selections; Rust rejects misspelled paths and
        # never silently applies SQL or query-budget flags to this storage scan.
        invalid = [["--columns", "profile.missing"], ["--columns", "profile..city"],
                   ["--row-groups", "999"], ["--row-groups", "0,0"], ["--batch-size", "0"],
                   ["--query-memory-limit-mb", "1"], ["--sql", "SELECT 1"]]
        for extra in invalid:
            result = subprocess.run([args.rust, "scan", "--data", str(path), *extra], capture_output=True, text=True)
            assert result.returncode != 0 and not result.stdout, (extra, result)
        existing = subprocess.run([args.rust, "scan", "--data", str(path), "--output", str(r_output)], capture_output=True, text=True)
        assert existing.returncode != 0 and ipc.open_file(r_output).read_all().num_rows > 0
        truncated = Path(temp) / "truncated.parquet"
        truncated.write_bytes(path.read_bytes()[:100])
        for command in ([args.rust, "scan"], [args.cpp]):
            result = subprocess.run([*command, "--data", str(truncated)], capture_output=True, text=True)
            assert result.returncode != 0 and not result.stdout
        # Damage an unrelated sibling's encoded pages without changing metadata.
        # Selecting profile.city must still succeed; a full scan must fail.
        damaged = Path(temp) / "damaged-sibling.parquet"
        contents = bytearray(path.read_bytes())
        wide = next(index for index in range(file.metadata.num_columns) if file.schema.column(index).path == "wide")
        chunk = file.metadata.row_group(0).column(wide)
        start = chunk.dictionary_page_offset if chunk.has_dictionary_page else chunk.data_page_offset
        contents[start:start + chunk.total_compressed_size] = b"\x00" * chunk.total_compressed_size
        damaged.write_bytes(contents)
        for command in ([args.rust, "scan", "--columns", "profile.city"], [args.cpp, "--leaves", "1"]):
            assert invoke([*command, "--data", str(damaged)])["rows"] == args.rows
        incomplete = Path(temp) / "failed.arrow"
        failed = subprocess.run([args.rust, "scan", "--data", str(damaged), "--output", str(incomplete)], capture_output=True, text=True)
        assert failed.returncode != 0 and not failed.stdout and not incomplete.exists()
        assert not list(Path(temp).glob(".dremel-scan-*.arrow.tmp"))
        for command in ([args.rust, "scan"], [args.cpp]):
            failed = subprocess.run([*command, "--data", str(damaged)], capture_output=True, text=True)
            assert failed.returncode != 0 and not failed.stdout
        empty_path = Path(temp) / "empty.parquet"
        pq.write_table(source.slice(0, 0), empty_path)
        for label, command in [("rust", [args.rust, "scan"]), ("cpp", [args.cpp])]:
            destination = Path(temp) / f"empty-{label}.arrow"
            result = invoke([*command, "--data", str(empty_path), "--output", str(destination)])
            assert result["rows"] == 0 and ipc.open_file(destination).read_all().num_rows == 0
    data = {"rows": args.rows, "batch_size": args.batch_size, "warmup": args.warmup, "iterations": args.iterations,
            "validate_only": args.validate_only, "generated_at": datetime.now(timezone.utc).isoformat(),
            "dataset_sha256": file_hash(path), "pyarrow": pa.__version__, "machine": platform.platform(),
            "engine_binaries": {label: {"path": str(Path(binary).resolve()), "sha256": file_hash(Path(binary))} for label, binary in [("rust", args.rust), ("cpp", args.cpp)]},
            "scope": "Single-thread scans include metadata and selected-column decoding, exclude process startup and IPC export. Warm filesystem cache; peak batch bytes are implementation-specific estimates, not a query RSS cap.",
            "queries": results}
    (out / "nested.json").write_text(json.dumps(data, indent=2) + "\n")
    subprocess.run([sys.executable, str(ROOT / "benchmarks/scripts/generate_benchmark_report.py"), "--results-dir", str(args.results_dir), "--include", "nested", "--output", str(out / "benchmark-report.html")], check=True)
    print(f"Nested Parquet correctness: {len(results)} / {len(results)} PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

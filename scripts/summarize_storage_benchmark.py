#!/usr/bin/env python3
"""Verify equal results across storage formats and summarize their costs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

FORMATS = (
    ("dremcol1", "DREMCOL1"),
    ("arrow-ipc", "Arrow IPC"),
    ("parquet-snappy", "Parquet Snappy"),
    ("parquet-zstd", "Parquet Zstd"),
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", type=Path, default=Path("results/storage"))
    args = parser.parse_args()

    documents = {}
    for directory, label in FORMATS:
        path = args.results_dir / directory / "comparison.json"
        documents[label] = json.loads(path.read_text())

    baseline = documents["DREMCOL1"]
    expected = {
        query["query_id"]: (query["result_hash"], query["correct"])
        for query in baseline["queries"]
    }
    rows = []
    for label, document in documents.items():
        actual = {
            query["query_id"]: (query["result_hash"], query["correct"])
            for query in document["queries"]
        }
        if actual != expected:
            raise RuntimeError(f"{label} query results differ from DREMCOL1")
        source = document["input"]
        rows.append(
            {
                "format": label,
                "file": source["file"],
                "bytes": source["bytes"],
                "rust_load_time_ms": source["rust_load_time_ms"],
                "cpp_load_time_ms": source["cpp_load_time_ms"],
                "rust_cpp_query_geomean": document["rust_cpp_geometric_mean"],
                "queries_correct": len(actual),
            }
        )

    output = {
        "baseline": "DREMCOL1",
        "cross_format_correctness": True,
        "queries_per_format": len(expected),
        "formats": rows,
        "scope": "full-file load and in-memory query execution",
    }
    args.results_dir.mkdir(parents=True, exist_ok=True)
    (args.results_dir / "summary.json").write_text(
        json.dumps(output, indent=2) + "\n"
    )

    lines = [
        "STORAGE INTEROPERABILITY BENCHMARK",
        "",
        f"Cross-format correctness: PASS ({len(expected)} queries per format)",
        "",
        "Format             Size MiB   Rust load ms   C++ load ms   Rust/C++ query",
    ]
    for row in rows:
        lines.append(
            f"{row['format']:<18} {row['bytes'] / 1024 / 1024:>8.2f}"
            f" {row['rust_load_time_ms']:>14.3f}"
            f" {row['cpp_load_time_ms']:>13.3f}"
            f" {row['rust_cpp_query_geomean']:>16.4f}x"
        )
    lines += [
        "",
        "Scope: full-file decoding into each engine's in-memory columns, followed by",
        "equivalent query execution. Row-group pruning and predicate pushdown are not",
        "included in this milestone.",
    ]
    report = "\n".join(lines) + "\n"
    (args.results_dir / "report.txt").write_text(report)
    print(report)


if __name__ == "__main__":
    main()

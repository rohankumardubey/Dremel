#!/usr/bin/env python3
"""Exercise ordered Parquet output, plans, metrics, and CLI rejections."""

import json
import subprocess
import tempfile
from pathlib import Path

from run_sort_benchmark import ROOT, explain, run_query

DATA = str(ROOT / "data/events-snappy.parquet")
MANIFEST = ROOT / "benchmark/parquet-sort/manifest.json"
ENGINES = (
    str(ROOT / "dremel-rs/target/release/dremel-rs"),
    str(ROOT / "dremel-cpp/build/dremel-cpp"),
)

cases = json.loads(MANIFEST.read_text())
dataset_rows = None
for item in cases:
    sql = (MANIFEST.parent / item["file"]).read_text().strip()
    plans = [explain(engine, DATA, sql, 4096, 2, True) for engine in ENGINES]
    assert plans[0] == plans[1], (item["query_id"], plans)
    assert any("ParquetStreamExec" in operator for operator in plans[0]), plans
    assert any("ExternalMergeSortExec" in operator for operator in plans[0]), plans
    assert not any(operator.startswith("SortExec") or operator.startswith("TopKExec")
                   for operator in plans[0]), plans
    results = [run_query(engine, DATA, sql, 4096, 2, True, True) for engine in ENGINES]
    controls = [run_query(engine, DATA, sql, 4096, None, False) for engine in ENGINES]
    signatures = {(x["rows"], x["output_bytes"], x["sha256"])
                  for x in (*results, *controls)}
    assert all(x["returncode"] == 0 for x in (*results, *controls)), item["query_id"]
    dataset_rows = results[0]["stats"]["parquet_total_rows"]
    assert len(signatures) == 1, (item["query_id"], signatures)
    for result in results:
        stats = result["stats"]
        assert stats["result_streamed"] and stats["spilled"], stats
        assert stats["parquet_columns_read"] < stats["parquet_total_columns"], stats
        assert stats["parquet_batches_read"] > 0, stats
        assert stats["parquet_compressed_bytes_read"] > 0, stats
        assert stats["query_memory_peak_bytes"] <= 2 * 1024 * 1024, stats
        assert stats["parquet_peak_decoded_batch_bytes"] <= 4 * 1024 * 1024, stats
        assert not result["spill_leftovers"], result
    print(f"{item['query_id']} ordered Parquet PASS")

sql = (MANIFEST.parent / cases[0]["file"]).read_text().strip()
for engine in ENGINES:
    for flags, error in (
        (["--streaming-parquet", "--stream-results"], "EXTERNAL_SORT_REQUIRES"),
        (["--direct-parquet", "--stream-results", "--query-memory-limit-mb", "2"],
         "EXTERNAL_SORT_REQUIRES"),
    ):
        with tempfile.TemporaryDirectory(prefix="dremel-parquet-sort-reject-") as directory:
            process = subprocess.run(
                [engine, "query", "--data", DATA, "--sql", sql, *flags,
                 "--spill-dir", directory], capture_output=True, check=False)
            assert process.returncode != 0 and error.encode() in process.stderr, (
                engine, process.stderr)
            assert not any(Path(directory).iterdir()), directory

    empty_sql = "SELECT event_id FROM events WHERE event_id < 0 ORDER BY event_id"
    empty = run_query(engine, DATA, empty_sql, 4096, 2, True, True)
    assert empty["returncode"] == 0 and empty["rows"] == 0, empty
    assert not empty["spill_leftovers"] and not empty["stats"]["spilled"], empty

    if dataset_rows is not None and dataset_rows >= 65_536:
        with tempfile.TemporaryDirectory(prefix="dremel-parquet-sort-cap-") as directory:
            over_cap = subprocess.run(
                [engine, "query", "--data", DATA, "--sql",
                 "SELECT event_id, user_id, score FROM events ORDER BY score DESC, event_id ASC LIMIT 10",
                 "--streaming-parquet", "--stream-results", "--spill-dir", directory,
                 "--query-memory-limit-mb", "2", "--memory-limit-mb", "1",
                 "--batch-size", "65536"], capture_output=True, check=False)
            assert over_cap.returncode != 0 and b"RESOURCE_EXHAUSTED" in over_cap.stderr, (
                engine, over_cap.stderr)
            assert not any(Path(directory).iterdir()), directory

print(f"Ordered Parquet streaming: {len(cases)} / {len(cases)} PASS")

#!/usr/bin/env python3
"""Verify bounded result streaming and its typed-NDJSON contract."""

import hashlib
import json
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENGINES = [
    ROOT / "dremel-rs/target/release/dremel-rs",
    ROOT / "dremel-cpp/build/dremel-cpp",
]
DATA = ROOT / "data/events.dremel"
PARQUET = ROOT / "data/events-snappy.parquet"
SQL = (
    "SELECT event_id, user_id, timestamp, country, device, event_type, "
    "campaign_id, bytes, duration_ms, success, score FROM events "
    "WHERE event_id <= 100000"
)


def run_to_file(engine: Path, sql: str, output: Path, parquet: bool = False):
    command = [
        str(engine),
        "query",
        "--data",
        str(PARQUET if parquet else DATA),
        "--query-memory-limit-mb",
        "1",
        "--batch-size",
        "4096",
        "--stream-results",
        "--sql",
        sql,
        "--stats",
    ]
    if parquet:
        command.append("--streaming-parquet")
    with output.open("wb") as stream:
        return subprocess.run(
            command,
            stdout=stream,
            stderr=subprocess.PIPE,
            text=False,
            check=False,
        )


def signature(path: Path) -> tuple[int, int, str]:
    digest = hashlib.sha256()
    rows = 0
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
            rows += chunk.count(b"\n")
            size += len(chunk)
    return rows, size, digest.hexdigest()


with tempfile.TemporaryDirectory(prefix="dremel-streaming-test-") as directory:
    temporary = Path(directory)
    native_signatures = []
    parquet_signatures = []
    for engine in ENGINES:
        native_output = temporary / f"{engine.name}-native.ndjson"
        native = run_to_file(engine, SQL, native_output)
        assert native.returncode == 0, (engine, native.stderr.decode())
        native_stats = json.loads(native.stderr.decode().splitlines()[-1])
        native_signature = signature(native_output)
        assert native_stats["result_streamed"], native_stats
        assert native_stats["rows_returned"] == native_signature[0] > 0, native_stats
        assert native_stats["result_output_bytes"] == native_signature[1], native_stats
        assert native_stats["query_memory_peak_bytes"] <= 1024 * 1024, native_stats
        with native_output.open() as rows:
            assert isinstance(json.loads(next(rows)), list)
        native_signatures.append(native_signature)

        materialized = subprocess.run(
            [
                str(engine),
                "query",
                "--data",
                str(DATA),
                "--query-memory-limit-mb",
                "1",
                "--sql",
                SQL,
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        assert materialized.returncode != 0 and "RESOURCE_EXHAUSTED" in (
            materialized.stderr
        ), (engine, materialized.stderr)

        explain = subprocess.run(
            [
                str(engine),
                "query",
                "--data",
                str(DATA),
                "--stream-results",
                "--sql",
                SQL,
                "--explain",
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        assert explain.returncode == 0 and "ResultStreamExec" in explain.stdout, (
            engine,
            explain,
        )
        unsupported = subprocess.run(
            [
                str(engine),
                "query",
                "--data",
                str(DATA),
                "--stream-results",
                "--sql",
                "SELECT country, COUNT(*) FROM events GROUP BY country",
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        assert unsupported.returncode != 0 and "STREAMING_UNSUPPORTED" in (
            unsupported.stderr
        ), (engine, unsupported.stderr)

        parquet_output = temporary / f"{engine.name}-parquet.ndjson"
        parquet = run_to_file(
            engine,
            "SELECT event_id, country, campaign_id FROM events WHERE event_id <= 1000",
            parquet_output,
            parquet=True,
        )
        assert parquet.returncode == 0, (engine, parquet.stderr.decode())
        parquet_stats = json.loads(parquet.stderr.decode().splitlines()[-1])
        assert parquet_stats["result_streamed"], parquet_stats
        assert parquet_stats["parquet_streaming_fallback"] is False, parquet_stats
        parquet_signatures.append(signature(parquet_output))
        print(f"{engine.name}: streaming results PASS")

    assert native_signatures[0] == native_signatures[1], native_signatures
    assert parquet_signatures[0] == parquet_signatures[1], parquet_signatures

print("Streaming results: 12 / 12 PASS")

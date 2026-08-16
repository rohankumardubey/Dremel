#!/usr/bin/env python3
"""Verify equivalent query workspace limits in both engines."""

import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENGINES = [
    ROOT / "dremel-rs/target/release/dremel-rs",
    ROOT / "dremel-cpp/build/dremel-cpp",
]
DATA = ROOT / "data/events.dremel"
PARQUET_DATA = ROOT / "data/events-snappy.parquet"


def run(engine: Path, limit_mb: int, sql: str, stats: bool = False):
    command = [
        str(engine),
        "query",
        "--data",
        str(DATA),
        "--query-memory-limit-mb",
        str(limit_mb),
        "--sql",
        sql,
    ]
    if stats:
        command.append("--stats")
    return subprocess.run(command, text=True, capture_output=True, check=False)


def run_parquet(engine: Path, streaming: bool):
    command = [
        str(engine),
        "query",
        "--data",
        str(PARQUET_DATA),
        "--memory-limit-mb",
        "1",
        "--batch-size",
        "4096",
        "--sql",
        "SELECT SUM(bytes), AVG(score), MIN(duration_ms), MAX(timestamp), "
        "COUNT(campaign_id) FROM events WHERE event_id <= 100000",
    ]
    command.append("--streaming-parquet" if streaming else "--direct-parquet")
    return subprocess.run(command, text=True, capture_output=True, check=False)


def server_round_trip(engine: Path):
    process = subprocess.Popen(
        [
            str(engine),
            "bench-server",
            "--data",
            str(DATA),
            "--query-memory-limit-mb",
            "1",
        ],
        text=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdin and process.stdout

    def command(value: str) -> str:
        process.stdin.write(value + "\n")
        process.stdin.flush()
        return process.stdout.readline().rstrip("\n")

    try:
        assert process.stdout.readline().startswith("READY\t"), engine
        assert command("PREPARE\ttoo_large\tSELECT event_id, score FROM events").startswith(
            "OK\t"
        )
        rejected = command("EXEC\ttoo_large\t0")
        assert rejected.startswith("ERROR\tRESOURCE_EXHAUSTED"), rejected
        assert command(
            "PREPARE\ttop_k\tSELECT event_id, score FROM events ORDER BY score DESC, event_id ASC LIMIT 100"
        ).startswith("OK\t")
        recovered = command("EXEC\ttop_k\t0").split("\t")
        assert recovered[:1] == ["RESULT"] and recovered[2] == "100", recovered
        assert command("SHUTDOWN") == "BYE"
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=10)


for engine in ENGINES:
    top_k = run(
        engine,
        1,
        "SELECT event_id, score FROM events ORDER BY score DESC, event_id ASC LIMIT 100",
        stats=True,
    )
    assert top_k.returncode == 0, (engine, top_k.stderr)
    assert len(top_k.stdout.splitlines()) == 100, engine
    stats = json.loads(top_k.stderr.splitlines()[-1])
    assert stats["query_memory_accounted_bytes"] <= 1024 * 1024, stats

    projection = run(engine, 1, "SELECT event_id, score FROM events")
    assert projection.returncode != 0 and "RESOURCE_EXHAUSTED" in projection.stderr, (
        engine,
        projection.stderr,
    )

    aggregate = run(
        engine,
        4,
        "SELECT user_id, COUNT(*) FROM events GROUP BY user_id",
    )
    assert aggregate.returncode != 0 and "RESOURCE_EXHAUSTED" in aggregate.stderr, (
        engine,
        aggregate.stderr,
    )

    join = run(
        engine,
        16,
        "SELECT COUNT(*) FROM events e JOIN users u ON e.user_id = u.user_id",
    )
    assert join.returncode != 0 and "RESOURCE_EXHAUSTED" in join.stderr, (
        engine,
        join.stderr,
    )

    parquet_stream = run_parquet(engine, True)
    assert parquet_stream.returncode == 0, (engine, parquet_stream.stderr)

    parquet_materialized = run_parquet(engine, False)
    assert (
        parquet_materialized.returncode != 0
        and "RESOURCE_EXHAUSTED" in parquet_materialized.stderr
    ), (
        engine,
        parquet_materialized.stderr,
    )
    server_round_trip(engine)
    print(f"{engine.name}: memory-bounded execution PASS")

print("Memory-bounded execution: 16 / 16 PASS")

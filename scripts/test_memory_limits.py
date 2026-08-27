#!/usr/bin/env python3
"""Verify equivalent query workspace limits in both engines."""

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
PARQUET_DATA = ROOT / "data/events-snappy.parquet"


def run(
    engine: Path,
    limit_mb: int,
    sql: str,
    stats: bool = False,
    spill_dir: Path | None = None,
):
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
    if spill_dir is not None:
        command.extend(("--spill-dir", str(spill_dir)))
    if stats:
        command.append("--stats")
    return subprocess.run(command, text=True, capture_output=True, check=False)


def run_parquet(engine: Path, streaming: bool, sql: str):
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
        sql,
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
        1,
        "SELECT COUNT(*) FROM events e JOIN users u ON e.user_id = u.user_id",
    )
    assert join.returncode != 0 and "RESOURCE_EXHAUSTED" in join.stderr, (
        engine,
        join.stderr,
    )

    aggregate_sql = (
        "SELECT SUM(bytes), AVG(score), MIN(duration_ms), MAX(timestamp), "
        "COUNT(campaign_id), MIN(event_id), MAX(user_id), COUNT(country), "
        "COUNT(device), COUNT(event_type), COUNT(success) FROM events "
        "WHERE event_id <= 100000"
    )
    parquet_stream = run_parquet(engine, True, aggregate_sql)
    assert parquet_stream.returncode == 0, (engine, parquet_stream.stderr)

    parquet_materialized = run_parquet(engine, False, aggregate_sql)
    assert (
        parquet_materialized.returncode != 0
        and "RESOURCE_EXHAUSTED" in parquet_materialized.stderr
    ), (
        engine,
        parquet_materialized.stderr,
    )

    join_sql = (
        "SELECT u.segment, SUM(e.bytes), MIN(e.score), MIN(e.duration_ms), "
        "MAX(e.timestamp), COUNT(e.campaign_id), COUNT(e.country), "
        "COUNT(e.device), COUNT(e.event_type), COUNT(e.success) "
        "FROM events e JOIN users u ON e.user_id = u.user_id "
        "WHERE e.event_id <= 100000 GROUP BY u.segment ORDER BY u.segment"
    )
    parquet_join_stream = run_parquet(engine, True, join_sql)
    assert parquet_join_stream.returncode == 0, (engine, parquet_join_stream.stderr)

    parquet_join_materialized = run_parquet(engine, False, join_sql)
    assert (
        parquet_join_materialized.returncode != 0
        and "RESOURCE_EXHAUSTED" in parquet_join_materialized.stderr
    ), (engine, parquet_join_materialized.stderr)

    spill_sql = (
        "SELECT event_id, COUNT(*) AS cnt FROM events GROUP BY event_id "
        "ORDER BY event_id DESC LIMIT 10"
    )
    with tempfile.TemporaryDirectory(prefix="dremel-spill-test-") as directory:
        spill_root = Path(directory)
        spilled = run(engine, 4, spill_sql, stats=True, spill_dir=spill_root)
        assert spilled.returncode == 0 and len(spilled.stdout.splitlines()) == 10, (
            engine,
            spilled.stderr,
        )
        spill_stats = json.loads(spilled.stderr.splitlines()[-1])
        assert (
            spill_stats["spilled"] and spill_stats["spill_files_created"] > 0
        ), spill_stats
        assert spill_stats["spill_bytes_written"] == spill_stats["spill_bytes_read"], (
            engine,
            spill_stats,
        )
        assert spill_stats["query_memory_peak_bytes"] <= 4 * 1024 * 1024, spill_stats
        assert not any(spill_root.iterdir()), (engine, list(spill_root.iterdir()))

        oversized_result = run(
            engine,
            4,
            "SELECT event_id, COUNT(*) FROM events GROUP BY event_id "
            "ORDER BY event_id LIMIT 100000",
            spill_dir=spill_root,
        )
        assert oversized_result.returncode != 0 and "RESOURCE_EXHAUSTED" in (
            oversized_result.stderr
        ), (engine, oversized_result.stderr)
        assert not any(spill_root.iterdir()), (engine, list(spill_root.iterdir()))
    server_round_trip(engine)
    print(f"{engine.name}: memory-bounded execution PASS")

print("Memory-bounded execution: 28 / 28 PASS")

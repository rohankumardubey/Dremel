#!/usr/bin/env python3
"""Verify memory-bounded external merge sort in both engines."""

import hashlib
import json
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENGINES = [
    ROOT / "target/release/dremel",
    ROOT / "benchmarks/cpp/build/dremel-cpp",
]
DATA = ROOT / "data/events.dremel"
SQL = (
    "SELECT event_id, campaign_id, country, score FROM events "
    "WHERE event_id <= 100000 ORDER BY campaign_id ASC NULLS LAST, "
    "country DESC, event_id DESC"
)
MULTIPASS_SQL = (
    "SELECT event_id, campaign_id, country, score FROM events "
    "ORDER BY campaign_id ASC NULLS LAST, country DESC, event_id DESC LIMIT 100"
)


def signature(data: bytes) -> tuple[int, int, str]:
    return data.count(b"\n"), len(data), hashlib.sha256(data).hexdigest()


def query(engine: Path, extra: list[str]) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [str(engine), "query", "--data", str(DATA), "--sql", SQL, *extra],
        capture_output=True,
        check=False,
    )


signatures = []
multipass_signatures = []
plans = []
for engine in ENGINES:
    with tempfile.TemporaryDirectory(prefix="dremel-sort-test-") as directory:
        spilled = query(
            engine,
            [
                "--stream-results",
                "--spill-dir",
                directory,
                "--query-memory-limit-mb",
                "2",
                "--stats",
            ],
        )
        assert spilled.returncode == 0, (engine, spilled.stderr.decode())
        assert not any(Path(directory).iterdir()), (engine, list(Path(directory).iterdir()))
    stats = json.loads(spilled.stderr.decode().splitlines()[-1])
    result_signature = signature(spilled.stdout)
    assert stats["result_streamed"] and stats["spilled"], stats
    assert stats["spill_files_created"] >= 2, stats
    assert stats["spill_bytes_written"] > 0 and stats["spill_bytes_read"] > 0, stats
    assert stats["spill_passes"] >= 2, stats
    assert stats["rows_returned"] == result_signature[0] > 0, stats
    assert stats["result_output_bytes"] == result_signature[1], stats
    assert stats["query_memory_peak_bytes"] <= 2 * 1024 * 1024, stats

    materialized = query(engine, [])
    assert materialized.returncode == 0, (engine, materialized.stderr.decode())
    assert signature(materialized.stdout) == result_signature, engine

    with tempfile.TemporaryDirectory(prefix="dremel-sort-plan-") as directory:
        explained = query(
            engine,
            [
                "--stream-results",
                "--spill-dir",
                directory,
                "--query-memory-limit-mb",
                "2",
                "--explain",
            ],
        )
    plan = explained.stdout.decode()
    assert explained.returncode == 0, (engine, explained.stderr.decode())
    assert "ExternalMergeSortExec" in plan and "ResultStreamExec" in plan, plan
    assert "\n  SortExec" not in plan and "\n  TopKExec" not in plan, plan
    plans.append([line.strip() for line in plan.splitlines() if line.strip()])

    missing_configuration = query(engine, ["--stream-results"])
    assert missing_configuration.returncode != 0 and "EXTERNAL_SORT_REQUIRES" in (
        missing_configuration.stderr.decode()
    ), (engine, missing_configuration.stderr.decode())

    unsupported = subprocess.run(
        [
            str(engine),
            "query",
            "--data",
            str(DATA),
            "--stream-results",
            "--spill-dir",
            tempfile.gettempdir(),
            "--query-memory-limit-mb",
            "2",
            "--sql",
            "SELECT country, COUNT(*) FROM events GROUP BY country ORDER BY country",
        ],
        capture_output=True,
        check=False,
    )
    assert unsupported.returncode != 0 and "STREAMING_UNSUPPORTED" in (
        unsupported.stderr.decode()
    ), (engine, unsupported.stderr.decode())

    with tempfile.TemporaryDirectory(prefix="dremel-sort-multipass-") as directory:
        multipass = subprocess.run(
            [
                str(engine),
                "query",
                "--data",
                str(DATA),
                "--sql",
                MULTIPASS_SQL,
                "--stream-results",
                "--spill-dir",
                directory,
                "--query-memory-limit-mb",
                "2",
                "--stats",
            ],
            capture_output=True,
            check=False,
        )
        assert multipass.returncode == 0, (engine, multipass.stderr.decode())
        assert not any(Path(directory).iterdir()), (engine, list(Path(directory).iterdir()))
    multipass_stats = json.loads(multipass.stderr.decode().splitlines()[-1])
    if multipass_stats["spill_partitions"] > 32:
        assert multipass_stats["spill_passes"] >= 3, multipass_stats
    multipass_signatures.append(signature(multipass.stdout))
    signatures.append(result_signature)
    print(f"{engine.name}: external merge sort PASS")

assert signatures[0] == signatures[1], signatures
assert multipass_signatures[0] == multipass_signatures[1], multipass_signatures
assert plans[0] == plans[1], plans
print("External merge sort: 20 / 20 PASS")

#!/usr/bin/env python3
"""Cross-engine admission-control integration tests."""

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENGINES = [
    ROOT / "dremel-rs/target/release/dremel-rs",
    ROOT / "dremel-cpp/build/dremel-cpp",
]
DATA = ROOT / "data/events.dremel"


def run(engine, extra, sql):
    return subprocess.run(
        [str(engine), "query", "--data", str(DATA), *extra, "--sql", sql],
        text=True,
        capture_output=True,
        check=False,
    )


for engine in ENGINES:
    accepted = run(
        engine, ["--max-result-rows", "10"], "SELECT event_id FROM events LIMIT 10"
    )
    assert accepted.returncode == 0, (engine, accepted.stderr)
    grouped = run(
        engine,
        ["--max-result-rows", "10"],
        "SELECT country, COUNT(*) FROM events GROUP BY country",
    )
    assert grouped.returncode == 0, (engine, grouped.stderr)
    rejected = run(engine, ["--max-result-rows", "10"], "SELECT event_id FROM events")
    assert rejected.returncode != 0 and "RESOURCE_EXHAUSTED" in rejected.stderr, (
        engine,
        rejected.stderr,
    )
    group_rejected = run(
        engine,
        ["--max-result-rows", "5"],
        "SELECT country, COUNT(*) FROM events GROUP BY country",
    )
    assert (
        group_rejected.returncode != 0 and "upper bound 10" in group_rejected.stderr
    ), (engine, group_rejected.stderr)
    memory = run(engine, ["--memory-limit-mb", "1"], "SELECT COUNT(*) FROM events")
    assert memory.returncode != 0 and "RESOURCE_EXHAUSTED" in memory.stderr, (
        engine,
        memory.stderr,
    )
    print(f"{engine.name}: resource admission PASS")
print("Resource controls: 10 / 10 PASS")

#!/usr/bin/env python3
"""Check asynchronous Parquet results, admission, and failure recovery."""

from run_benchmark import ROOT, Server, compare_rows
from run_concurrency_benchmark import parse_status

DATA = str(ROOT / "data/events-snappy.parquet")
ENGINES = (
    ("Rust", str(ROOT / "dremel-rs/target/release/dremel-rs")),
    ("C++", str(ROOT / "dremel-cpp/build/dremel-cpp")),
)
SQL = (
    "SELECT event_id, country, score FROM events "
    "WHERE event_id <= 1000 ORDER BY score DESC, event_id ASC LIMIT 25"
)
WIDE_SQL = (
    "SELECT event_id, user_id, country, device, event_type, duration_ms, score "
    "FROM events WHERE event_id <= 100000 ORDER BY event_id LIMIT 10"
)

references = []
for mode in ("direct", "streaming"):
    flag = "--direct-parquet" if mode == "direct" else "--streaming-parquet"
    for name, engine in ENGINES:
        server = Server(
            f"{name} {mode}",
            [engine, "bench-server", "--data", DATA, flag,
             "--batch-size", "4096", "--memory-limit-mb", "16",
             "--max-active-queries", "1", "--queue-capacity", "2",
             "--scheduler-memory-mb", "128"],
        )
        try:
            server.prepare("ordered", SQL)
            _, expected = server.execute("ordered", True)
            response = server.command("SUBMIT\tfirst\tordered\t2\tinteractive\t15000\t32\t1")
            assert response == ["ACCEPTED", "first"], (name, mode, response)
            result = parse_status(server.command("WAIT\tfirst"))
            assert result["phase"] == "completed", (name, mode, result)
            same, detail = compare_rows(expected, result["rows"], True)
            assert same, (name, mode, detail)
            references.append((name, mode, expected))

            rejected = server.command("SUBMIT\toversized\tordered\t1\tbatch\t15000\t128\t0")
            assert rejected[0] == "REJECTED" and "ADMISSION_REJECTED" in rejected[1], rejected
            print(f"{name} {mode}: async result and admission PASS")
            rows = server.config()["rows"]
        finally:
            server.close()

        if rows < 65_536:
            continue
        limited = Server(
            f"{name} {mode} cap",
            [engine, "bench-server", "--data", DATA, flag,
             "--batch-size", "65536", "--memory-limit-mb", "1",
             "--max-active-queries", "1", "--queue-capacity", "2",
             "--scheduler-memory-mb", "128"],
        )
        try:
            limited.prepare("wide", WIDE_SQL)
            limited.prepare("count", "SELECT COUNT(*) FROM events")
            accepted = limited.command("SUBMIT\twide-request\twide\t1\tbatch\t15000\t32\t0")
            assert accepted[0] == "ACCEPTED", accepted
            failed = parse_status(limited.command("WAIT\twide-request"))
            assert failed["phase"] == "failed" and "RESOURCE_EXHAUSTED" in failed["error"], failed
            accepted = limited.command("SUBMIT\tafter-failure\tcount\t1\tbatch\t15000\t32\t1")
            assert accepted[0] == "ACCEPTED", accepted
            recovered = parse_status(limited.command("WAIT\tafter-failure"))
            assert recovered["phase"] == "completed", recovered
            print(f"{name} {mode}: decoded-memory failure recovery PASS")
        finally:
            limited.close()

for _, mode, rows in references[1:]:
    same, detail = compare_rows(references[0][2], rows, True)
    assert same, (mode, detail)
print("Asynchronous Parquet: 4 / 4 mode-engine combinations PASS")

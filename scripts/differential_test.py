#!/usr/bin/env python3
"""Differentially check representative SQL semantics against SQLite."""

from __future__ import annotations

import argparse
import csv
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from run_benchmark import Server

QUERIES = {
    "D001": "SELECT COUNT(*) FROM events;",
    "D002": "SELECT COUNT(campaign_id), SUM(bytes), AVG(score), MIN(duration_ms), MAX(duration_ms) FROM events;",
    "D003": "SELECT COUNT(*) FROM events WHERE success = true AND (country = 'IN' OR country = 'US');",
    "D004": "SELECT country, device, COUNT(*) AS cnt FROM events GROUP BY country, device;",
    "D005": "SELECT country, SUM(bytes) AS total FROM events GROUP BY country ORDER BY total DESC LIMIT 5;",
    "D006": "SELECT event_id, campaign_id > 2500 AS high_campaign FROM events LIMIT 100;",
    "D007": "SELECT event_id, NOT (campaign_id > 2500) AS low_campaign FROM events LIMIT 100;",
    "D008": "SELECT SUM(bytes + duration_ms), AVG(bytes * 1.0 / (duration_ms + 1)) FROM events;",
    "D009": "SELECT COUNT(*) FROM events WHERE campaign_id > 4500 OR success = true;",
    "D010": "SELECT event_type, AVG(score) AS avg_score FROM events GROUP BY event_type ORDER BY event_type ASC;",
    "D011": "SELECT SUM(CASE WHEN success = true THEN bytes ELSE 0 END) FROM events;",
    "D012": "SELECT COUNT(*) FROM events WHERE country IN ('IN','US') AND score BETWEEN 10.0 AND 90.0 AND event_type LIKE '%i%';",
    "D013": "SELECT COUNT(*) FROM events e JOIN users u ON e.user_id = u.user_id;",
    "D014": "SELECT u.segment, COUNT(*) AS total FROM events e JOIN users u ON e.user_id = u.user_id GROUP BY u.segment HAVING COUNT(*) > 0 ORDER BY u.segment;",
    "D015": "SELECT event_id, ROW_NUMBER() OVER (PARTITION BY country ORDER BY score DESC, event_id ASC) FROM events WHERE event_id <= 500 ORDER BY event_id;",
    "D016": "WITH totals AS (SELECT country, COUNT(*) AS total FROM events GROUP BY country) SELECT country, total FROM totals WHERE total > 0 ORDER BY country;",
    "D017": "SELECT event_id FROM events e WHERE event_id <= 500 AND EXISTS (SELECT campaign_id FROM campaigns c WHERE c.campaign_id = e.campaign_id) ORDER BY event_id;",
    "D018": "SELECT country AS value FROM events WHERE event_id <= 100 UNION SELECT region FROM users WHERE user_id <= 100;",
    "D019": "SELECT d.country, d.total FROM (SELECT country, COUNT(*) AS total FROM events GROUP BY country) d WHERE d.total > 0 ORDER BY d.country;",
    "D020": "SELECT MAX(budget) FROM campaigns;",
    "D021": "SELECT COUNT(*) FROM (SELECT campaign_id FROM campaigns WHERE campaign_id <= 5) c, (SELECT user_id FROM users WHERE user_id <= 10) u;",
    "D022": "SELECT campaign_id, budget + 1, budget + 0.5 FROM campaigns WHERE campaign_id <= 100 ORDER BY campaign_id;",
    "D023": "SELECT event_id, MIN(score) OVER (PARTITION BY country), MAX(score) OVER (PARTITION BY country) FROM events WHERE event_id <= 500 ORDER BY event_id;",
    "D024": "SELECT COUNT(*) FROM events WHERE event_id > 900000 AND event_id < 1000;",
    "D025": "SELECT COUNT(*) FROM campaigns c LEFT JOIN events e ON c.campaign_id = e.campaign_id WHERE e.event_id IS NULL;",
}


def engine_value(item):
    if item is None:
        return None
    return float(item["v"]) if item["t"] == "d" else item["v"]


def value_equal(left, right) -> bool:
    if left is None or right is None:
        return left is right
    if isinstance(left, (int, float, bool)) and isinstance(right, (int, float, bool)):
        x, y = float(left), float(right)
        return abs(x - y) <= max(1e-9, 1e-9 * max(abs(x), abs(y)))
    return left == right


def canonical(rows, ordered):
    if ordered:
        return rows
    return sorted(rows, key=lambda row: repr(tuple(row)))


def compare(actual, expected, ordered):
    actual, expected = canonical(actual, ordered), canonical(expected, ordered)
    if len(actual) != len(expected):
        return False
    return all(
        len(a) == len(e) and all(value_equal(x, y) for x, y in zip(a, e))
        for a, e in zip(actual, expected)
    )


def load_sqlite(path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.execute("""CREATE TABLE events(
      event_id INTEGER NOT NULL, user_id INTEGER NOT NULL, timestamp INTEGER NOT NULL,
      country TEXT NOT NULL, device TEXT NOT NULL, event_type TEXT NOT NULL,
      duration_ms INTEGER NOT NULL, bytes INTEGER NOT NULL, score REAL NOT NULL,
      success INTEGER NOT NULL, campaign_id INTEGER)""")
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        rows = (
            (
                int(r["event_id"]),
                int(r["user_id"]),
                int(r["timestamp"]),
                r["country"],
                r["device"],
                r["event_type"],
                int(r["duration_ms"]),
                int(r["bytes"]),
                float(r["score"]),
                int(r["success"] == "true"),
                None if not r["campaign_id"] else int(r["campaign_id"]),
            )
            for r in reader
        )
        db.executemany("INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?)", rows)
    db.execute("""CREATE TABLE users(
      user_id INTEGER NOT NULL, segment TEXT NOT NULL, signup_date TEXT NOT NULL,
      lifetime_value NUMERIC NOT NULL, region TEXT NOT NULL, active INTEGER NOT NULL)""")
    with (path.parent / "users.csv").open(newline="") as handle:
        reader = csv.DictReader(handle)
        rows = (
            (
                int(r["user_id"]),
                r["segment"],
                r["signup_date"],
                float(r["lifetime_value"]),
                r["region"],
                int(r["active"] == "true"),
            )
            for r in reader
        )
        db.executemany("INSERT INTO users VALUES(?,?,?,?,?,?)", rows)
    db.execute("""CREATE TABLE campaigns(
      campaign_id INTEGER NOT NULL, campaign_name TEXT NOT NULL, budget NUMERIC NOT NULL,
      start_date TEXT NOT NULL, end_date TEXT NOT NULL, channel TEXT NOT NULL)""")
    with (path.parent / "campaigns.csv").open(newline="") as handle:
        reader = csv.DictReader(handle)
        rows = (
            (
                int(r["campaign_id"]),
                r["campaign_name"],
                float(r["budget"]),
                r["start_date"],
                r["end_date"],
                r["channel"],
            )
            for r in reader
        )
        db.executemany("INSERT INTO campaigns VALUES(?,?,?,?,?,?)", rows)
    return db


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=5000)
    parser.add_argument("--threads", type=int, default=2)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="dremel-differential-") as directory:
        base = Path(directory)
        data, metadata = base / "events.csv", base / "metadata.json"
        subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts/generate_data.py"),
                "--rows",
                str(args.rows),
                "--seed",
                "0xD3E3A5E1",
                "--output",
                str(data),
                "--metadata",
                str(metadata),
            ],
            check=True,
        )
        tail = [
            "bench-server",
            "--data",
            str(data),
            "--threads",
            str(args.threads),
            "--batch-size",
            "1024",
        ]
        servers = [
            Server("Rust", [str(ROOT / "dremel-rs/target/release/dremel-rs"), *tail]),
            Server("C++", [str(ROOT / "dremel-cpp/build/dremel-cpp"), *tail]),
        ]
        db = load_sqlite(data)
        try:
            for query_id, sql in QUERIES.items():
                expected = [list(row) for row in db.execute(sql)]
                ordered = "ORDER BY" in sql.upper() or "LIMIT" in sql.upper()
                for server in servers:
                    server.prepare(query_id, sql)
                    _, typed_rows = server.execute(query_id, True)
                    actual = [
                        [engine_value(value) for value in row] for row in typed_rows
                    ]
                    if not compare(actual, expected, ordered):
                        raise RuntimeError(
                            f"{query_id} differs from SQLite in {server.name}\n"
                            f"engine={actual[:5]}\nsqlite={expected[:5]}"
                        )
                print(f"{query_id} Rust=C++=SQLite")
        finally:
            db.close()
            for server in servers:
                server.close()
    print(f"Differential correctness: {len(QUERIES)} / {len(QUERIES)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Run deterministic asynchronous workload mixes against both engines."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from collections import defaultdict
from pathlib import Path

from run_benchmark import ROOT, Server, compare_rows, result_hash


def percentile(values: list[int], fraction: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def latency(values: list[int]) -> dict:
    return {
        "count": len(values),
        "p50_ns": int(statistics.median(values)) if values else 0,
        "p95_ns": percentile(values, 0.95),
        "p99_ns": percentile(values, 0.99),
        "max_ns": max(values, default=0),
    }


def parse_status(parts: list[str]) -> dict:
    if len(parts) < 9 or parts[0] != "STATUS":
        raise RuntimeError(f"invalid asynchronous status: {parts}")
    return {
        "request_id": parts[1],
        "phase": parts[2],
        "priority": int(parts[3]),
        "group": parts[4],
        "queue_ns": int(parts[5]),
        "execution_ns": int(parts[6]),
        "row_count": int(parts[7]),
        "rows": json.loads(parts[8]),
        "error": parts[9] if len(parts) > 9 else "",
    }


def summarize(records: list[dict], offered: dict[str, int], elapsed_ns: int) -> dict:
    counts = defaultdict(int)
    for record in records:
        counts[record["phase"]] += 1
    completed = [record for record in records if record["phase"] == "completed"]
    group_completed = defaultdict(int)
    for record in completed:
        group_completed[record["group"]] += 1
    normalized = [
        group_completed[group] / count for group, count in offered.items() if count
    ]
    fairness = (
        (
            sum(normalized) ** 2
            / (len(normalized) * sum(value * value for value in normalized))
        )
        if normalized and any(normalized)
        else 1.0
    )
    per_priority = {}
    for priority in (0, 1, 2):
        subset = [record for record in records if record["priority"] == priority]
        per_priority[str(priority)] = {
            "submitted": len(subset),
            "completed": sum(record["phase"] == "completed" for record in subset),
            "queue_delay": latency([record["queue_ns"] for record in subset]),
            "execution_latency": latency(
                [record["execution_ns"] for record in subset if record["execution_ns"]]
            ),
        }
    per_group = {}
    for group, offered_count in offered.items():
        subset = [record for record in records if record["group"] == group]
        per_group[group] = {
            "offered": offered_count,
            "accepted": len(subset),
            "completed": sum(record["phase"] == "completed" for record in subset),
            "cancelled": sum(record["phase"] == "cancelled" for record in subset),
            "deadline": sum(record["phase"] == "deadline" for record in subset),
            "queue_delay": latency([record["queue_ns"] for record in subset]),
            "execution_latency": latency(
                [record["execution_ns"] for record in subset if record["execution_ns"]]
            ),
        }
    return {
        "elapsed_ns": elapsed_ns,
        "throughput_completed_qps": len(completed) / (elapsed_ns / 1e9),
        "submitted": sum(offered.values()),
        "accepted": len(records),
        "completed": counts["completed"],
        "failed": counts["failed"],
        "cancelled": counts["cancelled"],
        "deadline_misses": counts["deadline"],
        "admission_rejected": sum(offered.values()) - len(records),
        "queue_delay": latency([record["queue_ns"] for record in records]),
        "execution_latency": latency(
            [record["execution_ns"] for record in records if record["execution_ns"]]
        ),
        "jain_fairness_index": fairness,
        "per_priority": per_priority,
        "per_group": per_group,
    }


def run_mix(
    name: str, server: Server, manifest: dict, references: dict[str, list]
) -> dict:
    mix = manifest["mix"]
    query_ids = [query["query_id"] for query in manifest["queries"]]
    offered = defaultdict(int)
    accepted: list[tuple[str, str, bool]] = []
    # Initialize catalog and worker machinery outside the measured interval.
    warm_id = f"{name}-warmup"
    response = server.command(
        f"SUBMIT\t{warm_id}\t{query_ids[0]}\t1\twarmup\t15000\t4\t0"
    )
    if response[0] != "ACCEPTED":
        raise RuntimeError(f"{name}: scheduler warmup rejected: {response}")
    warm = parse_status(server.command(f"WAIT\t{warm_id}"))
    if warm["phase"] != "completed":
        raise RuntimeError(f"{name}: scheduler warmup failed: {warm}")

    started = time.perf_counter_ns()
    cancel_ids = []
    for index in range(mix["request_count"]):
        request_id = f"{name}-r{index:03d}"
        query_id = query_ids[index % len(query_ids)]
        priority = mix["priorities"][index % len(mix["priorities"])]
        group = mix["groups"][index % len(mix["groups"])]
        offered[group] += 1
        short_deadline = index > 0 and index % mix["short_deadline_every"] == 0
        deadline = (
            mix["short_deadline_ms"] if short_deadline else mix["normal_deadline_ms"]
        )
        response = server.command(
            f"SUBMIT\t{request_id}\t{query_id}\t{priority}\t{group}\t{deadline}\t"
            f"{mix['reservation_mb']}\t1"
        )
        if response[0] == "ACCEPTED":
            accepted.append((request_id, query_id, short_deadline))
            if index > 0 and index % mix["cancel_every"] == 0:
                cancel_ids.append(request_id)
        elif response[0] != "REJECTED":
            raise RuntimeError(f"{name}: unexpected submission response {response}")
    for request_id in cancel_ids:
        response = server.command(f"CANCEL\t{request_id}")
        if response[0] != "CANCELLED":
            raise RuntimeError(f"{name}: cancellation failed: {response}")
    records = []
    correctness_checked = 0
    for request_id, query_id, short_deadline in accepted:
        record = parse_status(server.command(f"WAIT\t{request_id}"))
        record["query_id"] = query_id
        record["short_deadline"] = short_deadline
        if record["phase"] == "completed":
            ordered = any(
                query["query_id"] == query_id
                and "ORDER BY" in query["description"].upper()
                for query in manifest["queries"]
            )
            correct, detail = compare_rows(
                references[query_id], record["rows"], ordered
            )
            if not correct:
                raise RuntimeError(f"{name} {request_id}: result mismatch: {detail}")
            record["correct"] = True
            correctness_checked += 1
        else:
            record["correct"] = None
        records.append(record)
    elapsed_ns = time.perf_counter_ns() - started
    summary = summarize(records, dict(offered), elapsed_ns)
    summary["completed_results_validated"] = correctness_checked
    summary["all_completed_results_correct"] = (
        correctness_checked == summary["completed"]
    )
    return {"summary": summary, "requests": records}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rust", default=str(ROOT / "dremel-rs/target/release/dremel-rs")
    )
    parser.add_argument("--cpp", default=str(ROOT / "dremel-cpp/build/dremel-cpp"))
    parser.add_argument("--data", default=str(ROOT / "data/events.dremel"))
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=4096)
    parser.add_argument(
        "--manifest", type=Path, default=ROOT / "benchmark/concurrency/manifest.json"
    )
    parser.add_argument(
        "--results-dir", type=Path, default=ROOT / "results/concurrency"
    )
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    scheduler = manifest["scheduler"]
    tail = [
        "bench-server",
        "--data",
        args.data,
        "--threads",
        str(args.threads),
        "--batch-size",
        str(args.batch_size),
        "--max-active-queries",
        str(scheduler["max_active_queries"]),
        "--queue-capacity",
        str(scheduler["queue_capacity"]),
        "--scheduler-memory-mb",
        str(scheduler["memory_mb"]),
    ]
    servers: list[Server] = []
    try:
        rust = Server("Rust", [args.rust, *tail])
        servers.append(rust)
        cpp = Server("C++", [args.cpp, *tail])
        servers.append(cpp)
        references = {}
        for query in manifest["queries"]:
            query_id = query["query_id"]
            sql = (
                (args.manifest.parent / query["file"])
                .read_text()
                .strip()
                .replace("\n", " ")
            )
            rust.prepare(query_id, sql)
            cpp.prepare(query_id, sql)
            _, rust_rows = rust.execute(query_id, True)
            _, cpp_rows = cpp.execute(query_id, True)
            ordered = "ORDER BY" in sql.upper()
            correct, detail = compare_rows(rust_rows, cpp_rows, ordered)
            if not correct:
                raise RuntimeError(f"reference {query_id} mismatch: {detail}")
            references[query_id] = rust_rows
        results = {
            "configuration": scheduler,
            "workload": manifest["mix"],
            "reference_hashes": {
                query_id: result_hash(
                    rows,
                    any(
                        query["query_id"] == query_id
                        and "ORDER BY" in query["description"].upper()
                        for query in manifest["queries"]
                    ),
                )
                for query_id, rows in references.items()
            },
            "rust": run_mix("rust", rust, manifest, references),
            "cpp": run_mix("cpp", cpp, manifest, references),
        }
        out = args.results_dir.resolve()
        out.mkdir(parents=True, exist_ok=True)
        (out / "concurrency.json").write_text(json.dumps(results, indent=2) + "\n")
        lines = ["CONCURRENT WORKLOAD BENCHMARK"]
        for engine in ("rust", "cpp"):
            summary = results[engine]["summary"]
            lines += [
                f"{engine.upper()}: {summary['throughput_completed_qps']:.2f} completed qps",
                (
                    f"  completed/failed/cancelled/deadline/rejected: "
                    f"{summary['completed']}/{summary['failed']}/{summary['cancelled']}/"
                    f"{summary['deadline_misses']}/{summary['admission_rejected']}"
                ),
                (
                    f"  queue p50/p95/p99 ms: "
                    f"{summary['queue_delay']['p50_ns'] / 1e6:.3f}/"
                    f"{summary['queue_delay']['p95_ns'] / 1e6:.3f}/"
                    f"{summary['queue_delay']['p99_ns'] / 1e6:.3f}"
                ),
                (
                    f"  execution p50/p95/p99 ms: "
                    f"{summary['execution_latency']['p50_ns'] / 1e6:.3f}/"
                    f"{summary['execution_latency']['p95_ns'] / 1e6:.3f}/"
                    f"{summary['execution_latency']['p99_ns'] / 1e6:.3f}"
                ),
                f"  Jain fairness: {summary['jain_fairness_index']:.6f}",
                (
                    f"  completed result validation: "
                    f"{summary['completed_results_validated']}/{summary['completed']}"
                ),
            ]
        report = "\n".join(lines) + "\n"
        (out / "report.txt").write_text(report)
        print(report)
        return 0
    finally:
        for server in reversed(servers):
            server.close()


if __name__ == "__main__":
    raise SystemExit(main())

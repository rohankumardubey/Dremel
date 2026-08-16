#!/usr/bin/env python3
"""Correctness validator and interleaved benchmark controller."""

from __future__ import annotations

import argparse
import csv
import datetime
import hashlib
import json
import math
import os
import platform
import shutil
import statistics
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def dataset_details(path: str, metadata: dict) -> dict[str, object]:
    source = Path(path)
    if source.suffix == ".dremel":
        expected = metadata.get("column_store_sha256")
        name = "DREMCOL1"
    elif source.suffix == ".csv":
        expected = metadata.get("csv_sha256")
        name = "CSV"
    elif source.suffix in (".arrow", ".parquet"):
        files = metadata.get("interoperable", {}).get("files", {})
        expected = files.get(source.name, {}).get("sha256")
        if source.suffix == ".arrow":
            name = "Arrow IPC"
        else:
            compression = source.stem.rsplit("-", 1)[-1].capitalize()
            name = f"Parquet ({compression})"
    else:
        raise RuntimeError(f"unsupported dataset format: {source.suffix}")
    if not expected:
        raise RuntimeError(f"no metadata hash for {source.name}")
    return {
        "path": str(source),
        "file": source.name,
        "format": name,
        "bytes": source.stat().st_size,
        "sha256": expected,
    }


class Server:
    def __init__(
        self, name: str, command: list[str], env: dict[str, str] | None = None
    ):
        self.name = name
        self.last_query_memory = {"limit_bytes": 0, "accounted_bytes": 0}
        self.last_scan_metrics = {
            "total_rows": 0,
            "rows_read": 0,
            "total_row_groups": 0,
            "row_groups_read": 0,
            "total_columns": 0,
            "columns_read": 0,
            "compressed_bytes_read": 0,
        }
        self.process = subprocess.Popen(
            command,
            cwd=ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )
        ready = self.process.stdout.readline().rstrip("\n")
        if not ready.startswith("READY\t"):
            error = self.process.stderr.read()
            raise RuntimeError(f"{name} did not become ready: {ready} {error}")
        self.load_ns = int(ready.split("\t")[1])

    def command(self, line: str) -> list[str]:
        assert self.process.stdin and self.process.stdout
        self.process.stdin.write(line + "\n")
        self.process.stdin.flush()
        response = self.process.stdout.readline().rstrip("\n")
        if not response or response.startswith("ERROR"):
            error = (
                self.process.stderr.readline().rstrip("\n")
                if self.process.stderr
                else ""
            )
            raise RuntimeError(
                f"{self.name}: {line.split(chr(9))[0]} failed: {response} {error}"
            )
        return response.split("\t")

    def config(self) -> dict[str, int]:
        p = self.command("CONFIG")
        return dict(
            zip(("threads", "batch_size", "partitions", "rows"), map(int, p[1:]))
        )

    def prepare(self, qid: str, sql: str) -> None:
        response = self.command(f"PREPARE\t{qid}\t{sql}")
        if response[:2] != ["OK", qid]:
            raise RuntimeError(f"bad prepare response from {self.name}: {response}")

    def execute(self, qid: str, rows: bool = False) -> tuple[int, list]:
        p = self.command(f"EXEC\t{qid}\t{int(rows)}")
        if len(p) >= 6:
            self.last_query_memory = {
                "limit_bytes": int(p[4]),
                "accounted_bytes": int(p[5]),
            }
        if len(p) >= 13:
            self.last_scan_metrics = dict(
                zip(
                    (
                        "total_rows",
                        "rows_read",
                        "total_row_groups",
                        "row_groups_read",
                        "total_columns",
                        "columns_read",
                        "compressed_bytes_read",
                    ),
                    map(int, p[6:13]),
                )
            )
        if len(p) >= 16:
            self.last_scan_metrics.update(
                {
                    "batches_read": int(p[13]),
                    "peak_decoded_batch_bytes": int(p[14]),
                    "streaming_fallback": p[15].lower() in ("1", "true"),
                }
            )
        return int(p[1]), json.loads(p[3])

    def e2e(self, qid: str, sql: str) -> int:
        return int(self.command(f"E2E\t{qid}\t{sql}\t0")[1])

    def explain(self, qid: str) -> list[str]:
        response = self.command(f"EXPLAIN\t{qid}")
        if len(response) > 1 and response[1].startswith("["):
            return json.loads(response[1])
        if len(response) > 1:
            return response[1].split(",")
        # The C++ server prefixes its comma-delimited normalized plan directly.
        return response[0].removeprefix("EXPLAIN,").split(",")

    def rss_kib(self) -> int | None:
        try:
            return int(
                subprocess.check_output(
                    ["ps", "-o", "rss=", "-p", str(self.process.pid)], text=True
                ).strip()
            )
        except (OSError, ValueError, subprocess.SubprocessError):
            return None

    def close(self) -> None:
        if self.process.poll() is None:
            try:
                self.command("SHUTDOWN")
                self.process.wait(timeout=10)
            except (OSError, RuntimeError, subprocess.SubprocessError):
                self.process.kill()


def value(v):
    if v is None:
        return ("n", None)
    return (v["t"], v["v"])


def row_key(row):
    return tuple(
        ("n", "")
        if x is None
        else (x["t"], format(x["v"], ".17g") if x["t"] == "f" else str(x["v"]))
        for x in row
    )


def equivalent_value(a, b) -> bool:
    a, b = value(a), value(b)
    if a[0] in ("i", "f") and b[0] in ("i", "f"):
        x, y = float(a[1]), float(b[1])
        return abs(x - y) <= max(1e-9, 1e-9 * max(abs(x), abs(y)))
    return a == b


def canonical(rows: list, ordered: bool) -> list:
    return rows if ordered else sorted(rows, key=row_key)


def compare_rows(rust: list, cpp: list, ordered: bool) -> tuple[bool, str]:
    rust, cpp = canonical(rust, ordered), canonical(cpp, ordered)
    if len(rust) != len(cpp):
        return False, f"row count {len(rust)} != {len(cpp)}"
    for i, (rr, cr) in enumerate(zip(rust, cpp)):
        if len(rr) != len(cr):
            return False, f"row {i} width differs"
        for j, (rv, cv) in enumerate(zip(rr, cr)):
            if not equivalent_value(rv, cv):
                return False, f"row {i}, column {j}: Rust={rv!r}, C++={cv!r}"
    return True, ""


def result_hash(rows: list, ordered: bool) -> str:
    normalized = canonical(rows, ordered)
    payload = json.dumps(
        normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def stats(samples: list[int]) -> dict:
    ordered = sorted(samples)
    p95_index = max(0, math.ceil(0.95 * len(ordered)) - 1)
    return {
        "samples_ns": samples,
        "sample_count": len(samples),
        "min_ns": min(samples),
        "median_ns": statistics.median(samples),
        "mean_ns": statistics.mean(samples),
        "p95_ns": ordered[p95_index],
        "stddev_ns": statistics.pstdev(samples),
    }


def version(command: list[str]) -> str:
    try:
        return subprocess.check_output(
            command, cwd=ROOT, text=True, stderr=subprocess.STDOUT
        ).splitlines()[0]
    except (OSError, subprocess.SubprocessError):
        return "unavailable"


def command_output(command: list[str]) -> str:
    try:
        return subprocess.check_output(
            command, cwd=ROOT, text=True, stderr=subprocess.STDOUT
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return "unavailable"


def cmake_cache_value(name: str) -> str | None:
    cache = ROOT / "dremel-cpp/build/CMakeCache.txt"
    if not cache.exists():
        return None
    prefix = f"{name}:"
    for line in cache.read_text().splitlines():
        if line.startswith(prefix) and "=" in line:
            return line.split("=", 1)[1]
    return None


def cxx_details() -> tuple[str, str, str]:
    compiler = os.getenv("CXX") or cmake_cache_value("CMAKE_CXX_COMPILER") or "unknown"
    compiler_version = os.getenv("CXX_VERSION")
    if not compiler_version and compiler != "unknown":
        compiler_version = version([compiler, "--version"])
    standard = (
        os.getenv("CPP_STANDARD") or cmake_cache_value("DREMEL_CXX_STANDARD") or "26"
    )
    stdlib = os.getenv("CXX_STDLIB_VERSION")
    if not stdlib and compiler != "unknown":
        try:
            macros = subprocess.check_output(
                [compiler, f"-std=c++{standard}", "-dM", "-E", "-x", "c++", "-"],
                input="#include <version>\n",
                text=True,
                stderr=subprocess.STDOUT,
            )
            stdlib = next(
                (
                    line.split()[2]
                    for line in macros.splitlines()
                    if line.startswith("#define _LIBCPP_VERSION ")
                ),
                "unknown",
            )
        except (OSError, subprocess.SubprocessError):
            stdlib = "unknown"
    return compiler, compiler_version or "unknown", stdlib or "unknown"


def environment_text(cfg, metadata, rust, cpp, args, rust_rss, cpp_rss) -> str:
    uname = platform.uname()
    try:
        memory = subprocess.check_output(
            ["sysctl", "-n", "hw.memsize"], text=True
        ).strip()
    except (OSError, subprocess.SubprocessError):
        memory = "unknown"
    try:
        cpu = subprocess.check_output(
            ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
        ).strip()
    except (OSError, subprocess.SubprocessError):
        cpu = platform.processor() or "unknown"
    git_sha = version(["git", "rev-parse", "HEAD"])
    git_dirty = "unknown"
    try:
        inside = (
            subprocess.run(
                ["git", "rev-parse", "--is-inside-work-tree"],
                cwd=ROOT,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            ).returncode
            == 0
        )
        if inside:
            git_dirty = (
                "yes"
                if subprocess.run(
                    ["git", "diff", "--quiet"],
                    cwd=ROOT,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                ).returncode
                else "no"
            )
    except OSError:
        pass
    rust_verbose = command_output(["rustc", "-vV"])
    rust_llvm = next(
        (
            line.split(":", 1)[1].strip()
            for line in rust_verbose.splitlines()
            if line.startswith("LLVM version:")
        ),
        "unknown",
    )
    cxx_path, cxx_version, cxx_stdlib = cxx_details()
    dataset = dataset_details(args.data, metadata)
    fields = {
        "benchmark_date_utc": datetime.datetime.now(datetime.UTC).isoformat(),
        "os": uname.system,
        "kernel": uname.release,
        "architecture": uname.machine,
        "hostname": uname.node,
        "cpu_model": cpu,
        "logical_cpu_count": os.cpu_count(),
        "total_ram_bytes": memory,
        "cpu_affinity": os.getenv("BENCH_CPUSET") or "unsupported / disabled",
        "rustc": version(["rustc", "--version"]),
        "rust_edition": "2024",
        "rust_llvm_backend": rust_llvm,
        "cargo": version(["cargo", "--version"]),
        "cxx_compiler": cxx_version,
        "cxx_compiler_path": cxx_path,
        "cxx_standard": f"C++{os.getenv('CPP_STANDARD') or cmake_cache_value('DREMEL_CXX_STANDARD') or '26'}",
        "cxx_standard_library": (
            f"libc++ {cxx_stdlib}"
            if platform.system() == "Darwin"
            else "detected by compiler default"
        ),
        "cmake": version(["cmake", "--version"]),
        "python": sys.version.splitlines()[0],
        "dataset_rows": metadata["row_count"],
        "dataset_format": dataset["format"],
        "dataset_sha256": dataset["sha256"],
        "dataset_seed": metadata["seed"],
        "batch_size": cfg["batch_size"],
        "thread_count": cfg["threads"],
        "logical_partitions": cfg["partitions"],
        "query_memory_limit_mb": args.query_memory_limit_mb,
        "warmup_count": args.warmup,
        "measurement_count": args.iterations,
        "tie_threshold_pct": args.tie_threshold,
        "lto": "enabled" if os.getenv("LTO", "0") == "1" else "disabled",
        "native_cpu_optimization": "enabled"
        if os.getenv("NATIVE", "0") == "1"
        else "disabled",
        "git_commit_sha": git_sha,
        "git_dirty": git_dirty,
        "rust_load_time_ms": rust.load_ns / 1e6,
        "cpp_load_time_ms": cpp.load_ns / 1e6,
        "rust_peak_rss_kib_observed": rust_rss,
        "cpp_peak_rss_kib_observed": cpp_rss,
        "cpu_governor": "not exposed portably",
        "turbo_boost": "not detected without privilege",
    }
    return "\n".join(f"{key}: {val}" for key, val in fields.items()) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rust", default=str(ROOT / "dremel-rs/target/release/dremel-rs")
    )
    parser.add_argument("--cpp", default=str(ROOT / "dremel-cpp/build/dremel-cpp"))
    parser.add_argument("--data", default=str(ROOT / "data/events.dremel"))
    parser.add_argument(
        "--threads", type=int, default=int(os.getenv("BENCH_THREADS", "4"))
    )
    parser.add_argument(
        "--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "4096"))
    )
    parser.add_argument(
        "--query-memory-limit-mb",
        type=int,
        default=int(os.getenv("QUERY_MEMORY_LIMIT_MB", "0")),
    )
    parser.add_argument("--warmup", type=int, default=int(os.getenv("WARMUP", "3")))
    parser.add_argument(
        "--iterations", type=int, default=int(os.getenv("ITERATIONS", "20"))
    )
    parser.add_argument(
        "--tie-threshold",
        type=float,
        default=float(os.getenv("TIE_THRESHOLD_PCT", "1.0")),
    )
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument(
        "--manifest", type=Path, default=ROOT / "benchmark/manifest.json"
    )
    parser.add_argument("--results-dir", type=Path, default=ROOT / "results")
    args = parser.parse_args()
    if (
        args.iterations < 1
        or args.warmup < 0
        or args.threads < 1
        or args.batch_size < 1
        or args.query_memory_limit_mb < 0
    ):
        parser.error(
            "threads, batch size and iterations must be positive; "
            "warmup and query memory limit cannot be negative"
        )
    manifest_path = args.manifest.resolve()
    manifest = json.loads(manifest_path.read_text())
    if not manifest or len({x["query_id"] for x in manifest}) != len(manifest):
        raise RuntimeError(
            "benchmark manifest must be non-empty and contain unique query IDs"
        )
    if manifest_path == (ROOT / "benchmark/manifest.json").resolve() and (
        len(manifest) != 64
        or [x["query_id"] for x in manifest] != [f"Q{i:03d}" for i in range(1, 65)]
    ):
        raise RuntimeError("baseline manifest must contain exactly Q001..Q064")
    query_count = len(manifest)
    for item in manifest:
        item["sql"] = (
            (manifest_path.parent / item["file"]).read_text().strip().replace("\n", " ")
        )
    metadata = json.loads((ROOT / "data/metadata.json").read_text())
    dataset = dataset_details(args.data, metadata)
    actual_sha = hashlib.sha256(Path(args.data).read_bytes()).hexdigest()
    expected_sha = dataset["sha256"]
    if actual_sha != expected_sha:
        raise RuntimeError("dataset SHA-256 does not match metadata")
    cmd_tail = [
        "bench-server",
        "--data",
        args.data,
        "--threads",
        str(args.threads),
        "--batch-size",
        str(args.batch_size),
        "--query-memory-limit-mb",
        str(args.query_memory_limit_mb),
    ]
    servers: list[Server] = []
    try:
        affinity = os.getenv("BENCH_CPUSET", "")
        prefix = (
            [shutil.which("taskset"), "-c", affinity]
            if affinity and platform.system() == "Linux" and shutil.which("taskset")
            else []
        )
        rust = Server("Rust", [*prefix, args.rust, *cmd_tail])
        servers.append(rust)
        cpp = Server("C++", [*prefix, args.cpp, *cmd_tail])
        servers.append(cpp)
        rust_cfg, cpp_cfg = rust.config(), cpp.config()
        if rust_cfg != cpp_cfg:
            raise RuntimeError(
                f"engine configuration differs: Rust={rust_cfg}, C++={cpp_cfg}"
            )
        cfg = rust_cfg
        if cfg != {
            "threads": args.threads,
            "batch_size": args.batch_size,
            "partitions": args.threads * 4,
            "rows": metadata["row_count"],
        }:
            raise RuntimeError(f"engine configuration does not match controller: {cfg}")
        cfg = {**cfg, "query_memory_limit_mb": args.query_memory_limit_mb}
        correctness = {}
        memory_accounting = {}
        print(f"Validating {query_count} queries...", flush=True)
        for index, item in enumerate(manifest):
            qid, sql = item["query_id"], item["sql"]
            rust.prepare(qid, sql)
            cpp.prepare(qid, sql)
            rust_plan, cpp_plan = rust.explain(qid), cpp.explain(qid)
            normalize = lambda plan: [x.split("(")[0] for x in plan]
            if normalize(rust_plan) != normalize(cpp_plan):
                raise RuntimeError(
                    f"{qid} physical operator mismatch: {rust_plan} != {cpp_plan}"
                )
            _, rr = rust.execute(qid, True)
            rust_memory = dict(rust.last_query_memory)
            _, cr = cpp.execute(qid, True)
            cpp_memory = dict(cpp.last_query_memory)
            if rust_memory["limit_bytes"] != cpp_memory["limit_bytes"]:
                raise RuntimeError(
                    f"{qid} query memory limits differ: "
                    f"Rust={rust_memory['limit_bytes']} C++={cpp_memory['limit_bytes']}"
                )
            ordered = "ORDER BY" in sql.upper()
            ok, detail = compare_rows(rr, cr, ordered)
            if not ok:
                print(
                    f"{qid} RESULT MISMATCH\n{detail}\nRust: {rr[:10]}\nC++: {cr[:10]}",
                    file=sys.stderr,
                )
                return 2
            correctness[qid] = {
                "correct": True,
                "row_count": len(rr),
                "result_hash": result_hash(rr, ordered),
            }
            memory_accounting[qid] = {"rust": rust_memory, "cpp": cpp_memory}
            print(f"  {qid} MATCH", flush=True)
        if args.validate_only:
            print(f"Correctness: {query_count} / {query_count} MATCH")
            return 0
        samples = {"rust": {}, "cpp": {}}
        rust_peak, cpp_peak = rust.rss_kib(), cpp.rss_kib()
        for qi, item in enumerate(manifest):
            qid, sql = item["query_id"], item["sql"]
            for iteration in range(args.warmup):
                order = (cpp, rust) if (qi + iteration) % 2 == 0 else (rust, cpp)
                for server in order:
                    server.execute(qid)
            ex = {"rust": [], "cpp": []}
            e2e = {"rust": [], "cpp": []}
            for iteration in range(args.iterations):
                order = (cpp, rust) if (qi + iteration) % 2 == 0 else (rust, cpp)
                for server in order:
                    ex["rust" if server is rust else "cpp"].append(
                        server.execute(qid)[0]
                    )
            for iteration in range(args.iterations):
                order = (rust, cpp) if (qi + iteration) % 2 == 0 else (cpp, rust)
                for server in order:
                    e2e["rust" if server is rust else "cpp"].append(
                        server.e2e(qid, sql)
                    )
            for name in ("rust", "cpp"):
                samples[name][qid] = {
                    **correctness[qid],
                    "execution": stats(ex[name]),
                    "end_to_end": stats(e2e[name]),
                }
            rust_peak = max(filter(None, (rust_peak, rust.rss_kib())), default=None)
            cpp_peak = max(filter(None, (cpp_peak, cpp.rss_kib())), default=None)
            print(f"Measured {qid} ({qi + 1}/{query_count})", flush=True)
        out = args.results_dir.resolve()
        out.mkdir(parents=True, exist_ok=True)
        common = {
            "configuration": cfg,
            "dataset": metadata,
            "input": dataset,
            "warmup": args.warmup,
            "iterations": args.iterations,
            "queries": None,
        }
        for name in ("rust", "cpp"):
            doc = dict(common)
            doc["engine"] = name
            doc["queries"] = [
                dict(
                    query_id=x["query_id"],
                    category=x["category"],
                    **samples[name][x["query_id"]],
                )
                for x in manifest
            ]
            (out / f"{name}.json").write_text(json.dumps(doc, indent=2) + "\n")
        comparisons = []
        wins = {"Rust": 0, "C++": 0, "TIE": 0}
        ratios = []
        for item in manifest:
            qid = item["query_id"]
            r = samples["rust"][qid]
            c = samples["cpp"][qid]
            ratio = r["execution"]["median_ns"] / c["execution"]["median_ns"]
            ratios.append(ratio)
            diff = abs(ratio - 1) * 100
            winner = (
                "TIE" if diff < args.tie_threshold else ("Rust" if ratio < 1 else "C++")
            )
            wins[winner] += 1
            comparisons.append(
                {
                    "query_id": qid,
                    "category": item["category"],
                    "correct": True,
                    "cpp_execution_median_ms": c["execution"]["median_ns"] / 1e6,
                    "rust_execution_median_ms": r["execution"]["median_ns"] / 1e6,
                    "cpp_execution_p95_ms": c["execution"]["p95_ns"] / 1e6,
                    "rust_execution_p95_ms": r["execution"]["p95_ns"] / 1e6,
                    "rust_cpp_ratio": ratio,
                    "winner": winner,
                    "difference_pct": diff,
                    "cpp_e2e_median_ms": c["end_to_end"]["median_ns"] / 1e6,
                    "rust_e2e_median_ms": r["end_to_end"]["median_ns"] / 1e6,
                    "result_hash": r["result_hash"],
                    "query_memory_limit_bytes": memory_accounting[qid]["rust"][
                        "limit_bytes"
                    ],
                    "rust_query_memory_accounted_bytes": memory_accounting[qid][
                        "rust"
                    ]["accounted_bytes"],
                    "cpp_query_memory_accounted_bytes": memory_accounting[qid]["cpp"][
                        "accounted_bytes"
                    ],
                }
            )
        geomean = math.exp(statistics.mean(math.log(x) for x in ratios))
        comparison_doc = {
            "configuration": cfg,
            "dataset": metadata,
            "input": {
                **dataset,
                "rust_load_time_ms": rust.load_ns / 1e6,
                "cpp_load_time_ms": cpp.load_ns / 1e6,
            },
            "tie_threshold_pct": args.tie_threshold,
            "cpp_wins": wins["C++"],
            "rust_wins": wins["Rust"],
            "ties": wins["TIE"],
            "rust_cpp_geometric_mean": geomean,
            "rust_peak_rss_kib": rust_peak,
            "cpp_peak_rss_kib": cpp_peak,
            "queries": comparisons,
        }
        (out / "comparison.json").write_text(
            json.dumps(comparison_doc, indent=2) + "\n"
        )
        with (out / "comparison.csv").open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=comparisons[0].keys())
            writer.writeheader()
            writer.writerows(comparisons)
        lines = ["=" * 60, "DREMEL-RS vs DREMEL-CPP", "=" * 60]
        for item, row in zip(manifest, comparisons):
            qid = row["query_id"]
            r = samples["rust"][qid]
            c = samples["cpp"][qid]
            wording = (
                f"Rust {(row['rust_cpp_ratio'] - 1) * 100:.2f}% slower"
                if row["rust_cpp_ratio"] >= 1
                else f"Rust {(1 - row['rust_cpp_ratio']) * 100:.2f}% faster"
            )
            lines += [
                "",
                "-" * 60,
                f"{qid}  {item['category']}",
                item["sql"],
                "",
                "C++ Dremel",
                f"  execution median : {c['execution']['median_ns'] / 1e6:.3f} ms",
                f"  execution p95    : {c['execution']['p95_ns'] / 1e6:.3f} ms",
                f"  execution mean   : {c['execution']['mean_ns'] / 1e6:.3f} ms",
                f"  execution stddev : {c['execution']['stddev_ns'] / 1e6:.3f} ms",
                f"  end-to-end median: {c['end_to_end']['median_ns'] / 1e6:.3f} ms",
                "",
                "Rust Dremel",
                f"  execution median : {r['execution']['median_ns'] / 1e6:.3f} ms",
                f"  execution p95    : {r['execution']['p95_ns'] / 1e6:.3f} ms",
                f"  execution mean   : {r['execution']['mean_ns'] / 1e6:.3f} ms",
                f"  execution stddev : {r['execution']['stddev_ns'] / 1e6:.3f} ms",
                f"  end-to-end median: {r['end_to_end']['median_ns'] / 1e6:.3f} ms",
                "",
                f"Primary winner : {row['winner']}",
                f"Difference     : {wording}",
                f"Rust/C++ ratio : {row['rust_cpp_ratio']:.4f}x",
                "Correctness    : MATCH",
                f"Rows returned  : {r['row_count']}",
                f"Result hash    : {r['result_hash']}",
                f"C++ memory    : {row['cpp_query_memory_accounted_bytes']} accounted bytes",
                f"Rust memory   : {row['rust_query_memory_accounted_bytes']} accounted bytes",
            ]
        interpretation = (
            f"Rust median latency was approximately {(geomean - 1) * 100:.2f}% higher"
            if geomean >= 1
            else f"Rust median latency was approximately {(1 - geomean) * 100:.2f}% lower"
        )
        lines += [
            "",
            "=" * 60,
            "SUMMARY",
            "=" * 60,
            "",
            f"Dataset rows:         {metadata['row_count']:,}",
            f"Dataset format:       {dataset['format']}",
            f"Dataset hash:         {expected_sha}",
            f"Queries tested:       {query_count}",
            f"Queries correct:      {query_count} / {query_count}",
            f"Threads:              {cfg['threads']}",
            f"Batch size:           {cfg['batch_size']}",
            f"Partitions:           {cfg['partitions']}",
            "Query memory cap:     "
            + (
                f"{args.query_memory_limit_mb} MiB"
                if args.query_memory_limit_mb
                else "unlimited"
            ),
            f"Warmups:              {args.warmup}",
            f"Iterations:           {args.iterations}",
            f"LTO:                  {'enabled' if os.getenv('LTO', '0') == '1' else 'disabled'}",
            f"Native tuning:        {'enabled' if os.getenv('NATIVE', '0') == '1' else 'disabled'}",
            f"C++ wins:             {wins['C++']}",
            f"Rust wins:            {wins['Rust']}",
            f"Ties:                 {wins['TIE']}",
            f"Rust/C++ geometric mean: {geomean:.4f}x",
            f"Rust peak RSS:        {rust_peak or 'unsupported'} KiB",
            f"C++ peak RSS:         {cpp_peak or 'unsupported'} KiB",
            "",
            "Interpretation:",
            f"On this machine and workload, {interpretation}. This compares these",
            "equivalent educational implementations, not Rust and C++ universally.",
        ]
        report = "\n".join(lines) + "\n"
        (out / "report.txt").write_text(report)
        print(report)
        (out / "environment.txt").write_text(
            environment_text(cfg, metadata, rust, cpp, args, rust_peak, cpp_peak)
        )
        return 0
    finally:
        for server in reversed(servers):
            server.close()


if __name__ == "__main__":
    raise SystemExit(main())

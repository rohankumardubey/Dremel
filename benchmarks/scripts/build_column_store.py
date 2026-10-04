#!/usr/bin/env python3
"""Convert the canonical CSV into the shared DREMCOL1 binary column store."""

from __future__ import annotations

import argparse
import array
import csv
import hashlib
import json
import struct
import sys
from pathlib import Path

MAGIC = b"DREMCOL1"
VERSION = 1


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def write_array(handle, code: str, values) -> None:
    data = array.array(code, values)
    if sys.byteorder != "little":
        data.byteswap()
    data.tofile(handle)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=Path, default=Path("data/events.csv"))
    parser.add_argument("--output", type=Path, default=Path("data/events.dremel"))
    parser.add_argument("--metadata", type=Path, default=Path("data/metadata.json"))
    args = parser.parse_args()

    metadata = json.loads(args.metadata.read_text())
    expected_rows = metadata["row_count"]
    if (
        args.output.exists()
        and metadata.get("column_store_version") == VERSION
        and metadata.get("column_store_sha256") == digest(args.output)
    ):
        print(f"column store current: {args.output}")
        return

    column_names = (
        "event_id",
        "user_id",
        "timestamp",
        "duration_ms",
        "bytes",
        "score",
        "success",
        "campaign_id",
        "campaign_def",
    )
    columns = {name: [] for name in column_names}
    dictionaries = {name: ({}, [], []) for name in ("country", "device", "event_type")}

    with args.csv.open(newline="") as f:
        for row in csv.DictReader(f):
            for name in ("event_id", "user_id", "timestamp", "duration_ms", "bytes"):
                columns[name].append(int(row[name]))
            columns["score"].append(float(row["score"]))
            columns["success"].append(int(row["success"] == "true"))
            columns["campaign_def"].append(int(bool(row["campaign_id"])))
            columns["campaign_id"].append(int(row["campaign_id"] or 0))
            for name, (ids, values, encoded) in dictionaries.items():
                if row[name] not in ids:
                    ids[row[name]] = len(values)
                    values.append(row[name])
                encoded.append(ids[row[name]])

    if len(columns["event_id"]) != expected_rows:
        raise SystemExit("CSV row count differs from metadata")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("wb") as f:
        f.write(MAGIC)
        f.write(struct.pack("<IQ", VERSION, expected_rows))
        for name in ("event_id", "user_id", "timestamp"):
            write_array(f, "q", columns[name])
        for name in ("country", "device", "event_type"):
            _, values, encoded = dictionaries[name]
            f.write(struct.pack("<I", len(values)))
            for value in values:
                raw = value.encode()
                f.write(struct.pack("<I", len(raw)))
                f.write(raw)
            write_array(f, "I", encoded)
        write_array(f, "q", columns["duration_ms"])
        write_array(f, "q", columns["bytes"])
        write_array(f, "d", columns["score"])
        write_array(f, "B", columns["success"])
        write_array(f, "q", columns["campaign_id"])
        write_array(f, "B", columns["campaign_def"])

    metadata["column_store_version"] = VERSION
    metadata["column_store_sha256"] = digest(args.output)
    metadata["column_store_file"] = args.output.name
    args.metadata.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    print(f"built {args.output}: sha256={metadata['column_store_sha256']}")


if __name__ == "__main__":
    main()

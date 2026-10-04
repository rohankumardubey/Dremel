#!/usr/bin/env python3
"""Build deterministic Arrow IPC and Parquet datasets with official PyArrow."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv
import pyarrow.ipc as ipc
import pyarrow.parquet as parquet

VERSION = 1
DICTIONARY_COLUMNS = {
    "events": ("country", "device", "event_type"),
    "users": ("segment", "region"),
    "campaigns": ("channel",),
}
SCHEMAS = {
    "events": pa.schema(
        [
            ("event_id", pa.int64()),
            ("user_id", pa.int64()),
            ("timestamp", pa.int64()),
            ("country", pa.string()),
            ("device", pa.string()),
            ("event_type", pa.string()),
            ("duration_ms", pa.int64()),
            ("bytes", pa.int64()),
            ("score", pa.float64()),
            ("success", pa.bool_()),
            ("campaign_id", pa.int64()),
        ]
    ),
    "users": pa.schema(
        [
            ("user_id", pa.int64()),
            ("segment", pa.string()),
            ("signup_date", pa.date32()),
            ("lifetime_value", pa.decimal128(18, 2)),
            ("region", pa.string()),
            ("active", pa.bool_()),
        ]
    ),
    "campaigns": pa.schema(
        [
            ("campaign_id", pa.int64()),
            ("campaign_name", pa.string()),
            ("budget", pa.decimal128(18, 2)),
            ("start_date", pa.date32()),
            ("end_date", pa.date32()),
            ("channel", pa.string()),
        ]
    ),
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def source_hashes(directory: Path) -> dict[str, str]:
    return {
        f"{name}.csv": sha256(directory / f"{name}.csv")
        for name in ("events", "users", "campaigns")
    }


def outputs_current(metadata: dict[str, object], directory: Path, row_group_size: int) -> bool:
    state = metadata.get("interoperable")
    if not isinstance(state, dict):
        return False
    if (
        state.get("version") != VERSION
        or state.get("pyarrow_version") != pa.__version__
        or state.get("row_group_size") != row_group_size
        or state.get("source_sha256") != source_hashes(directory)
    ):
        return False
    files = state.get("files")
    if not isinstance(files, dict):
        return False
    for name, details in files.items():
        if not isinstance(name, str) or not isinstance(details, dict):
            return False
        path = directory / name
        if not path.exists() or details.get("sha256") != sha256(path):
            return False
    return bool(files)


def read_table(name: str, path: Path) -> pa.Table:
    schema = SCHEMAS[name]
    table = csv.read_csv(
        path,
        convert_options=csv.ConvertOptions(
            column_types={field.name: field.type for field in schema},
            null_values=[""],
            strings_can_be_null=True,
        ),
    )
    for column_name in DICTIONARY_COLUMNS[name]:
        index = table.schema.get_field_index(column_name)
        encoded = pc.dictionary_encode(table.column(index).combine_chunks())
        table = table.set_column(index, column_name, encoded)
    return table.replace_schema_metadata(
        {
            b"dremel.schema_version": b"1",
            b"dremel.table": name.encode(),
        }
    )


def write_table(
    name: str,
    table: pa.Table,
    directory: Path,
    row_group_size: int,
) -> list[Path]:
    arrow_path = directory / f"{name}.arrow"
    with pa.OSFile(str(arrow_path), "wb") as sink:
        with ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table, max_chunksize=row_group_size)

    paths = [arrow_path]
    for compression in ("snappy", "zstd"):
        path = directory / f"{name}-{compression}.parquet"
        parquet.write_table(
            table,
            str(path),
            compression=compression,
            row_group_size=row_group_size,
            use_dictionary=list(DICTIONARY_COLUMNS[name]),
            write_statistics=True,
            version="2.6",
        )
        paths.append(path)
    return paths


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, default=Path("data"))
    parser.add_argument("--metadata", type=Path, default=Path("data/metadata.json"))
    parser.add_argument("--row-group-size", type=int, default=65_536)
    args = parser.parse_args()
    if args.row_group_size <= 0:
        parser.error("--row-group-size must be positive")

    metadata = json.loads(args.metadata.read_text())
    if outputs_current(metadata, args.directory, args.row_group_size):
        print(f"Arrow and Parquet datasets current: PyArrow {pa.__version__}")
        return

    output_paths: list[Path] = []
    row_counts: dict[str, int] = {}
    for name in ("events", "users", "campaigns"):
        source = args.directory / f"{name}.csv"
        table = read_table(name, source)
        row_counts[name] = table.num_rows
        output_paths.extend(write_table(name, table, args.directory, args.row_group_size))

    files = {
        path.name: {"bytes": path.stat().st_size, "sha256": sha256(path)}
        for path in output_paths
    }
    metadata["interoperable"] = {
        "version": VERSION,
        "pyarrow_version": pa.__version__,
        "row_group_size": args.row_group_size,
        "row_counts": row_counts,
        "source_sha256": source_hashes(args.directory),
        "files": files,
    }
    args.metadata.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    print(
        f"built {len(output_paths)} Arrow/Parquet files with PyArrow {pa.__version__}"
    )


if __name__ == "__main__":
    main()

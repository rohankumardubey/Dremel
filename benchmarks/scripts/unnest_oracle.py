"""Independent row expansion from PyArrow values, never from engine output."""
from __future__ import annotations

import pyarrow as pa

from run_nested_sql_benchmark import typed


def elements(value, left=False, predicate=lambda value: True):
    matches = [(item, index + 1) for index, item in enumerate(value or []) if predicate(item)]
    return matches or ([(None, None)] if left else [])


def flat_tables(records):
    layouts = {
        "samples": [("record_id", pa.int64()), ("value", pa.int64()), ("pos", pa.int64())],
        "samples_left": [("record_id", pa.int64()), ("value", pa.int64()), ("pos", pa.int64())],
        "samples_on": [("record_id", pa.int64()), ("value", pa.int64()), ("pos", pa.int64())],
        "items": [("record_id", pa.int64()), ("price", pa.int64()), ("name", pa.string()), ("pos", pa.int64())],
        "tags": [("record_id", pa.int64()), ("item_pos", pa.int64()), ("tag", pa.string()), ("pos", pa.int64())],
        "matrix": [("record_id", pa.int64()), ("outer_pos", pa.int64()), ("value", pa.int32()), ("pos", pa.int64())],
        "matrix_left": [("record_id", pa.int64()), ("outer_pos", pa.int64()), ("value", pa.int32()), ("pos", pa.int64())],
        "large": [("record_id", pa.int64()), ("value", pa.int64()), ("pos", pa.int64())],
        "fixed": [("record_id", pa.int64()), ("value", pa.int32()), ("pos", pa.int64())],
        "product": [("record_id", pa.int64()), ("value", pa.int64()), ("price", pa.int64())],
    }
    rows = {name: [] for name in layouts}
    for record in records:
        rid = record["record_id"]
        for name, left, predicate in [("samples", False, lambda v: True), ("samples_left", True, lambda v: True),
                ("samples_on", True, lambda v: v is not None and v > 2)]:
            rows[name].extend((rid, value, pos) for value, pos in elements(record["samples"], left, predicate))
        for item, pos in elements(record["items"]):
            price, name, tags = (None, None, None) if item is None else (item["price"], item["name"], item["tags"])
            rows["items"].append((rid, price, name, pos))
            rows["tags"].extend((rid, pos, tag, index) for tag, index in elements(tags, left=True))
        for key, left in [("matrix", False), ("matrix_left", True)]:
            for inner, outer_pos in elements(record["matrix"], left):
                rows[key].extend((rid, outer_pos, value, pos) for value, pos in elements(inner, left))
        rows["large"].extend((rid, value, pos) for value, pos in elements(record["large_values"]))
        rows["fixed"].extend((rid, value, pos) for value, pos in elements(record["fixed_values"]))
        for value, _ in elements(record["samples"]):
            rows["product"].extend((rid, value, None if item is None else item["price"]) for item, _ in elements(record["items"]))
    return {key: pa.Table.from_arrays([pa.array([row[index] for row in rows[key]], type=kind)
            for index, (_, kind) in enumerate(layout)], names=[name for name, _ in layout]) for key, layout in layouts.items()}


def containers(records, kind):
    result = []
    for record in records:
        key = "items" if kind == "struct" else "matrix"
        for value, pos in elements(record[key]):
            result.append([typed(record["record_id"]), typed(value), typed(pos)])
            if len(result) == 25:
                return result
    return result

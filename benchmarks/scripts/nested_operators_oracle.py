"""Independent structural-value oracle over official PyArrow Python values."""
from collections import defaultdict
from functools import cmp_to_key

from run_nested_sql_benchmark import typed


def order_key(value):
    if value is None:
        return (0,)
    if isinstance(value, dict):
        return (1, tuple(order_key(child) for child in value.values()))
    if isinstance(value, list):
        return (2, tuple(order_key(child) for child in value))
    return (3, value)


def compare(left, right):
    if left is None or right is None:
        return None
    a, b = order_key(left), order_key(right)
    return (a > b) - (a < b)


def group_rows(values, having=False):
    groups = defaultdict(list)
    for value in values:
        groups[order_key(value)].append(value)
    return [[items[0], len(items)] for _, items in sorted(groups.items())
            if not having or (items[0] is not None and len(items) > 1)]


def expected(item, records):
    field, action = item["field"], item["action"]
    values = [row[field] for row in records]
    if action in ("group", "having"):
        result = group_rows(values, action == "having")
    elif action == "element_group":
        result = group_rows([element for collection in values if collection is not None for element in collection])
    elif action == "distinct":
        result = [[value] for value, _ in group_rows(values)]
    elif action == "sort":
        def ordering(left, right):
            a, b = left[field], right[field]
            if a is None or b is None:
                comparison = (a is not None) - (b is not None)
            else:
                comparison = compare(a, b) * (-1 if item.get("descending") else 1)
            return comparison or (left["record_id"] > right["record_id"]) - (left["record_id"] < right["record_id"])
        result = [[row["record_id"], row[field]] for row in sorted(records, key=cmp_to_key(ordering))[3:28]]
    elif action == "extrema":
        visible = [value for value in values if value is not None]
        result = [[min(visible, key=order_key) if visible else None,
                   max(visible, key=order_key) if visible else None, len(visible)]]
    elif action == "filter":
        desired = -1 if item.get("operator") == "<" else 0
        result = [[sum(compare(row[field], row[item["other"]]) == desired for row in records)]]
    elif action == "membership":
        result = []
        for row in records[:25]:
            match = compare(row[field], row[item["other"]]) == 0
            result.append([row["record_id"], True if match else None, False if match else None])
    elif action == "nullif":
        result = [[row["record_id"], None if compare(row[field], row[item["other"]]) == 0 else row[field]] for row in records[:25]]
    elif action == "between":
        result = [[row["record_id"], None if row[field] is None else True] for row in records[:25]]
    elif action == "join":
        total, matched = 0, 0
        for collection in values:
            for left in collection or []:
                matches = sum(compare(left, right) == 0 for right in collection)
                total += max(1, matches)
                matched += matches
        result = [[total, matched]]
    else:
        raise ValueError(action)
    return [[typed(value) for value in row] for row in result]


def rank_records(records):
    """Order-preserving precomputed scalar keys, not a general flattening engine."""
    families = [("profile", "profile2"), ("samples", "samples2")]
    ranks = {}
    for fields in families:
        keys = sorted({order_key(row[field]) for row in records for field in fields if row[field] is not None})
        for field in fields:
            ranks[field] = {key: rank for rank, key in enumerate(keys)}
    return [{"record_id": row["record_id"], **{
        field: None if row[field] is None else mapping[order_key(row[field])]
        for field, mapping in ranks.items()}} for row in records]

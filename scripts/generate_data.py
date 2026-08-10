#!/usr/bin/env python3
"""Generate the one shared, deterministic events dataset."""

from __future__ import annotations

import argparse
import csv
import datetime
import hashlib
import json
from pathlib import Path

VERSION = 1
SCHEMA_VERSION = 1
DIMENSION_VERSION = 1
MASK = (1 << 64) - 1
COUNTRIES = ("IN", "US", "GB", "DE", "FR", "JP", "BR", "CA", "AU", "SG")
DEVICES = ("mobile", "desktop", "tablet")
EVENTS = ("view", "click", "search", "purchase", "login", "logout", "download", "share")
SEGMENTS = ("free", "plus", "pro", "enterprise")
REGIONS = ("apac", "amer", "emea")
CHANNELS = ("search", "social", "display", "email", "partner")


class SplitMix64:
    def __init__(self, seed: int) -> None:
        self.state = seed & MASK

    def next(self) -> int:
        self.state = (self.state + 0x9E3779B97F4A7C15) & MASK
        z = self.state
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK
        return (z ^ (z >> 31)) & MASK


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def generate_dimensions(directory: Path, seed: int) -> dict[str, object]:
    """Generate deterministic dimension tables used only by production-v1."""
    users = directory / "users.csv"
    campaigns = directory / "campaigns.csv"
    epoch = datetime.date(2020, 1, 1)

    rng = SplitMix64(seed ^ 0x55534552535F5631)
    with users.open("w", newline="") as f:
        out = csv.writer(f, lineterminator="\n")
        out.writerow(
            ("user_id", "segment", "signup_date", "lifetime_value", "region", "active")
        )
        for user_id in range(1, 250_001):
            segment = SEGMENTS[rng.next() % len(SEGMENTS)]
            signup_date = epoch + datetime.timedelta(days=rng.next() % 1461)
            lifetime_cents = rng.next() % 2_500_001
            region = REGIONS[rng.next() % len(REGIONS)]
            active = "true" if rng.next() % 10 < 8 else "false"
            out.writerow(
                (
                    user_id,
                    segment,
                    signup_date.isoformat(),
                    f"{lifetime_cents // 100}.{lifetime_cents % 100:02d}",
                    region,
                    active,
                )
            )

    rng = SplitMix64(seed ^ 0x43414D504149474E)
    with campaigns.open("w", newline="") as f:
        out = csv.writer(f, lineterminator="\n")
        out.writerow(
            (
                "campaign_id",
                "campaign_name",
                "budget",
                "start_date",
                "end_date",
                "channel",
            )
        )
        campaign_epoch = datetime.date(2023, 1, 1)
        for campaign_id in range(1, 5_001):
            start = campaign_epoch + datetime.timedelta(days=rng.next() % 900)
            end = start + datetime.timedelta(days=1 + rng.next() % 120)
            budget_cents = 10_000 + rng.next() % 50_000_001
            channel = CHANNELS[rng.next() % len(CHANNELS)]
            out.writerow(
                (
                    campaign_id,
                    f"campaign-{campaign_id:04d}",
                    f"{budget_cents // 100}.{budget_cents % 100:02d}",
                    start.isoformat(),
                    end.isoformat(),
                    channel,
                )
            )

    return {
        "dimension_generation_version": DIMENSION_VERSION,
        "users_file": users.name,
        "users_row_count": 250_000,
        "users_sha256": sha256(users),
        "campaigns_file": campaigns.name,
        "campaigns_row_count": 5_000,
        "campaigns_sha256": sha256(campaigns),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--seed", default="0xD3E3A5E1")
    parser.add_argument("--output", type=Path, default=Path("data/events.csv"))
    parser.add_argument("--metadata", type=Path, default=Path("data/metadata.json"))
    args = parser.parse_args()
    seed = int(args.seed, 0)
    if args.rows < 0:
        parser.error("--rows must be non-negative")
    old: dict[str, object] = {}
    if args.output.exists() and args.metadata.exists():
        try:
            old = json.loads(args.metadata.read_text())
            if (
                old.get("row_count") == args.rows
                and old.get("seed") == seed
                and old.get("generation_version") == VERSION
                and old.get("schema_version") == SCHEMA_VERSION
                and old.get("csv_sha256") == sha256(args.output)
            ):
                users = args.output.parent / "users.csv"
                campaigns = args.output.parent / "campaigns.csv"
                dimensions_current = (
                    old.get("dimension_generation_version") == DIMENSION_VERSION
                    and users.exists()
                    and campaigns.exists()
                    and old.get("users_sha256") == sha256(users)
                    and old.get("campaigns_sha256") == sha256(campaigns)
                )
                if not dimensions_current:
                    old.update(generate_dimensions(args.output.parent, seed))
                    args.metadata.write_text(
                        json.dumps(old, indent=2, sort_keys=True) + "\n"
                    )
                    print("generated production-v1 dimension tables")
                print(f"dataset current: {args.output} ({args.rows} rows)")
                return
        except (OSError, ValueError, json.JSONDecodeError):
            pass
    args.output.parent.mkdir(parents=True, exist_ok=True)
    rng = SplitMix64(seed)
    with args.output.open("w", newline="") as f:
        out = csv.writer(f, lineterminator="\n")
        out.writerow(
            (
                "event_id",
                "user_id",
                "timestamp",
                "country",
                "device",
                "event_type",
                "duration_ms",
                "bytes",
                "score",
                "success",
                "campaign_id",
            )
        )
        for event_id in range(1, args.rows + 1):
            user_id = rng.next() % 250_000 + 1
            timestamp = 1_704_067_200 + rng.next() % 31_536_000
            country = COUNTRIES[rng.next() % len(COUNTRIES)]
            device = DEVICES[rng.next() % len(DEVICES)]
            event_type = EVENTS[rng.next() % len(EVENTS)]
            duration_ms = rng.next() % 10_001
            byte_count = rng.next() % 1_000_001
            score = (rng.next() % 1_000_001) / 10_000.0
            success = "true" if rng.next() & 1 else "false"
            raw_campaign = rng.next()
            campaign = "" if raw_campaign % 10 < 3 else raw_campaign % 5_000 + 1
            out.writerow(
                (
                    event_id,
                    user_id,
                    timestamp,
                    country,
                    device,
                    event_type,
                    duration_ms,
                    byte_count,
                    f"{score:.4f}",
                    success,
                    campaign,
                )
            )
    digest = sha256(args.output)
    metadata = {
        "row_count": args.rows,
        "seed": seed,
        "schema_version": SCHEMA_VERSION,
        "generation_version": VERSION,
        "csv_sha256": digest,
    }
    metadata.update(generate_dimensions(args.output.parent, seed))
    args.metadata.parent.mkdir(parents=True, exist_ok=True)
    args.metadata.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    print(f"generated {args.output}: {args.rows} rows, sha256={digest}")


if __name__ == "__main__":
    main()

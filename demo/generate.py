#!/usr/bin/env python3
"""Generates a sample CSV dataset for the stats-cruncher demo."""

import csv
import random
import sys
from datetime import datetime, timezone, timedelta

ROWS = 5000_000
COUNTRIES = ["DE", "US", "FR", "PL", "GB", "JP", "BR", "CA", "AU", "NL"]
EVENT_TYPES = ["purchase"] * 20 + ["refund"]

def random_dt(start: datetime, end: datetime) -> str:
    delta = end - start
    offset = timedelta(seconds=random.randint(0, int(delta.total_seconds())))
    return (start + offset).strftime("%Y-%m-%dT%H:%M:%SZ")

def main(output_path: str) -> None:
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end   = datetime(2026, 1, 1, tzinfo=timezone.utc)

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "amount", "country", "event_type", "occurred_at"])
        for i in range(1, ROWS + 1):
            writer.writerow([
                random.randint(1, 50_000),
                round(random.uniform(0.01, 999.99), 2),
                random.choice(COUNTRIES),
                random.choice(EVENT_TYPES),
                random_dt(start, end),
            ])

    print(f"Generated {ROWS:,} rows → {output_path}", flush=True)

if __name__ == "__main__":
    import os
    path = sys.argv[1] if len(sys.argv) > 1 else "/data/sample.csv"
    if os.path.exists(path):
        print(f"Data already exists at {path}, skipping generation.", flush=True)
    else:
        main(path)

#!/usr/bin/env python3
"""Generate deterministic synthetic log fixtures for benchmarking.

Usage example:
    python3 scripts/gen_fixture.py --rows 1000 --seed 42 --out fixtures/basic.csv
"""

import argparse
import csv
import io
import os
import random
from datetime import datetime, timedelta

EVENT_IDS = [4624, 4625, 4688, 4103]
EVENT_WEIGHTS = [50, 20, 20, 10]
HOSTS = [f"WS{i:02d}" for i in range(1, 51)]
USERS = ["alice", "bob", "carol", "dave", "eve", "frank"]
PROCS = ["powershell.exe", "cmd.exe", "chrome.exe", "explorer.exe"]
PATHS = [
    "C:\\Windows\\System32\\",
    "C:\\Windows\\SysWOW64\\",
    "C:\\Program Files\\Common Files\\",
    "C:\\Users\\Public\\",
    "C:\\Temp\\",
    "C:\\ProgramData\\",
]
WIDE_VALUES = [f"x{i:02d}" for i in range(1, 51)]


def format_ts(moment, index):
    """Alternate millisecond and nanosecond (9-digit) fractional seconds."""
    base = moment.strftime("%Y-%m-%dT%H:%M:%S")
    if index % 2 == 0:
        return f"{base}.{moment.microsecond // 1000:03d}"
    return f"{base}.{moment.microsecond * 1000:09d}"


def build_rows(rows, seed, variant):
    rng = random.Random(seed)
    current = datetime(2024, 1, 1, 0, 0, 0)
    gap_positions = {rows // 3, rows * 2 // 3}
    wide = variant == "wide"
    result = []

    for index in range(rows):
        if index in gap_positions:
            current += timedelta(minutes=37)
        current += timedelta(seconds=rng.randint(1, 10))

        event_id = rng.choices(EVENT_IDS, weights=EVENT_WEIGHTS, k=1)[0]
        host = rng.choice(HOSTS)

        if result and rng.random() < 0.01:
            user = result[-1]["user"]
            proc = result[-1]["proc"]
        else:
            user = rng.choice(USERS)
            proc = rng.choice(PROCS)

        path = rng.choice(PATHS)

        if rng.random() < 0.2:
            msg = f'event={event_id} note="value,{index}"'
        else:
            msg = f"event={event_id} proc={proc}"

        row = {
            "ts": format_ts(current, index),
            "EventID": str(event_id),
            "host": host,
            "user": user,
            "proc": proc,
            "path": path,
            "msg": msg,
        }
        if wide:
            for n in range(1, 13):
                row[f"extra{n:02d}"] = rng.choice(WIDE_VALUES)
        result.append(row)

    return result


def write_output(path, rows, variant):
    wide = variant == "wide"
    header = ["ts", "EventID", "host", "user", "proc", "path", "msg"]
    if wide:
        header.extend(f"extra{n:02d}" for n in range(1, 13))

    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    writer.writerow(header)
    for row in rows:
        writer.writerow([row[column] for column in header])

    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)

    payload = buffer.getvalue()
    if variant == "utf16":
        with open(path, "wb") as handle:
            handle.write(b"\xff\xfe")
            handle.write(payload.encode("utf-16-le"))
    else:
        with open(path, "w", encoding="utf-8", newline="") as handle:
            handle.write(payload)


def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic log fixtures.")
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", required=True)
    parser.add_argument(
        "--variant",
        choices=["ascii", "utf16", "wide"],
        default="ascii",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    rows = build_rows(args.rows, args.seed, args.variant)
    write_output(args.out, rows, args.variant)


if __name__ == "__main__":
    main()

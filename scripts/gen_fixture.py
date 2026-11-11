#!/usr/bin/env python3
"""Generate deterministic synthetic log fixtures for benchmarking.

Usage example:
    python3 scripts/gen_fixture.py --rows 1000 --seed 42 --out fixtures/basic.csv

Variants:
    ascii   7 columns (default)
    utf16   same records, UTF-16LE with a BOM
    wide    19 columns (7 base + 12 short extras)
    long    120 columns, about 1 KiB of text cells per record (3 long
            ~160-character cells plus 110 short extras). Intended for the
            T27 "many columns, long cells" measurement.
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

BASE_COLUMNS = ["ts", "EventID", "host", "user", "proc", "path", "msg"]
LONG_DETAIL_COLUMNS = ["detail01", "detail02", "detail03"]
LONG_EXTRA_COLUMNS = [f"extra{i:03d}" for i in range(1, 111)]
LONG_SHORT_VALUES = [f"v{i:03d}" for i in range(1, 201)]
LONG_TEXT = (
    "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod "
    "tempor incididunt ut labore et dolore magna aliqua"
)


def long_cell(index, slot, width=160):
    """Deterministic long text cell, unique per row and slot."""
    prefix = f"row={index} part={slot} "
    body = (LONG_TEXT + " ") * 3
    return (prefix + body)[:width]


def format_ts(moment, index):
    """Alternate millisecond and nanosecond (9-digit) fractional seconds."""
    base = moment.strftime("%Y-%m-%dT%H:%M:%S")
    if index % 2 == 0:
        return f"{base}.{moment.microsecond // 1000:03d}"
    return f"{base}.{moment.microsecond * 1000:09d}"


def iter_rows(rows, seed, variant):
    """Yield rows one at a time so large fixtures never sit in memory."""
    rng = random.Random(seed)
    current = datetime(2024, 1, 1, 0, 0, 0)
    gap_positions = {rows // 3, rows * 2 // 3}
    wide = variant == "wide"
    long = variant == "long"
    previous = None

    for index in range(rows):
        if index in gap_positions:
            current += timedelta(minutes=37)
        current += timedelta(seconds=rng.randint(1, 10))

        event_id = rng.choices(EVENT_IDS, weights=EVENT_WEIGHTS, k=1)[0]
        host = rng.choice(HOSTS)

        if previous is not None and rng.random() < 0.01:
            user = previous["user"]
            proc = previous["proc"]
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
        if long:
            for slot, column in enumerate(LONG_DETAIL_COLUMNS, start=1):
                row[column] = long_cell(index, slot)
            for column in LONG_EXTRA_COLUMNS:
                row[column] = rng.choice(LONG_SHORT_VALUES)
        previous = row
        yield row


def header_for(variant):
    header = list(BASE_COLUMNS)
    if variant == "wide":
        header.extend(f"extra{n:02d}" for n in range(1, 13))
    if variant == "long":
        header.extend(LONG_DETAIL_COLUMNS)
        header.extend(LONG_EXTRA_COLUMNS)
    return header


def write_output(path, rows, variant):
    header = header_for(variant)

    def write(text):
        writer = csv.writer(text)
        writer.writerow(header)
        for row in rows:
            writer.writerow([row[column] for column in header])

    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)

    if variant == "utf16":
        with open(path, "wb") as handle:
            handle.write(b"\xff\xfe")
            with io.TextIOWrapper(handle, encoding="utf-16-le", newline="") as text:
                write(text)
    else:
        with open(path, "w", encoding="utf-8", newline="") as handle:
            write(handle)


def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic log fixtures.")
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", required=True)
    parser.add_argument(
        "--variant",
        choices=["ascii", "utf16", "wide", "long"],
        default="ascii",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    write_output(args.out, iter_rows(args.rows, args.seed, args.variant), args.variant)


if __name__ == "__main__":
    main()

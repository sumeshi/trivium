#!/usr/bin/env python3
"""Read architecture_bench JSONL (legacy debug lines are ignored)."""
import argparse
import json
from pathlib import Path


def read(path):
    fixture = None
    records = {}
    for line in Path(path).read_text().splitlines():
        if not line.startswith("{"):
            continue
        value = json.loads(line)
        if "fixture" in value:
            fixture = Path(value["fixture"]).name
            records[fixture] = {"header": value, "operations": {}}
        elif "rows" in value:
            records[fixture]["shape"] = value
        elif "operation" in value:
            records[fixture]["operations"][value["operation"]] = value
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", type=Path)
    args = parser.parse_args()
    builds = [(path.stem, read(path)) for path in args.logs]
    for fixture in builds[0][1]:
        print(f"\n{fixture}\n")
        print("| operation (ms) | " + " | ".join(label for label, _ in builds) + " |")
        print("|---|" + "---:|" * len(builds))
        operations = builds[0][1][fixture]["operations"]
        for operation in operations:
            if operation == "complete":
                continue
            medians = [records[fixture]["operations"][operation]["median_ms"] for _, records in builds]
            print(f"| {operation} | " + " | ".join(f"{value:.2f}" for value in medians) + " |")
        peaks = [records[fixture]["operations"]["complete"]["peak_rss_kib"] / 1024 for _, records in builds]
        print("| VmHWM (MiB) | " + " | ".join(f"{value:.1f}" for value in peaks) + " |")


if __name__ == "__main__":
    main()

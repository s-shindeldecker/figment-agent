#!/usr/bin/env python3
"""
Print Looker CSV header row (after Name_ae / Name_csm deduplication) for aligning
config/e100_output_columns.yaml with a live export.
"""

import argparse
import csv
import sys
from pathlib import Path


def _dedupe_name_headers(raw_headers: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for col in raw_headers:
        if col == "Name":
            count = seen.get(col, 0)
            out.append("Name_ae" if count == 0 else "Name_csm")
            seen[col] = count + 1
        else:
            out.append(col)
            seen[col] = seen.get(col, 0) + 1
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("csv_path", type=Path, help="Path to Looker CSV export")
    args = p.parse_args()
    path = args.csv_path
    if not path.is_file():
        print(f"Not a file: {path}", file=sys.stderr)
        return 1
    with open(path, newline="", encoding="utf-8-sig") as f:
        raw = next(csv.reader(f))
    deduped = _dedupe_name_headers(raw)
    for i, h in enumerate(deduped, start=1):
        print(f"{i:3}\t{h}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

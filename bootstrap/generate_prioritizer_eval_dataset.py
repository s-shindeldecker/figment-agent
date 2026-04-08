#!/usr/bin/env python3
"""
Build LaunchDarkly offline-evaluation dataset rows (JSONL) from account JSON files.

Each input file must contain a JSON **array** of account objects (same shape as
``AccountRecord`` fields). The script emits one JSONL object per file:

  {"input": "<exact user message from prioritize_with_ai_config>", ...}

Use ``input`` as the dataset column when your AI Config already supplies the agent
instructions (system prompt). See docs/prioritizer-offline-eval.md.

Usage:
  python bootstrap/generate_prioritizer_eval_dataset.py accounts_a.json accounts_b.json \\
    > my_eval.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import fields
from pathlib import Path
from typing import Any, Dict, List

# Allow ``python bootstrap/generate_prioritizer_eval_dataset.py`` from repo root
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from agents.prioritizer import build_prioritizer_user_message  # noqa: E402
from core.schema import AccountRecord  # noqa: E402


def _account_records_from_json(rows: List[Dict[str, Any]]) -> List[AccountRecord]:
    names = {f.name for f in fields(AccountRecord)}
    out: List[AccountRecord] = []
    for i, r in enumerate(rows):
        if not isinstance(r, dict):
            raise ValueError(f"Row {i}: expected object, got {type(r)}")
        if not r.get("account_name"):
            raise ValueError(f"Row {i}: missing account_name")
        kwargs = {k: v for k, v in r.items() if k in names}
        out.append(AccountRecord(**kwargs))
    return out


def _emit_row(
    input_path: Path,
    *,
    omit_last_updated: bool = False,
    metadata_extra: Dict[str, Any] | None = None,
) -> dict:
    data = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"{input_path}: expected JSON array of account objects")
    accounts = _account_records_from_json(data)
    if omit_last_updated:
        for a in accounts:
            a.last_updated = None  # excluded from JSON (same as missing key)
    row: Dict[str, Any] = {
        "input": build_prioritizer_user_message(accounts),
        "metadata": {"source_file": str(input_path.name), **(metadata_extra or {})},
    }
    return row


def main() -> None:
    p = argparse.ArgumentParser(description="JSONL rows for LD prioritizer offline eval")
    p.add_argument(
        "account_json",
        nargs="+",
        type=Path,
        help="JSON files, each a list of account objects",
    )
    p.add_argument(
        "--omit-last-updated",
        action="store_true",
        help="Strip last_updated from each account so JSONL is stable for git / sharing",
    )
    args = p.parse_args()
    for path in args.account_json:
        if not path.exists():
            print(f"error: file not found: {path}", file=sys.stderr)
            sys.exit(1)
        row = _emit_row(path, omit_last_updated=args.omit_last_updated)
        sys.stdout.write(json.dumps(row, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()

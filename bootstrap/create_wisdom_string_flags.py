#!/usr/bin/env python3
"""
Create or update the three E100 Wisdom string multivariate flags in LaunchDarkly.

Uses the same REST API as the LaunchDarkly MCP ``create-feature-flag`` / ``update-feature-flag``
tools (POST/PATCH /api/v2/flags/...).

Prerequisites:
  LD_API_KEY (with write access), LD_PROJECT_KEY (default: default)

Usage:
  python bootstrap/create_wisdom_string_flags.py           # upsert all three
  python bootstrap/create_wisdom_string_flags.py --dry-run # print bodies only
  python bootstrap/create_wisdom_string_flags.py --print-mcp-hints
      # JSON shapes for manual Cursor MCP ``create-feature-flag`` calls
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Allow ``from bootstrap.X import`` when run as ``python bootstrap/create_wisdom_string_flags.py``
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from bootstrap.wisdom_prompt_flag_defaults import (  # noqa: E402
    DEFAULT_PROMPT_BY_FLAG_KEY,
    WISDOM_FLAG_META,
    assert_keys_align_with_codebase,
)

LD_API_KEY = os.getenv("LD_API_KEY")
LD_PROJECT_KEY = os.getenv("LD_PROJECT_KEY", "default")


def ld_request(method: str, path: str, payload: dict = None) -> tuple[int, object]:
    url = f"https://app.launchdarkly.com{path}"
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": LD_API_KEY,
            "Content-Type": "application/json",
        },
        method=method,
    )
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read().decode() if resp.length else ""
            body = json.loads(raw) if raw.strip() else {}
            return resp.status, body
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = raw
        return e.code, body


def _feature_flag_body(flag_key: str, name: str, prompt_value: str) -> dict:
    # LaunchDarkly multivariate flags require at least two variations.
    # Index 0 = off / empty (runner skips empty prompt). Index 1 = production prompt.
    return {
        "key": flag_key,
        "name": name,
        "description": (
            "E100 Enterpret Wisdom MCP prompt body (full string passed to Tier 2). "
            "Evaluated with the same user context as LD_SDK_KEY (e.g. user/e100-agent). "
            "Turn flag on and target variation \"production\" (index 1) for live runs."
        ),
        "variations": [
            {
                "value": "",
                "name": "empty",
                "description": "No query — Tier 2 skips this slot when this variation is served",
            },
            {
                "value": prompt_value,
                "name": "production",
                "description": "Wisdom query instructions (edit in LD UI as needed)",
            },
        ],
        "defaults": {
            "onVariation": 1,
            "offVariation": 0,
        },
        "temporary": False,
        "tags": ["e100", "wisdom-prompt"],
        "isFlagOn": True,
    }


def create_flag(flag_key: str, name: str, prompt_value: str) -> None:
    status, body = ld_request(
        "POST",
        f"/api/v2/flags/{LD_PROJECT_KEY}",
        _feature_flag_body(flag_key, name, prompt_value),
    )
    if status in (200, 201):
        print(f"  Created flag: {flag_key}")
        return
    print(f"  Error creating {flag_key}: HTTP {status} — {body}")


def update_flag_variation(flag_key: str, prompt_value: str) -> None:
    """JSON Patch: set production variation string value (index 1 if two variations exist)."""
    st, existing = ld_request("GET", f"/api/v2/flags/{LD_PROJECT_KEY}/{flag_key}")
    idx = 1
    if st == 200 and isinstance(existing, dict):
        n = len(existing.get("variations") or [])
        if n < 2:
            print(
                f"  Warning: {flag_key} has {n} variation(s); LD requires 2 for multivariate. "
                "Add an empty first variation in the LD UI or delete the flag and re-run this script."
            )
            idx = 0
        elif n > 1:
            idx = 1
    patch = [
        {"op": "replace", "path": f"/variations/{idx}/value", "value": prompt_value},
    ]
    status, body = ld_request(
        "PATCH",
        f"/api/v2/flags/{LD_PROJECT_KEY}/{flag_key}",
        {"patch": patch, "comment": "E100 bootstrap: sync Wisdom prompt production body"},
    )
    if status == 200:
        print(f"  Updated variations[{idx}].value: {flag_key}")
        return
    print(f"  Error patching {flag_key}: HTTP {status} — {body}")


def flag_exists(flag_key: str) -> bool:
    status, _ = ld_request("GET", f"/api/v2/flags/{LD_PROJECT_KEY}/{flag_key}")
    return status == 200


def upsert_all(*, dry_run: bool) -> None:
    assert_keys_align_with_codebase()
    for flag_key, name in WISDOM_FLAG_META:
        prompt = DEFAULT_PROMPT_BY_FLAG_KEY[flag_key]
        if dry_run:
            print(f"[dry-run] {flag_key}: {len(prompt)} chars")
            continue
        if flag_exists(flag_key):
            update_flag_variation(flag_key, prompt)
        else:
            create_flag(flag_key, name, prompt)


def print_mcp_hints() -> None:
    """Payloads for Cursor LaunchDarkly MCP tool ``create-feature-flag``."""
    assert_keys_align_with_codebase()
    print(
        "Use MCP server ``user-LaunchDarkly`` → tool ``create-feature-flag`` "
        "once per flag (or run this script with LD_API_KEY).\n"
    )
    for flag_key, name in WISDOM_FLAG_META:
        body = _feature_flag_body(
            flag_key,
            name,
            DEFAULT_PROMPT_BY_FLAG_KEY[flag_key],
        )
        envelope = {
            "request": {
                "projectKey": LD_PROJECT_KEY,
                "FeatureFlagBody": body,
            }
        }
        print(f"--- {flag_key} ---")
        print(json.dumps(envelope, indent=2))
        print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Upsert E100 Wisdom string flags in LaunchDarkly")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate keys and print flag keys + prompt lengths only",
    )
    parser.add_argument(
        "--print-mcp-hints",
        action="store_true",
        help="Print JSON for manual MCP create-feature-flag calls",
    )
    args = parser.parse_args()

    if args.print_mcp_hints:
        print_mcp_hints()
        return

    if not args.dry_run and not LD_API_KEY:
        raise SystemExit("LD_API_KEY environment variable not set")

    print(f"Project: {LD_PROJECT_KEY}")
    upsert_all(dry_run=args.dry_run)
    if args.dry_run:
        print("Dry run OK — keys align with agents.wisdom_prompts.WISDOM_PROMPT_FLAG_KEYS")
    elif LD_API_KEY:
        print(
            "\nNext: In LaunchDarkly, ensure each flag is **on** in the environment tied to "
            "LD_SDK_KEY so the SDK serves the **production** variation (index 1), not **empty**."
        )


if __name__ == "__main__":
    main()

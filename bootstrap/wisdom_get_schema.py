#!/usr/bin/env python3
"""
Print Enterpret Wisdom Knowledge Graph schema via MCP ``get_schema``.

Requires ``WISDOM_AUTH_TOKEN`` (and optional ``WISDOM_SERVER_URL``) in the environment
or ``.env``. Runs ``initialize_wisdom`` first when that tool exists (Enterpret recommendation).

Usage:
  python bootstrap/wisdom_get_schema.py
  python bootstrap/wisdom_get_schema.py --list-tools
  python bootstrap/wisdom_get_schema.py --no-warmup
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from agents.wisdom_mcp import (  # noqa: E402
    DEFAULT_WISDOM_MCP_URL,
    WisdomMCPClient,
    WisdomMCPError,
    wisdom_warmup_if_available,
)


def _args_from_schema(tool_def: dict) -> Dict[str, Any]:
    """Minimal args satisfying required JSON Schema properties (same idea as wisdom_warmup)."""
    schema = tool_def.get("inputSchema") or {}
    props = schema.get("properties") or {}
    required = list(schema.get("required") or [])
    args: Dict[str, Any] = {}
    for r in required:
        spec = props.get(r) or {}
        t = spec.get("type")
        if t == "string":
            args[r] = ""
        elif t == "object":
            args[r] = {}
        elif t == "boolean":
            args[r] = False
        elif t == "array":
            args[r] = []
        elif t in ("number", "integer"):
            args[r] = 0
        else:
            args[r] = None
    return args


def _pick_schema_tool(tools: List[dict]) -> tuple[str, dict]:
    by_name = {t.get("name"): t for t in tools if t.get("name")}
    for candidate in ("get_schema", "getSchema", "get_graph_schema"):
        if candidate in by_name:
            return candidate, by_name[candidate]
    raise WisdomMCPError(
        "No get_schema-like tool in tools/list. Use --list-tools. "
        f"Available: {sorted(by_name.keys())}"
    )


async def _run(*, warmup: bool, list_tools: bool) -> None:
    load_dotenv(_REPO_ROOT / ".env")
    token = (os.getenv("WISDOM_AUTH_TOKEN") or "").strip()
    if not token:
        raise SystemExit(
            "WISDOM_AUTH_TOKEN is not set. Add it to .env (Enterpret Settings → Wisdom MCP)."
        )
    base = (os.getenv("WISDOM_SERVER_URL") or DEFAULT_WISDOM_MCP_URL).strip().rstrip("/")

    async with WisdomMCPClient(base, token) as client:
        tools = await client.list_tools()
        if list_tools:
            for t in sorted(tools, key=lambda x: (x.get("name") or "")):
                print(t.get("name") or "?")
            return

        if warmup:
            await wisdom_warmup_if_available(client, tools)

        name, tdef = _pick_schema_tool(tools)
        args = _args_from_schema(tdef)
        try:
            result = await client.call_tool(name, args)
        except WisdomMCPError as e:
            if not args:
                raise
            try:
                result = await client.call_tool(name, {})
            except WisdomMCPError:
                raise e from None

        print(json.dumps(result, indent=2, default=str))


def main() -> None:
    p = argparse.ArgumentParser(description="Call Wisdom MCP get_schema")
    p.add_argument(
        "--list-tools",
        action="store_true",
        help="Print tool names from tools/list and exit",
    )
    p.add_argument(
        "--no-warmup",
        action="store_true",
        help="Skip initialize_wisdom before get_schema",
    )
    args = p.parse_args()
    try:
        asyncio.run(_run(warmup=not args.no_warmup, list_tools=args.list_tools))
    except WisdomMCPError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

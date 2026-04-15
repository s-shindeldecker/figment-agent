#!/usr/bin/env python3
"""
Evaluate a LaunchDarkly **completion-mode** AI Config, call **Anthropic** with the
returned model + messages, parse JSON rows, and apply ``apply_prioritizer_response``.

Production ``run.py`` still uses deterministic ``merge_and_score``; this script is for
testing the model path in isolation.

Setup
-----
  pip install "figment-agent[launchdarkly-ai]"

Env
---
  LD_SDK_KEY or LAUNCHDARKLY_SDK_KEY
  ANTHROPIC_API_KEY
  LD_PRIORITIZER_AI_CONFIG_KEY  (or pass --config-key)

AI Config messages should include a template variable ``{{accounts_json}}`` (passed in
``completion_config`` variables as ``accounts_json``). Ask the model for a JSON **array**
of objects with at least ``account_name``, plus ``expansion_score``, ``priority_rank``,
and optional ``notes`` (see ``agents/prioritizer.py``).

Example
-------
  python scripts/ld_prioritizer_smoke.py \\
    --sample-json docs/examples/prioritizer_eval_scenario_small.json
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, List

# Repo root on sys.path
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from agents.ld_wisdom_config import _env_first  # reuse LD env naming
from agents.prioritizer import apply_prioritizer_response
from core.deduplicator import merge_accounts
from core.schema import AccountRecord


def _sdk_key() -> str:
    k = _env_first("LD_SDK_KEY", "LAUNCHDARKLY_SDK_KEY") or ""
    return k.strip()


def _context():
    from ldclient import Context

    key = _env_first("LD_CONTEXT_KEY", "LAUNCHDARKLY_CONTEXT_KEY") or "figment-e100"
    b = Context.builder(key)
    project = _env_first("LD_PROJECT_KEY")
    env_key = _env_first("LD_ENV_KEY")
    if project:
        b.set("projectKey", project)
    if env_key:
        b.set("environmentKey", env_key)
    return b.build()


def _records_from_json_rows(rows: List[dict]) -> List[AccountRecord]:
    fields = set(AccountRecord.__dataclass_fields__)
    out: List[AccountRecord] = []
    for i, d in enumerate(rows):
        if not isinstance(d, dict):
            raise ValueError(f"Row {i} is not an object")
        kw = {k: v for k, v in d.items() if k in fields}
        if not kw.get("account_name"):
            raise ValueError(f"Row {i} missing account_name")
        out.append(AccountRecord(**kw))
    return out


def _accounts_json_payload(accounts: List[AccountRecord]) -> str:
    def slim(a: AccountRecord) -> dict:
        d = dataclasses.asdict(a)
        return {k: v for k, v in d.items() if v is not None}

    return json.dumps([slim(a) for a in accounts], indent=2, default=str)


def _parse_model_json_array(text: str) -> List[dict]:
    s = text.strip()
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", s)
    if fence:
        s = fence.group(1).strip()
    parsed = json.loads(s)
    if isinstance(parsed, dict):
        for key in ("rows", "prioritized", "accounts", "results"):
            v = parsed.get(key)
            if isinstance(v, list):
                parsed = v
                break
        else:
            raise ValueError("Expected a JSON array or an object with a list field")
    if not isinstance(parsed, list):
        raise ValueError("Top-level JSON must be an array")
    return [x for x in parsed if isinstance(x, dict)]


def _ld_messages_to_anthropic(messages: List[Any]):
    system_parts: List[str] = []
    anth_msgs: List[dict] = []
    for m in messages or []:
        role = getattr(m, "role", None)
        content = getattr(m, "content", None)
        if role == "system":
            system_parts.append(str(content))
        elif role in ("user", "assistant"):
            anth_msgs.append({"role": role, "content": str(content)})
    system = "\n\n".join(system_parts) if system_parts else None
    return system, anth_msgs


def main() -> int:
    parser = argparse.ArgumentParser(description="LD AI Config + Anthropic prioritizer smoke test")
    parser.add_argument(
        "--sample-json",
        type=Path,
        default=_ROOT / "docs/examples/prioritizer_eval_scenario_small.json",
        help="JSON array of account-shaped objects",
    )
    parser.add_argument(
        "--config-key",
        default=(os.getenv("LD_PRIORITIZER_AI_CONFIG_KEY") or "").strip(),
        help="LaunchDarkly AI Config key (default: env LD_PRIORITIZER_AI_CONFIG_KEY)",
    )
    parser.add_argument("--max-tokens", type=int, default=4096)
    args = parser.parse_args()

    if not _sdk_key():
        print("Missing LD_SDK_KEY (or LAUNCHDARKLY_SDK_KEY).", file=sys.stderr)
        return 1
    api_key = (os.getenv("ANTHROPIC_API_KEY") or "").strip()
    if not api_key:
        print("Missing ANTHROPIC_API_KEY.", file=sys.stderr)
        return 1
    if not args.config_key:
        print("Missing AI config key: set LD_PRIORITIZER_AI_CONFIG_KEY or pass --config-key.", file=sys.stderr)
        return 1

    try:
        import ldclient
        from ldclient.config import Config
        from ldai.client import AICompletionConfigDefault, LDAIClient
        from ldai.tracker import TokenUsage
        import anthropic
    except ImportError as e:
        print(
            "Install optional deps: pip install \"figment-agent[launchdarkly-ai]\"\n"
            f"Import error: {e}",
            file=sys.stderr,
        )
        return 1

    raw = json.loads(args.sample_json.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        print("--sample-json must contain a JSON array.", file=sys.stderr)
        return 1
    merged = merge_accounts(_records_from_json_rows(raw))
    accounts_json = _accounts_json_payload(merged)

    ldclient.set_config(Config(_sdk_key()))
    ld = ldclient.get()
    aiclient = LDAIClient(ld)
    ctx = _context()
    fallback = AICompletionConfigDefault(enabled=False)
    cfg = aiclient.completion_config(
        args.config_key,
        ctx,
        fallback,
        {"accounts_json": accounts_json},
    )
    if not cfg.enabled:
        print(
            "AI Config evaluation returned disabled/fallback. Check key, targeting, and fallthrough.",
            file=sys.stderr,
        )
        return 1
    if not cfg.model or not cfg.messages:
        print("Evaluated config missing model or messages.", file=sys.stderr)
        return 1

    tracker = cfg.tracker
    system, anth_messages = _ld_messages_to_anthropic(cfg.messages)
    client = anthropic.Anthropic(api_key=api_key)
    temp_param = cfg.model.get_parameter("temperature")
    max_tokens = cfg.model.get_parameter("maxTokens")
    max_tok = int(max_tokens) if max_tokens is not None else args.max_tokens

    try:

        def _call():
            kwargs: dict = {
                "model": cfg.model.name,
                "max_tokens": max_tok,
                "messages": anth_messages,
            }
            if temp_param is not None:
                kwargs["temperature"] = float(temp_param)
            if system:
                kwargs["system"] = system
            return client.messages.create(**kwargs)

        resp = tracker.track_duration_of(_call) if tracker else _call()
        if tracker:
            tracker.track_success()
            u = getattr(resp, "usage", None)
            if u is not None:
                inp = getattr(u, "input_tokens", 0) or 0
                out = getattr(u, "output_tokens", 0) or 0
                tracker.track_tokens(TokenUsage(total=inp + out, input=inp, output=out))
    except Exception as e:
        if tracker:
            tracker.track_error()
        print(f"Anthropic request failed: {e}", file=sys.stderr)
        return 1
    finally:
        try:
            ld.close()
        except Exception:
            pass

    block = resp.content[0]
    text = getattr(block, "text", None) or str(block)
    try:
        rows = _parse_model_json_array(text)
    except (json.JSONDecodeError, ValueError) as e:
        print(f"Could not parse model output as JSON array: {e}\n---\n{text}\n---", file=sys.stderr)
        return 1

    ranked = apply_prioritizer_response(list(merged), rows)
    print(json.dumps([dataclasses.asdict(a) for a in ranked], indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

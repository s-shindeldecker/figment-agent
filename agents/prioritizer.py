"""
Single AI Config pass: interpret merged account data and assign priority_rank / expansion_score.

Uses ``ai_client.agent_config`` for instructions and model. Invokes Anthropic Messages API
via httpx when ``ldai`` provider packages are unavailable.
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict
from typing import Any, List, Optional

import httpx

from agents.wisdom_mcp import extract_json_array_from_text
from core.schema import AccountRecord

PRIORITIZER_AI_CONFIG_KEY = os.getenv("E100_PRIORITIZER_AI_CONFIG_KEY", "e100-prioritizer").strip()


def prioritizer_llm_requested() -> bool:
    """
    When False, ``run.py`` skips the LaunchDarkly AI Config / Anthropic prioritizer and uses
    ``merge_and_score`` (heuristic scoring in ``core/scorer.py``) only.

    Env ``E100_PRIORITIZER_MODE``:
      - ``llm`` (default if unset) — attempt LLM prioritization, then fall back on failure.
      - ``deterministic`` / ``heuristic`` / ``off`` / ``0`` / ``false`` / ``no`` — never call the LLM.
    """
    raw = (os.getenv("E100_PRIORITIZER_MODE") or "llm").strip().lower()
    if raw in ("deterministic", "heuristic", "off", "0", "false", "no"):
        return False
    if raw in ("llm", "ai", "on", "1", "true", "yes", "auto", ""):
        return True
    return True

# User message prefix (after system / AI Config instructions). Kept stable for offline eval datasets.
PRIORITIZER_USER_INTRO = (
    "Merged account records (JSON). Assess and rank for GTM priority.\n"
)

_USER_PAYLOAD_SUFFIX = (
    "\n\nRespond with ONLY a JSON array (no markdown), same length as input accounts, "
    "one object per account you are ranking, ordered by final priority (best first). "
    "Each object must include: "
    '"account_name" (string), "priority_rank" (integer starting at 1), '
    '"expansion_score" (number), optional "notes" (string). '
    "Include every account from the input; use exact account_name values."
)


def _accounts_json(accounts: List[AccountRecord]) -> str:
    rows = []
    for a in accounts:
        d = {k: v for k, v in asdict(a).items() if v is not None}
        rows.append(d)
    return json.dumps(rows, indent=2)


def build_prioritizer_user_message(accounts: List[AccountRecord]) -> str:
    """
    Exact user-role content sent to the model (system = AI Config instructions).

    Use this string as dataset ``input`` for LaunchDarkly offline evaluations when the
    evaluation's AI Config already carries your agent instructions.
    """
    return PRIORITIZER_USER_INTRO + _accounts_json(accounts) + _USER_PAYLOAD_SUFFIX


def apply_prioritizer_response(
    accounts: List[AccountRecord],
    rows: List[dict],
) -> List[AccountRecord]:
    """Merge model output into records; stable sort by priority_rank; renumber 1..n."""
    if not accounts:
        return accounts
    by_lower = {a.account_name.lower().strip(): a for a in accounts}
    matched: set[str] = set()

    for row in rows:
        name = row.get("account_name") or row.get("accountName")
        if not isinstance(name, str) or not name.strip():
            continue
        k = name.lower().strip()
        if k not in by_lower:
            continue
        acct = by_lower[k]
        if row.get("expansion_score") is not None:
            try:
                acct.expansion_score = float(row["expansion_score"])
            except (TypeError, ValueError):
                pass
        if row.get("priority_rank") is not None:
            try:
                acct.priority_rank = int(row["priority_rank"])
            except (TypeError, ValueError):
                pass
        if row.get("notes") is not None:
            acct.notes = str(row["notes"])
        matched.add(k)

    base = max((a.priority_rank or 0 for a in accounts), default=0)
    extra_i = 0
    for a in accounts:
        k = a.account_name.lower().strip()
        if k not in matched or a.priority_rank is None:
            extra_i += 1
            a.priority_rank = base + extra_i
            if a.expansion_score is None:
                a.expansion_score = 0.0

    ordered = sorted(accounts, key=lambda x: (x.priority_rank or 10**9, x.account_name or ""))
    for i, a in enumerate(ordered, start=1):
        a.priority_rank = i
    return ordered


async def _anthropic_messages(
    model: str,
    system: str,
    user: str,
) -> str:
    api_key = (os.getenv("ANTHROPIC_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is not set")

    async with httpx.AsyncClient(timeout=httpx.Timeout(180.0, connect=30.0)) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": model,
                "max_tokens": 16384,
                "system": system,
                "messages": [{"role": "user", "content": user}],
            },
        )
        if resp.status_code != 200:
            detail = (resp.text or "")[:4000]
            print(
                f"[Prioritizer] Anthropic API HTTP {resp.status_code} "
                f"(model={model!r}). Body (truncated):\n{detail}"
            )
        resp.raise_for_status()
        data = resp.json()
    parts = data.get("content") or []
    texts = [p.get("text") for p in parts if isinstance(p, dict) and p.get("type") == "text"]
    return "\n".join(texts).strip()


async def _invoke_via_provider_factory(cfg: Any, system: str, user: str) -> Optional[str]:
    try:
        from ldai.models import LDMessage
        from ldai.providers.ai_provider_factory import AIProviderFactory
    except ImportError:
        return None

    provider = await AIProviderFactory.create(cfg, None)
    if provider is None:
        return None
    messages = [
        LDMessage(role="system", content=system),
        LDMessage(role="user", content=user),
    ]
    response = await provider.invoke_model(messages)
    if response is None or response.message is None:
        return None
    return (response.message.content or "").strip()


async def prioritize_with_ai_config(
    ai_client: Any,
    context: Any,
    accounts: List[AccountRecord],
) -> Optional[List[AccountRecord]]:
    """
    If the prioritizer AI Config is enabled, run the model and apply rankings.
    Returns None to signal caller should use merge_and_score instead.
    """
    if not ai_client or not context or not accounts:
        return None

    from ldai.models import AIAgentConfigDefault

    cfg = ai_client.agent_config(
        PRIORITIZER_AI_CONFIG_KEY,
        context,
        AIAgentConfigDefault.disabled(),
    )
    if not cfg.enabled:
        print(
            f"[Prioritizer] AI Config {PRIORITIZER_AI_CONFIG_KEY!r} disabled — "
            "using deterministic merge_and_score"
        )
        return None

    system = (cfg.instructions or "").strip() or (
        "You prioritize B2B accounts for expansion. Output strict JSON only."
    )
    user_body = build_prioritizer_user_message(accounts)

    model_name = cfg.model.name if cfg.model else "claude-sonnet-4-20250514"
    text = ""
    try:
        text = await _invoke_via_provider_factory(cfg, system, user_body)
        if not text:
            print("[Prioritizer] ldai provider unavailable — trying Anthropic API directly")
            text = await _anthropic_messages(model_name, system, user_body)
    except Exception as e:
        print(f"[Prioritizer] Model invocation failed — {e}")
        if cfg.tracker:
            cfg.tracker.track_error()
        return None

    rows = extract_json_array_from_text(text)
    if not rows:
        print("[Prioritizer] Could not parse JSON array from model output — fallback to merge_and_score")
        if cfg.tracker:
            cfg.tracker.track_error()
        return None

    try:
        out = apply_prioritizer_response(list(accounts), rows)
    except Exception as e:
        print(f"[Prioritizer] Failed to apply rankings — {e}")
        if cfg.tracker:
            cfg.tracker.track_error()
        return None

    if cfg.tracker:
        cfg.tracker.track_success()
    print(f"[Prioritizer] Applied LLM rankings to {len(out)} accounts")
    return out

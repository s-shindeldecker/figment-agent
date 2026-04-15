"""
E100 merge output → priority_rank / expansion_score.

When enabled, evaluates the LaunchDarkly **agent-mode** AI Config (default key
``e100-prioritizer``), calls the model (ldai provider package if installed, else
Anthropic Messages API via httpx), parses a JSON array, and merges with
``apply_prioritizer_response``. On any failure or disabled config, ``run.py`` falls
back to ``merge_and_score`` / ``core/scorer.py``.

**Inspect prompts:** ``E100_PRIORITIZER_LOG_PROMPT=1`` (truncated console preview),
``E100_PRIORITIZER_PROMPT_LOG_PATH=/path/file.txt`` (full system + user on disk),
``E100_PRIORITIZER_DEBUG=1`` (traceback on errors; model text preview if JSON parse fails).

**LaunchDarkly AI metrics (httpx path):** after each Anthropic response, emits
``track_duration``, ``track_tokens`` (from the API ``usage`` field), then
``track_success`` / ``track_error``; ``ldclient.flush()`` runs so events reach LD.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Tuple

import httpx

from agents.wisdom_mcp import extract_json_array_from_text, format_json_array_parse_failure
from core.schema import AccountRecord

PRIORITIZER_AI_CONFIG_KEY = os.getenv("E100_PRIORITIZER_AI_CONFIG_KEY", "e100-prioritizer").strip()

# Used only when the LaunchDarkly agent variation has empty instructions (misconfiguration).
_DEFAULT_AGENT_INSTRUCTIONS = """You are a GTM prioritization assistant for an enterprise SaaS expansion list.
You receive JSON account records (Looker usage, competitive intel, notes).
Assess commercial potential, urgency, and fit; assign expansion_score (higher = more priority).
The user message will ask for a JSON array only. Each element must include:
  account_name (exact match to input),
  priority_rank (integer, 1 = top priority),
  expansion_score (number),
  notes (optional string with rationale).
Include every input account. Output JSON array only, no markdown fences."""


def _anthropic_read_timeout_sec() -> float:
    """httpx read timeout for the prioritizer Messages call (large prompts can take many minutes)."""
    raw = (os.getenv("E100_PRIORITIZER_ANTHROPIC_TIMEOUT_SEC") or "").strip()
    if raw:
        try:
            return max(60.0, float(raw))
        except ValueError:
            pass
    return 900.0


def _prioritizer_max_output_tokens(cfg: Any) -> int:
    """
    Anthropic ``max_tokens`` for the prioritizer reply.

    Default 64k: ~400+ ranked rows with ``notes`` can exceed 16k output tokens and truncate
    mid-JSON (``Unterminated string``). Override with env or LD ``maxTokens`` on the variation.
    """
    cap = 128_000
    floor = 4_096
    raw = (os.getenv("E100_PRIORITIZER_MAX_OUTPUT_TOKENS") or "").strip()
    if raw:
        try:
            return max(floor, min(cap, int(raw)))
        except ValueError:
            pass
    if cfg.model:
        mt = cfg.model.get_parameter("maxTokens")
        if mt is not None:
            try:
                return max(floor, min(cap, int(mt)))
            except (TypeError, ValueError):
                pass
    return 64_000


def prioritizer_llm_requested() -> bool:
    """
    When False, ``run.py`` skips the LaunchDarkly AI Config / Anthropic prioritizer and uses
    ``merge_and_score`` only.

    Env ``E100_PRIORITIZER_MODE``:
      - ``llm`` (default if unset) — attempt LLM prioritization, then fall back on failure.
      - ``deterministic`` / ``heuristic`` / ``off`` / ``0`` / ``false`` / ``no`` — never call the LLM.
    """
    raw = (os.getenv("E100_PRIORITIZER_MODE") or "llm").strip().lower()
    if raw in ("deterministic", "heuristic", "off", "0", "false", "no"):
        return False
    return True


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

# Omit from LLM user JSON by default (saves tokens; looker_extras alone can be 100k+ chars).
_LLM_INPUT_SKIP_FIELDS = frozenset(
    {"looker_extras", "wisdom_extras", "tier3_extras", "last_updated"}
)


def _accounts_json_full(accounts: List[AccountRecord]) -> str:
    rows = []
    for a in accounts:
        d = {k: v for k, v in asdict(a).items() if v is not None}
        rows.append(d)
    return json.dumps(rows, indent=2, default=str)


def _accounts_json_for_prioritizer_llm(accounts: List[AccountRecord]) -> str:
    """Structured fields only — no wide Sheets/Looker/ZI extra dicts unless env requests them."""
    if (os.getenv("E100_PRIORITIZER_INCLUDE_EXTRAS_IN_PROMPT") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        return _accounts_json_full(accounts)
    rows: List[dict] = []
    for a in accounts:
        d = asdict(a)
        for k in _LLM_INPUT_SKIP_FIELDS:
            d.pop(k, None)
        slim = {k: v for k, v in d.items() if v is not None and v != {} and v != []}
        rows.append(slim)
    return json.dumps(rows, indent=2, default=str)


def build_prioritizer_user_message(accounts: List[AccountRecord]) -> str:
    """
    User-role content for the prioritizer model (system = AI Config instructions).

    Use as dataset ``input`` for LaunchDarkly offline evaluations when the evaluation's
    AI Config already carries agent instructions.

    By default omits ``looker_extras`` / ``wisdom_extras`` / ``tier3_extras`` (see
    ``_accounts_json_for_prioritizer_llm``). Set ``E100_PRIORITIZER_INCLUDE_EXTRAS_IN_PROMPT=1``
    to send the full wide record JSON.
    """
    return PRIORITIZER_USER_INTRO + _accounts_json_for_prioritizer_llm(accounts) + _USER_PAYLOAD_SUFFIX


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


def _flush_ld_client_events() -> None:
    """Best-effort flush so AI metric events reach LaunchDarkly before process continues."""
    try:
        import ldclient

        c = ldclient.get()
        if c is not None and hasattr(c, "flush"):
            c.flush()
    except Exception:
        pass


def _emit_prioritizer_ld_metrics(
    tracker: Any,
    elapsed_sec: float,
    usage_raw: Optional[Dict[str, Any]],
) -> None:
    """Emit duration + token metrics for LD AI Config monitoring (httpx path)."""
    if not tracker:
        return
    try:
        from ldai.tracker import TokenUsage
    except ImportError:
        return
    tracker.track_duration(max(0, int(elapsed_sec * 1000)))
    if usage_raw:
        inp = int(usage_raw.get("input_tokens") or 0)
        out = int(usage_raw.get("output_tokens") or 0)
        if inp or out:
            tracker.track_tokens(TokenUsage(total=inp + out, input=inp, output=out))


async def _anthropic_messages(
    model: str,
    system: str,
    user: str,
    *,
    max_output_tokens: int,
) -> Tuple[str, Optional[Dict[str, Any]]]:
    api_key = (os.getenv("ANTHROPIC_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is not set")

    read_sec = _anthropic_read_timeout_sec()
    # Long read for big account batches; connect stays bounded.
    timeout = httpx.Timeout(30.0, read=read_sec, write=30.0, connect=30.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": model,
                "max_tokens": max_output_tokens,
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
        try:
            data = resp.json()
        except json.JSONDecodeError as je:
            snippet = (resp.text or "")[:2000]
            raise RuntimeError(
                f"Anthropic response was not JSON ({je}); body (truncated): {snippet!r}"
            ) from je
    stop = data.get("stop_reason")
    if stop == "max_tokens":
        print(
            "[Prioritizer] Anthropic stop_reason=max_tokens — reply was cut at max_tokens; "
            "JSON may be incomplete. Set E100_PRIORITIZER_MAX_OUTPUT_TOKENS or LD maxTokens higher."
        )
    parts = data.get("content") or []
    texts = [p.get("text") for p in parts if isinstance(p, dict) and p.get("type") == "text"]
    body = "\n".join(texts).strip()
    usage = data.get("usage")
    usage_raw = usage if isinstance(usage, dict) else None
    return body, usage_raw


def _prioritizer_verbose_logs() -> bool:
    for name in ("E100_PRIORITIZER_LOG_PROMPT", "E100_PRIORITIZER_DEBUG"):
        if (os.getenv(name) or "").strip().lower() in ("1", "true", "yes", "on"):
            return True
    return False


def _maybe_log_prioritizer_prompt(system: str, user: str) -> None:
    """
    Optional inspection of the Anthropic request (system = LD instructions, user = our payload).

    - ``E100_PRIORITIZER_PROMPT_LOG_PATH=/path/to/file.txt`` — write full system + user (UTF-8).
    - ``E100_PRIORITIZER_LOG_PROMPT=1`` — print truncated previews (avoid huge console dumps).
    """
    path = (os.getenv("E100_PRIORITIZER_PROMPT_LOG_PATH") or "").strip()
    if path:
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write("=== SYSTEM (LaunchDarkly agent instructions) ===\n")
                f.write(system)
                f.write("\n\n=== USER (build_prioritizer_user_message) ===\n")
                f.write(user)
            print(
                f"[Prioritizer] Wrote full prompt to {path!r} "
                f"({len(system) + len(user)} chars total)."
            )
        except OSError as ex:
            print(f"[Prioritizer] Could not write E100_PRIORITIZER_PROMPT_LOG_PATH={path!r}: {ex}")

    if not (os.getenv("E100_PRIORITIZER_LOG_PROMPT") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        return

    sys_cap, usr_cap, tail = 4000, 2500, 1200
    sys_show = system if len(system) <= sys_cap else system[:sys_cap] + "\n… [truncated]"
    usr_head = user if len(user) <= usr_cap else user[:usr_cap] + "\n… [truncated]"
    usr_tail = ""
    if len(user) > usr_cap + tail:
        usr_tail = "\n--- user message tail ---\n" + user[-tail:]
    print(
        f"[Prioritizer] LOG_PROMPT preview — system ({len(system)} chars):\n{sys_show}\n"
        f"[Prioritizer] LOG_PROMPT preview — user ({len(user)} chars), start:\n{usr_head}{usr_tail}\n"
    )


def _prefer_direct_anthropic_http(cfg: Any, model_name: str) -> bool:
    """
    Skip ldai's LangChain provider path when we can call Anthropic's HTTP API directly.

    Avoids noisy \"ldai_langchain not found\" logs for typical LD Anthropic agent configs.
    """
    if not (os.getenv("ANTHROPIC_API_KEY") or "").strip():
        return False
    prov = ""
    if cfg.provider and getattr(cfg.provider, "name", None):
        prov = str(cfg.provider.name).strip().lower()
    if prov == "anthropic":
        return True
    m = (model_name or "").lower()
    return "claude" in m


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


async def prioritize_with_ai_config(accounts: List[AccountRecord]) -> Optional[List[AccountRecord]]:
    """
    If the prioritizer AI Config is enabled and the model returns valid JSON, apply LLM rankings.

    Uses the same LD client and evaluation context as Tier-2 flag reads
    (``agents.ld_wisdom_config``). Returns None so the caller can use ``merge_and_score``.
    """
    if not accounts:
        return None

    from agents.ld_wisdom_config import _evaluation_context, _get_client

    ld_client = _get_client()
    context = _evaluation_context()
    if ld_client is None or context is None:
        print(
            "[Prioritizer] LaunchDarkly unavailable (install figment-agent[launchdarkly], "
            "set LD_SDK_KEY) — using deterministic merge_and_score"
        )
        return None

    try:
        from ldai.client import LDAIClient
        from ldai.models import AIAgentConfigDefault
    except ImportError:
        print(
            "[Prioritizer] launchdarkly-server-sdk-ai not installed "
            "(pip install \"figment-agent[launchdarkly]\") — using deterministic merge_and_score"
        )
        return None

    ai_client = LDAIClient(ld_client)
    cfg = ai_client.agent_config(
        PRIORITIZER_AI_CONFIG_KEY,
        context,
        AIAgentConfigDefault.disabled(),
    )
    if not cfg.enabled:
        print(
            f"[Prioritizer] AI Config {PRIORITIZER_AI_CONFIG_KEY!r} disabled in LaunchDarkly — "
            "using deterministic merge_and_score"
        )
        return None

    system = (cfg.instructions or "").strip() or _DEFAULT_AGENT_INSTRUCTIONS
    user_body = build_prioritizer_user_message(accounts)
    _maybe_log_prioritizer_prompt(system, user_body)

    model_name = cfg.model.name if cfg.model else "claude-sonnet-4-20250514"
    use_direct_http = _prefer_direct_anthropic_http(cfg, model_name)
    text = ""
    try:
        if use_direct_http:
            print(
                "[Prioritizer] Using Anthropic Messages API (httpx); "
                "skipping ldai LangChain provider for this config."
            )
        else:
            text = await _invoke_via_provider_factory(cfg, system, user_body)
        if not text:
            read_sec = _anthropic_read_timeout_sec()
            max_out = _prioritizer_max_output_tokens(cfg)
            prompt_chars = len(system) + len(user_body)
            if not use_direct_http:
                print(
                    "[Prioritizer] ldai provider unavailable (install optional LangChain extras to "
                    "avoid this) — calling Anthropic Messages API (httpx)"
                )
            print(
                f"[Prioritizer] Anthropic request starting: model={model_name!r}, "
                f"accounts={len(accounts)}, prompt≈{prompt_chars // 1024} KiB, "
                f"max_output_tokens={max_out}, read_timeout={read_sec:.0f}s — "
                f"large batches often take several minutes; console will log again when the response arrives."
            )
            t0 = time.monotonic()
            text, anthropic_usage = await _anthropic_messages(
                model_name,
                system,
                user_body,
                max_output_tokens=max_out,
            )
            elapsed = time.monotonic() - t0
            print(
                f"[Prioritizer] Anthropic finished in {elapsed:.1f}s "
                f"({len(text)} chars of model text)"
            )
            _emit_prioritizer_ld_metrics(cfg.tracker, elapsed, anthropic_usage)
    except Exception as e:
        msg = str(e).strip() or repr(e)
        print(f"[Prioritizer] Model invocation failed — {type(e).__name__}: {msg}")
        if (os.getenv("E100_PRIORITIZER_DEBUG") or "").strip().lower() in ("1", "true", "yes", "on"):
            import traceback

            traceback.print_exc()
        if cfg.tracker:
            cfg.tracker.track_error()
        _flush_ld_client_events()
        return None

    rows = extract_json_array_from_text(text)
    if not rows:
        print("[Prioritizer] Could not parse JSON array from model output — using deterministic merge_and_score")
        print(f"[Prioritizer] Parse detail:\n{format_json_array_parse_failure(text)}")
        if _prioritizer_verbose_logs():
            prev = 4000
            body = text if len(text) <= prev else text[:prev] + "\n… [truncated]"
            print(f"[Prioritizer] Model output preview ({len(text)} chars, first {prev}):\n{body}\n")
        if cfg.tracker:
            cfg.tracker.track_error()
        _flush_ld_client_events()
        return None

    try:
        out = apply_prioritizer_response(list(accounts), rows)
    except Exception as e:
        print(f"[Prioritizer] Failed to apply rankings — {e}")
        if cfg.tracker:
            cfg.tracker.track_error()
        _flush_ld_client_events()
        return None

    if cfg.tracker:
        cfg.tracker.track_success()
    print(
        f"[Prioritizer] Applied LLM rankings ({len(out)} accounts; "
        f"AI Config={PRIORITIZER_AI_CONFIG_KEY!r})"
    )
    _flush_ld_client_events()
    return out

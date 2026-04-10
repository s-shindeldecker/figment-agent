"""
Wisdom Tier-2 prompt jobs from ``config/settings.yaml`` only.

Stable **job keys** identify each query for logging and for ``WISDOM_CYPHER_*`` env overrides
(see ``WISDOM_CYPHER_ENV_SUFFIX_BY_FLAG_KEY``). Embedded Cypher in ``config/wisdom_cypher.yaml``
does not use the prompt text when ``execute_cypher_query`` runs; prompts still apply for
``search_knowledge_graph`` fallback when Cypher is unset or disabled.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml

from agents.ld_wisdom_config import get_wisdom_prompts_overlay_from_ld
from agents.wisdom_mcp import WisdomMCPError

# Stable Tier-2 job identifiers (order preserved). Same strings as legacy LD flag keys.
WISDOM_TIER2_JOB_KEYS: Tuple[str, ...] = (
    "e100-wisdom-prompt-competitive-displacement",
    "e100-wisdom-prompt-switching-intent",
)

# Backward-compatible alias (bootstrap / docs).
WISDOM_PROMPT_FLAG_KEYS = WISDOM_TIER2_JOB_KEYS

# ``WISDOM_CYPHER_<SUFFIX>`` per job key (suffix names are historical).
WISDOM_CYPHER_ENV_SUFFIX_BY_FLAG_KEY: Dict[str, str] = {
    "e100-wisdom-prompt-competitive-displacement": "COMPETITIVE_DISPLACEMENT",
    "e100-wisdom-prompt-switching-intent": "SWITCHING_INTENT",
}


def _settings_path() -> Path:
    return Path(__file__).resolve().parent.parent / "config" / "settings.yaml"


def _read_settings_dict() -> dict:
    path = _settings_path()
    if not path.is_file():
        data: dict = {}
    else:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        data = data if isinstance(data, dict) else {}
    return _merge_wisdom_ld_overlay(data)


def describe_wisdom_prompts_ld_overlay() -> Optional[str]:
    """
    Human-readable summary of non-empty fields applied from the LaunchDarkly
    ``figment-wisdom-tier2-prompts`` flag. Returns None if the flag contributed nothing.
    """
    ld = get_wisdom_prompts_overlay_from_ld()
    if not ld:
        return None
    parts: List[str] = []
    for key in ("tier2_prompt_fallback", "tier2_prompt_default"):
        raw = ld.get(key)
        if isinstance(raw, str) and raw.strip():
            parts.append(key)
    tp_ld = ld.get("tier2_prompts")
    if isinstance(tp_ld, dict):
        for jk, jv in sorted(tp_ld.items(), key=lambda x: str(x[0])):
            if isinstance(jv, str) and jv.strip():
                parts.append(f"tier2_prompts[{jk}]")
    if not parts:
        return None
    return ", ".join(parts)


def _merge_wisdom_ld_overlay(data: dict) -> dict:
    """Overlay wisdom.* from LaunchDarkly JSON flag (LD strings win when non-empty)."""
    ld = get_wisdom_prompts_overlay_from_ld()
    if not ld:
        return data
    out = dict(data)
    wisdom = out.get("wisdom")
    if not isinstance(wisdom, dict):
        wisdom = {}
    else:
        wisdom = dict(wisdom)
    for key in ("tier2_prompt_fallback", "tier2_prompt_default"):
        raw = ld.get(key)
        if isinstance(raw, str) and raw.strip():
            wisdom[key] = raw.strip()
    tp_ld = ld.get("tier2_prompts")
    if isinstance(tp_ld, dict):
        cur = wisdom.get("tier2_prompts")
        merged_tp = dict(cur) if isinstance(cur, dict) else {}
        for jk, jv in tp_ld.items():
            if isinstance(jv, str) and jv.strip():
                merged_tp[str(jk)] = jv.strip()
        wisdom["tier2_prompts"] = merged_tp
    out["wisdom"] = wisdom
    return out


def _load_wisdom_jobs_from_settings() -> List[Tuple[str, str]]:
    """
    One (job_key, prompt_body) per ``WISDOM_TIER2_JOB_KEYS`` row.

    Per-job text: ``wisdom.tier2_prompts.<job_key>``. Shared default:
    ``wisdom.tier2_prompt_default`` or ``wisdom.tier2_prompt_fallback``.
    """
    data = _read_settings_dict()
    wisdom = data.get("wisdom")
    if not isinstance(wisdom, dict):
        wisdom = {}

    per = wisdom.get("tier2_prompts")
    if per is not None and not isinstance(per, dict):
        per = {}

    default = (
        str(wisdom.get("tier2_prompt_default") or "").strip()
        or str(wisdom.get("tier2_prompt_fallback") or "").strip()
    )

    jobs: List[Tuple[str, str]] = []
    for job_key in WISDOM_TIER2_JOB_KEYS:
        body = ""
        if isinstance(per, dict):
            raw = per.get(job_key)
            if raw is not None:
                body = str(raw).strip()
        if not body:
            body = default
        if not body:
            raise WisdomMCPError(
                "No Tier 2 prompt text for job "
                f"{job_key!r}. Set wisdom.tier2_prompt_fallback (or "
                "wisdom.tier2_prompt_default) or wisdom.tier2_prompts."
                f"{job_key} in config/settings.yaml."
            )
        jobs.append((job_key, body))
    return jobs


def resolve_wisdom_prompt_jobs() -> Tuple[List[Tuple[str, str]], str]:
    """
    Returns ``((job_key, prompt_body), ...), source_label)``.

    Loads exactly two jobs in ``WISDOM_TIER2_JOB_KEYS`` order from ``config/settings.yaml``.
    """
    jobs = _load_wisdom_jobs_from_settings()
    ld_prompt_bits = describe_wisdom_prompts_ld_overlay()
    if ld_prompt_bits:
        print(
            f"[Tier2] Loaded {len(jobs)} Wisdom prompt job(s); "
            f"LaunchDarkly overrides: {ld_prompt_bits}; "
            "base from config/settings.yaml"
        )
    else:
        print(
            f"[Tier2] Loaded {len(jobs)} Wisdom prompt job(s) from "
            "config/settings.yaml (no LaunchDarkly prompt overlay)"
        )
    return jobs, "config/settings.yaml+ld"

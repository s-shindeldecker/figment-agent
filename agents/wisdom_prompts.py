"""
Wisdom Tier-2 prompt bodies from LaunchDarkly string feature flags.

Each **flag key** is the job id end-to-end (LD, logs, merged accounts). Cypher env vars are
the short suffixes in ``WISDOM_CYPHER_ENV_SUFFIX_BY_FLAG_KEY`` (``WISDOM_CYPHER_<SUFFIX>``).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

from agents.wisdom_mcp import WisdomMCPError

# LaunchDarkly string flag keys, Tier-2 evaluation order.
WISDOM_PROMPT_FLAG_KEYS: Tuple[str, ...] = (
    "e100-wisdom-prompt-competitive-displacement",
    "e100-wisdom-prompt-switching-intent",
    "e100-wisdom-prompt-eppo-coverage",
)

# ``WISDOM_CYPHER_<SUFFIX>`` for each flag above (no need to derive names from the key string).
WISDOM_CYPHER_ENV_SUFFIX_BY_FLAG_KEY: Dict[str, str] = {
    "e100-wisdom-prompt-competitive-displacement": "COMPETITIVE_DISPLACEMENT",
    "e100-wisdom-prompt-switching-intent": "SWITCHING_INTENT",
    "e100-wisdom-prompt-eppo-coverage": "EPPO_COVERAGE",
}


def _settings_path() -> Path:
    return Path(__file__).resolve().parent.parent / "config" / "settings.yaml"


def _load_fallback_prompt_from_yaml() -> str:
    path = _settings_path()
    if not path.exists():
        return ""
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    block = (data.get("wisdom") or {}).get("tier2_prompt_fallback") or ""
    return str(block).strip()


def resolve_wisdom_prompt_jobs(
    ld_client: Any,
    context: Any,
) -> Tuple[List[Tuple[str, str]], str]:
    """
    Returns ((flag_key, prompt_body), ...), source_label).

    Order: non-empty string flags (``WISDOM_PROMPT_FLAG_KEYS`` order), else YAML fallback
    as a single job keyed by ``settings.yaml``.
    """
    jobs: List[Tuple[str, str]] = []

    if ld_client is not None and context is not None:
        for flag_key in WISDOM_PROMPT_FLAG_KEYS:
            try:
                raw = ld_client.variation(flag_key, context, "")
            except Exception as e:
                print(f"[Tier2] Flag {flag_key!r} variation error — {e}")
                raw = ""
            text = raw if isinstance(raw, str) else str(raw or "")
            text = text.strip()
            if text:
                jobs.append((flag_key, text))
        if jobs:
            print(f"[Tier2] Loaded {len(jobs)} prompt(s) from LaunchDarkly string flags")
            return jobs, "LaunchDarkly string flags"

    fb = _load_fallback_prompt_from_yaml()
    if fb:
        print("[Tier2] Using wisdom.tier2_prompt_fallback from config/settings.yaml")
        return [("settings.yaml", fb)], "settings.yaml"

    raise WisdomMCPError(
        "No Tier 2 prompts: create string flags "
        f"{', '.join(WISDOM_PROMPT_FLAG_KEYS)} in LaunchDarkly, "
        "or add wisdom.tier2_prompt_fallback in config/settings.yaml."
    )

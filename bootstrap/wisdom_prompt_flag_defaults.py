"""
Default Wisdom prompt bodies for the three E100 string feature flags.

Keys must match agents.wisdom_prompts.WISDOM_PROMPT_FLAG_KEYS (flag key → prompt text for variation 0).
"""

from typing import Dict, Tuple

# (flag_key, human-readable name for LD UI)
WISDOM_FLAG_META: Tuple[Tuple[str, str], ...] = (
    (
        "e100-wisdom-prompt-competitive-displacement",
        "E100 Wisdom prompt — competitive displacement",
    ),
    (
        "e100-wisdom-prompt-switching-intent",
        "E100 Wisdom prompt — switching intent",
    ),
    (
        "e100-wisdom-prompt-eppo-coverage",
        "E100 Wisdom prompt — Eppo coverage",
    ),
)

# Full default variation value (string) per flag key — same intent as former Tier 2 AI Configs.
DEFAULT_PROMPT_BY_FLAG_KEY: Dict[str, str] = {
    "e100-wisdom-prompt-competitive-displacement": (
        "You guide Enterpret Wisdom queries for competitive displacement. Focus on customer "
        "accounts with the most mentions of Statsig, Optimizely, Eppo, or other A/B testing / "
        "experimentation tools in Gong (or similar sources) over roughly the last 12 months. "
        "For each account, summarize whether they are evaluating, frustrated, or actively using "
        "a competitor alongside LaunchDarkly. "
        "Normalize urgency: immediate (active switching intent) | active (evaluating) | "
        "watch (mentioned but no clear action). "
        "When the tool response allows, return a JSON array of objects: "
        "[{account_name, competitor, urgency, context or deal_context, quote?}]."
    ),
    "e100-wisdom-prompt-switching-intent": (
        "You guide Enterpret Wisdom queries for switching intent. Focus on accounts currently "
        "evaluating or running Statsig or Eppo alongside or instead of LaunchDarkly. "
        "Surface the strongest signals of dissatisfaction or active switching intent. "
        "Normalize urgency: immediate | active | watch. "
        "When possible, return a JSON array: "
        "[{account_name, competitor, urgency, deal_context, competitor_spend?, "
        "renewal_window_months?}]."
    ),
    "e100-wisdom-prompt-eppo-coverage": (
        "You guide Enterpret Wisdom queries for Eppo. Focus on accounts that mention Eppo in "
        "Gong calls or Zendesk tickets in roughly the last 12 months. "
        "For each, note whether Eppo appears to be expanding inside an existing LaunchDarkly "
        "customer relationship. "
        "Normalize urgency: immediate | active | watch. "
        "When possible, return a JSON array: "
        "[{account_name, competitor, urgency, context or deal_context, "
        "is_existing_ld_customer?}]."
    ),
}


def assert_keys_align_with_codebase() -> None:
    """Fail fast if bootstrap drifts from agents.wisdom_prompts."""
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))
    from agents.wisdom_prompts import (
        WISDOM_CYPHER_ENV_SUFFIX_BY_FLAG_KEY,
        WISDOM_PROMPT_FLAG_KEYS,
    )

    code_keys = tuple(WISDOM_PROMPT_FLAG_KEYS)
    meta_keys = tuple(k for k, _ in WISDOM_FLAG_META)
    if code_keys != meta_keys:
        raise SystemExit(
            f"bootstrap/wisdom_prompt_flag_defaults.py keys {meta_keys!r} must match "
            f"agents.wisdom_prompts.WISDOM_PROMPT_FLAG_KEYS {code_keys!r}"
        )
    for k in code_keys:
        if k not in DEFAULT_PROMPT_BY_FLAG_KEY:
            raise SystemExit(f"Missing DEFAULT_PROMPT_BY_FLAG_KEY[{k!r}]")
    if set(WISDOM_CYPHER_ENV_SUFFIX_BY_FLAG_KEY.keys()) != set(code_keys):
        raise SystemExit(
            "WISDOM_CYPHER_ENV_SUFFIX_BY_FLAG_KEY keys must match WISDOM_PROMPT_FLAG_KEYS"
        )

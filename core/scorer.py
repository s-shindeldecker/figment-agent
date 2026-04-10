"""
Heuristic expansion signals for E100 ranking (no LLM).

Weights come from ``config/settings.yaml`` scoring section where mirrored in constants
below. ``run.py`` always ranks via ``merge_and_score`` / this module.
"""
from pathlib import Path
from typing import Any, Dict

import yaml

from core.schema import AccountRecord

_SETTINGS_PATH = Path(__file__).resolve().parent.parent / "config" / "settings.yaml"

TIER1_WEIGHTS = {
    "zero_exp_usage": 3,
    "high_cmau_low_exp": 3,
    "open_opp": 3,
    "never_used_exp": 2,
    "utilisation_below_5pct": 2,
    "declining_trend": 2,
    "enterprise_plan": 1,
    "arr_over_200k": 1,
    "icp_rank_top_500": 1,
}

TIER2_URGENCY_SCORES = {
    "immediate": 3,
    "active": 2,
    "watch": 1,
}

_DEFAULT_TIER3_WEIGHTS: Dict[str, float] = {
    "competitor_hit": 2.0,
    "per_keyword": 0.75,
    "keyword_cap": 3.0,
    "dual_signal_bonus": 1.5,
    "urgency_multiplier": 0.35,
}


def _tier3_weights() -> Dict[str, float]:
    w = dict(_DEFAULT_TIER3_WEIGHTS)
    try:
        with open(_SETTINGS_PATH, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        raw = (data.get("scoring") or {}).get("tier3_weights")
        if isinstance(raw, dict):
            for k, v in raw.items():
                if k in w and v is not None:
                    w[k] = float(v)
    except Exception:
        pass
    return w


def score_tier1(account: AccountRecord) -> float:
    score = 0.0
    if (account.exp_events_mtd or 0) == 0:
        score += TIER1_WEIGHTS["zero_exp_usage"]
    if (account.exp_utilisation_rate or 1) < 0.05:
        score += TIER1_WEIGHTS["utilisation_below_5pct"]
    if account.plan in ("Enterprise", "Enterprise 2023", "Guardian"):
        score += TIER1_WEIGHTS["enterprise_plan"]
    if (account.arr or 0) >= 200000:
        score += TIER1_WEIGHTS["arr_over_200k"]
    return score


def score_tier2(account: AccountRecord) -> float:
    return float(TIER2_URGENCY_SCORES.get(account.urgency, 1))


def score_tier3(account: AccountRecord) -> float:
    tw = _tier3_weights()
    score = 0.0
    if account.competitor:
        score += tw["competitor_hit"]
    mk = (account.tier3_extras or {}).get("matched_keywords", "")
    nkw = len([x for x in mk.split(",") if x.strip()]) if mk else 0
    score += min(nkw * tw["per_keyword"], tw["keyword_cap"])
    if account.competitor and nkw:
        score += tw["dual_signal_bonus"]
    urg = float(TIER2_URGENCY_SCORES.get(account.urgency, 1))
    score += urg * tw["urgency_multiplier"]
    return score


def score(account: AccountRecord) -> float:
    if account.tier == 1:
        return score_tier1(account)
    if account.tier == 2:
        return score_tier2(account)
    if account.tier == 3:
        return score_tier3(account)
    return 0.0

"""
Heuristic expansion signals for E100 ranking (no LLM).

Weights come from ``config/settings.yaml`` scoring section where mirrored in constants
below. ``run.py`` always ranks via ``merge_and_score`` / this module.
"""
from core.schema import AccountRecord


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


def score(account: AccountRecord) -> float:
    if account.tier == 1:
        return score_tier1(account)
    elif account.tier == 2:
        return score_tier2(account)
    return 0.0

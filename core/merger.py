import os
from dataclasses import replace
from pathlib import Path
from typing import List, Optional, Tuple

import yaml

from core.deduplicator import merge_accounts
from core.schema import AccountRecord
from core.scorer import score

_SETTINGS_PATH = Path(__file__).resolve().parent.parent / "config" / "settings.yaml"


def _load_output_section() -> dict:
    try:
        with open(_SETTINGS_PATH, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return data.get("output") or {}
    except Exception:
        return {}


def _env_truthy(name: str) -> bool:
    return (os.getenv(name) or "").strip().lower() in ("1", "true", "yes", "on")


def e100_summary_use_tier_quotas() -> bool:
    """
    When True, the merged E100 list (Sheets summary, console, Slack) is capped
    with tier quotas plus score-based backfill. Set ``E100_SUMMARY_USE_FULL_MERGE=1``
    to keep the full ``merge_and_score`` list everywhere.
    """
    if _env_truthy("E100_SUMMARY_USE_FULL_MERGE"):
        return False
    out = _load_output_section()
    es = out.get("e100_summary")
    if isinstance(es, dict) and "use_tier_quotas" in es:
        return bool(es.get("use_tier_quotas"))
    return True


def _quota_from_config(name_env: str, yaml_key: str, default: int) -> int:
    raw = (os.getenv(name_env) or "").strip()
    if raw:
        try:
            return max(0, int(raw))
        except ValueError:
            pass
    out = _load_output_section()
    es = out.get("e100_summary") if isinstance(out.get("e100_summary"), dict) else {}
    v = es.get(yaml_key)
    if isinstance(v, int):
        return max(0, v)
    try:
        return max(0, int(v))
    except (TypeError, ValueError):
        return default


def e100_summary_quotas() -> Tuple[int, int, int]:
    """Max counts per merged tier (1/2/3) before backfilling from other tiers by score."""
    return (
        _quota_from_config("E100_SUMMARY_TIER1_MAX", "tier1_max", 50),
        _quota_from_config("E100_SUMMARY_TIER2_MAX", "tier2_max", 25),
        _quota_from_config("E100_SUMMARY_TIER3_MAX", "tier3_max", 25),
    )


def _account_key(a: AccountRecord) -> str:
    return (a.account_name or "").strip().lower()


def select_e100_summary_list(
    accounts_ranked: List[AccountRecord],
    tier1_max: int,
    tier2_max: int,
    tier3_max: int,
) -> List[AccountRecord]:
    """
    Build a summary list of up to (tier1_max + tier2_max + tier3_max) accounts.

    Takes up to ``tierN_max`` from each tier (using ``AccountRecord.tier`` on the
    merged row — same semantics as :func:`core.deduplicator.merge_accounts`).
    Shortfalls are filled with the next highest-scoring accounts not yet selected
    (global order follows ``accounts_ranked``).
    """
    target_total = tier1_max + tier2_max + tier3_max
    if target_total <= 0:
        return []

    p1 = [a for a in accounts_ranked if a.tier == 1]
    p2 = [a for a in accounts_ranked if a.tier == 2]
    p3 = [a for a in accounts_ranked if a.tier == 3]

    take1 = p1[:tier1_max]
    take2 = p2[:tier2_max]
    take3 = p3[:tier3_max]
    selected: List[AccountRecord] = take1 + take2 + take3
    selected_keys = {_account_key(a) for a in selected}
    need = target_total - len(selected)

    if need > 0:
        for a in accounts_ranked:
            k = _account_key(a)
            if not k or k in selected_keys:
                continue
            selected.append(a)
            selected_keys.add(k)
            need -= 1
            if need <= 0:
                break

    out = sorted(selected, key=lambda a: a.expansion_score or 0, reverse=True)
    for i, account in enumerate(out, start=1):
        account.priority_rank = i
    return out


def resolve_e100_summary_list(full_ranked: List[AccountRecord]) -> List[AccountRecord]:
    """
    Apply tier quotas + backfill when enabled; otherwise return ``full_ranked`` unchanged.
    """
    if not e100_summary_use_tier_quotas():
        return full_ranked
    q1, q2, q3 = e100_summary_quotas()
    return select_e100_summary_list(full_ranked, q1, q2, q3)


def clone_accounts_for_sheet_export(
    accounts: list[AccountRecord],
) -> list[AccountRecord]:
    """
    Shallow copies for per-tier Sheets tabs so tier-local scores/ranks do not
    mutate records shared with ``combined`` / ``merge_and_score``.
    """
    return [
        replace(a, expansion_score=None, priority_rank=None) for a in accounts
    ]


def score_and_rank_for_export(accounts: list[AccountRecord]) -> list[AccountRecord]:
    """
    Set expansion_score and per-tab priority_rank (1..n). Mutates the given
    records in place — pass clones from ``clone_accounts_for_sheet_export``.
    """
    for account in accounts:
        account.expansion_score = score(account)
    ranked = sorted(accounts, key=lambda a: a.expansion_score or 0, reverse=True)
    for i, account in enumerate(ranked, start=1):
        account.priority_rank = i
    return ranked


def merge_and_score(accounts: list[AccountRecord]) -> list[AccountRecord]:
    """
    Merge rows that share an account name across tiers, score, and rank.
    Returns list sorted by expansion_score descending with priority_rank set.
    """
    deduped = merge_accounts(accounts)

    for account in deduped:
        account.expansion_score = score(account)

    ranked = sorted(deduped, key=lambda a: a.expansion_score or 0, reverse=True)

    for i, account in enumerate(ranked, start=1):
        account.priority_rank = i

    return ranked

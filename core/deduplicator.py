"""
Merge accounts that share the same account name across tiers.

Looker (Tier 1) rows are the preferred base for commercial context; Tier 2
Enterpret fields overlay when the base value is empty. ``looker_extras``,
``wisdom_extras``, and ``tier3_extras`` merge with Looker keys winning in
``looker_extras`` and later rows winning in ``wisdom_extras`` / ``tier3_extras``.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
from typing import Dict, List, Optional, Set

from core.schema import AccountRecord

_URGENCY_RANK = {"immediate": 3, "active": 2, "watch": 1}


def _norm_name(name: Optional[str]) -> str:
    return (name or "").strip().lower()


def _is_empty_scalar(val) -> bool:
    if val is None:
        return True
    if isinstance(val, str):
        return not val.strip()
    return False


def _merge_str(base: Optional[str], overlay: Optional[str]) -> Optional[str]:
    if not _is_empty_scalar(base):
        return base
    return overlay if not _is_empty_scalar(overlay) else base


def _merge_optional_float(
    base: Optional[float], overlay: Optional[float]
) -> Optional[float]:
    if base is not None:
        return base
    return overlay


def _merge_optional_int(base: Optional[int], overlay: Optional[int]) -> Optional[int]:
    if base is not None:
        return base
    return overlay


def _merge_optional_bool(
    base: Optional[bool], overlay: Optional[bool]
) -> Optional[bool]:
    if base is not None:
        return base
    return overlay


def _merge_urgency(
    base: Optional[str], overlay: Optional[str]
) -> Optional[str]:
    b = _URGENCY_RANK.get((base or "").lower(), 0)
    o = _URGENCY_RANK.get((overlay or "").lower(), 0)
    m = max(b, o)
    for label, rank in _URGENCY_RANK.items():
        if rank == m:
            return label
    return base or overlay


def _merge_deal_context(
    base: Optional[str], overlay: Optional[str]
) -> Optional[str]:
    b = (base or "").strip()
    o = (overlay or "").strip()
    if not b:
        return o or None
    if not o:
        return b or None
    if b == o:
        return b
    return f"{b}\n---\n{o}"


def _merge_looker_extras(a: Dict[str, str], b: Dict[str, str]) -> Dict[str, str]:
    # b (first in merge order / base) wins over a for the same key
    return {**a, **b}


def _merge_wisdom_extras(a: Dict[str, str], b: Dict[str, str]) -> Dict[str, str]:
    # Later overlay wins
    return {**a, **b}


def _merge_tier3_extras(a: Dict[str, str], b: Dict[str, str]) -> Dict[str, str]:
    return {**a, **b}


def _merge_pair(base: AccountRecord, overlay: AccountRecord) -> AccountRecord:
    looker_extras = _merge_looker_extras(overlay.looker_extras, base.looker_extras)
    wisdom_extras = _merge_wisdom_extras(base.wisdom_extras, overlay.wisdom_extras)
    tier3_extras = _merge_tier3_extras(base.tier3_extras, overlay.tier3_extras)

    return replace(
        base,
        sfdc_account_id=_merge_str(base.sfdc_account_id, overlay.sfdc_account_id),
        ld_account_id=_merge_str(base.ld_account_id, overlay.ld_account_id),
        arr=_merge_optional_float(base.arr, overlay.arr),
        plan=_merge_str(base.plan, overlay.plan),
        rating=_merge_str(base.rating, overlay.rating),
        geo=_merge_str(base.geo, overlay.geo),
        industry=_merge_str(base.industry, overlay.industry),
        renewal_date=_merge_str(base.renewal_date, overlay.renewal_date),
        ae=_merge_str(base.ae, overlay.ae),
        csm=_merge_str(base.csm, overlay.csm),
        exp_events_mtd=_merge_optional_float(
            base.exp_events_mtd, overlay.exp_events_mtd
        ),
        exp_events_entitled=_merge_optional_float(
            base.exp_events_entitled, overlay.exp_events_entitled
        ),
        exp_utilisation_rate=_merge_optional_float(
            base.exp_utilisation_rate, overlay.exp_utilisation_rate
        ),
        is_using_exp_90d=_merge_optional_bool(
            base.is_using_exp_90d, overlay.is_using_exp_90d
        ),
        days_since_last_iteration=_merge_optional_float(
            base.days_since_last_iteration, overlay.days_since_last_iteration
        ),
        active_experiments=_merge_optional_int(
            base.active_experiments, overlay.active_experiments
        ),
        competitor=_merge_str(base.competitor, overlay.competitor),
        competitor_spend=_merge_optional_float(
            base.competitor_spend, overlay.competitor_spend
        ),
        renewal_window_months=_merge_optional_int(
            base.renewal_window_months, overlay.renewal_window_months
        ),
        urgency=_merge_urgency(base.urgency, overlay.urgency),
        deal_context=_merge_deal_context(base.deal_context, overlay.deal_context),
        notes=_merge_str(base.notes, overlay.notes),
        looker_extras=looker_extras,
        wisdom_extras=wisdom_extras,
        tier3_extras=tier3_extras,
    )


def _merge_account_group(group: List[AccountRecord]) -> AccountRecord:
    t1 = [a for a in group if a.tier == 1]
    t2 = [a for a in group if a.tier == 2]
    t3 = [a for a in group if a.tier not in (1, 2)]
    ordered = t1 + t2 + t3
    acc = ordered[0]
    for other in ordered[1:]:
        acc = _merge_pair(acc, other)

    tiers_seen: Set[int] = {a.tier for a in group if a.tier is not None}
    merged_tier = 1 if 1 in tiers_seen else (2 if 2 in tiers_seen else None)
    if 3 in tiers_seen and merged_tier is None:
        merged_tier = 3

    sources: List[str] = []
    for a in ordered:
        if a.source and a.source not in sources:
            sources.append(a.source)
    merged_source = "+".join(sources) if sources else acc.source

    note_extra = None
    if len(tiers_seen) > 1:
        note_extra = (
            f"Merged tiers {sorted(tiers_seen)} — dual motion where applicable"
        )
    new_notes = acc.notes
    if note_extra:
        if new_notes and note_extra not in new_notes:
            new_notes = f"{new_notes}\n{note_extra}"
        elif not new_notes:
            new_notes = note_extra

    return replace(
        acc,
        tier=merged_tier,
        source=merged_source,
        notes=new_notes,
    )


def merge_accounts(accounts: List[AccountRecord]) -> List[AccountRecord]:
    """
    One row per normalized account name, merging Tier 1 + Tier 2 (and Tier 3)
    into a single ``AccountRecord``.
    """
    buckets: Dict[str, List[AccountRecord]] = defaultdict(list)
    for account in accounts:
        key = _norm_name(account.account_name)
        if not key:
            continue
        buckets[key].append(account)

    out: List[AccountRecord] = []
    for _key, group in buckets.items():
        if len(group) == 1:
            out.append(group[0])
        else:
            out.append(_merge_account_group(group))
    return out


def deduplicate(accounts: List[AccountRecord]) -> List[AccountRecord]:
    """
    Deprecated name for :func:`merge_accounts` (kept for callers that still
    import ``deduplicate``).
    """
    return merge_accounts(accounts)

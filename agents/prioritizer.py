"""
Apply ranker output (e.g. JSON rows) onto ``AccountRecord`` lists — used by tests and hooks.

The main pipeline uses deterministic ``merge_and_score`` / ``core/scorer.py`` only.
"""
from __future__ import annotations

from typing import List

from core.schema import AccountRecord


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

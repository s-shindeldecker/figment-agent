from dataclasses import replace

from core.schema import AccountRecord
from core.scorer import score
from core.deduplicator import merge_accounts


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

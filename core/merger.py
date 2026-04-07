from core.schema import AccountRecord
from core.scorer import score
from core.deduplicator import deduplicate


def merge_and_score(accounts: list[AccountRecord]) -> list[AccountRecord]:
    """
    Deduplicate, score, and rank all accounts from all tiers.
    Returns list sorted by expansion_score descending with priority_rank set.
    """
    deduped = deduplicate(accounts)

    for account in deduped:
        account.expansion_score = score(account)

    ranked = sorted(deduped, key=lambda a: a.expansion_score or 0, reverse=True)

    for i, account in enumerate(ranked, start=1):
        account.priority_rank = i

    return ranked

from core.schema import AccountRecord
from core.scorer import score
from core.deduplicator import merge_accounts


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

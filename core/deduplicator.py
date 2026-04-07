from core.schema import AccountRecord


def deduplicate(accounts: list[AccountRecord]) -> list[AccountRecord]:
    """
    Accounts can appear in multiple tiers (e.g. existing LD customer
    also running a competitor). Keep both tier entries but flag the
    overlap so the team knows to pursue both motions.
    """
    seen: dict[str, AccountRecord] = {}
    for account in accounts:
        key = account.account_name.lower().strip()
        if key not in seen:
            seen[key] = account
        else:
            existing = seen[key]
            # If account appears in T1 and T2, flag it
            if existing.tier != account.tier:
                existing.notes = (
                    f"Also appears in Tier {account.tier} — "
                    f"dual motion: activation + displacement"
                )
    return list(seen.values())

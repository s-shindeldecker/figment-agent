"""Placeholder Tier 3 collector (future: agentic multi-source scraping)."""

from typing import List

from core.schema import AccountRecord


async def collect() -> List[AccountRecord]:
    print("[Tier3] Stub — no accounts (wire agentic collectors here later)")
    return []

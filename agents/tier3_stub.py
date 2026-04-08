"""Placeholder Tier 3 collector (future: agentic multi-source scraping)."""

from typing import Any, List

from core.schema import AccountRecord


async def collect(
    ai_client: Any = None,
    context: Any = None,
) -> List[AccountRecord]:
    print("[Tier3] Stub — no accounts (wire agentic collectors here later)")
    return []

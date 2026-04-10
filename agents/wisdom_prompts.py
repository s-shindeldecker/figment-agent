"""
Tier-2 Wisdom job identifiers and Cypher env suffix mapping.

Tier 2 is Cypher-only (``execute_cypher_query``); no prose / search_knowledge_graph path.
"""
from __future__ import annotations

from typing import Dict, List, Tuple

# Stable Tier-2 job identifiers (order preserved).
WISDOM_TIER2_JOB_KEYS: Tuple[str, ...] = (
    "e100-wisdom-prompt-competitive-displacement",
    "e100-wisdom-prompt-switching-intent",
)

# Backward-compatible alias (bootstrap / docs).
WISDOM_PROMPT_FLAG_KEYS = WISDOM_TIER2_JOB_KEYS

# ``WISDOM_CYPHER_<SUFFIX>`` per job key (suffix names are historical).
WISDOM_CYPHER_ENV_SUFFIX_BY_FLAG_KEY: Dict[str, str] = {
    "e100-wisdom-prompt-competitive-displacement": "COMPETITIVE_DISPLACEMENT",
    "e100-wisdom-prompt-switching-intent": "SWITCHING_INTENT",
}


def tier2_job_keys() -> List[str]:
    """Ordered Tier-2 job keys for the E100 Wisdom pipeline."""
    return list(WISDOM_TIER2_JOB_KEYS)

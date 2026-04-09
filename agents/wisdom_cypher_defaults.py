"""
Embedded default Cypher for Tier-2 Wisdom jobs (config/wisdom_cypher.yaml).

Resolved when env WISDOM_CYPHER_* is unset. Disabled with WISDOM_DISABLE_EMBEDDED_CYPHER=1.

Competitive displacement and switching intent use **two** embedded queries each (Gong + Zendesk).
Override a whole job with a single env string (WISDOM_CYPHER_COMPETITIVE_DISPLACEMENT, etc.)
to skip the split and run one query only.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Optional

import yaml

# LD suffix -> ordered YAML keys (run each non-empty query in order for that job)
_SUFFIX_TO_YAML_KEYS: Dict[str, List[str]] = {
    "COMPETITIVE_DISPLACEMENT": [
        "competitive_displacement_gong",
        "competitive_displacement_zendesk",
    ],
    "SWITCHING_INTENT": [
        "switching_intent_gong",
        "switching_intent_zendesk",
    ],
}

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "wisdom_cypher.yaml"
_cache: Optional[Dict[str, str]] = None


def _embedded_disabled() -> bool:
    v = (os.getenv("WISDOM_DISABLE_EMBEDDED_CYPHER") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _load_yaml_cypher_map() -> Dict[str, str]:
    global _cache
    if _cache is not None:
        return _cache
    out: Dict[str, str] = {}
    if not _CONFIG_PATH.is_file():
        _cache = out
        return out
    with open(_CONFIG_PATH, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        _cache = out
        return out
    for key, val in data.items():
        if isinstance(val, str) and val.strip():
            out[str(key)] = val.strip()
    _cache = out
    return out


def reload_wisdom_cypher_defaults_for_tests() -> None:
    """Clear cache (pytest / hot reload)."""
    global _cache
    _cache = None


def get_embedded_cypher_queries_for_suffix(ld_suffix: str) -> List[str]:
    """
    Return ordered non-empty embedded Cypher strings for this LD env suffix.
    """
    if _embedded_disabled():
        return []
    keys = _SUFFIX_TO_YAML_KEYS.get(ld_suffix)
    if not keys:
        return []
    m = _load_yaml_cypher_map()
    out: List[str] = []
    for k in keys:
        q = m.get(k)
        if q:
            out.append(q)
    return out


def get_embedded_cypher_for_suffix(ld_suffix: str) -> Optional[str]:
    """
    First embedded query for suffix, or None. Prefer ``get_embedded_cypher_queries_for_suffix``
    for multi-query jobs.
    """
    qs = get_embedded_cypher_queries_for_suffix(ld_suffix)
    return qs[0] if qs else None

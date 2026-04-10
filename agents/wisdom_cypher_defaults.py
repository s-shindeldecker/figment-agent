"""
Embedded default Cypher for Tier-2 Wisdom jobs (config/wisdom_cypher.yaml).

Resolved when env WISDOM_CYPHER_* is unset. Disabled with WISDOM_DISABLE_EMBEDDED_CYPHER=1.

LaunchDarkly: one JSON flag per wisdom map key (see agents.ld_wisdom_config); overlays YAML per key.
WISDOM_DISABLE_LD_CYPHER=1 skips all LD Cypher reads.

Competitive displacement and switching intent use **two** embedded queries each (Gong + Zendesk).
Override a whole job with a single env string (WISDOM_CYPHER_COMPETITIVE_DISPLACEMENT, etc.)
to skip the split and run one query only.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml

from agents import ld_wisdom_config as _ld_wisdom_config

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


def _read_cypher_map_from_yaml_file() -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not _CONFIG_PATH.is_file():
        return out
    with open(_CONFIG_PATH, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        return out
    for key, val in data.items():
        if isinstance(val, str) and val.strip():
            out[str(key)] = val.strip()
    return out


def _embedded_disabled() -> bool:
    v = (os.getenv("WISDOM_DISABLE_EMBEDDED_CYPHER") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _load_yaml_cypher_map() -> Dict[str, str]:
    """
    Effective Cypher map: repo YAML (unless WISDOM_DISABLE_EMBEDDED_CYPHER) merged with
    LaunchDarkly JSON (LD wins per key).
    """
    global _cache
    if _cache is not None:
        return _cache
    base: Dict[str, str] = {}
    if not _embedded_disabled():
        base = _read_cypher_map_from_yaml_file()
    ld_map = _ld_wisdom_config.get_wisdom_cypher_ld_overlay()
    merged = {**base, **ld_map}
    _cache = merged
    return merged


def reload_wisdom_cypher_defaults_for_tests() -> None:
    """Clear cache (pytest / hot reload)."""
    global _cache
    _cache = None


def describe_embedded_cypher_key_sources(ld_suffix: str) -> List[Tuple[str, str]]:
    """
    For each ordered map key that has a non-empty merged Cypher string, return
    ``(yaml_key, 'launchdarkly' | 'yaml')`` indicating whether that snippet came from
    the LaunchDarkly flag or from ``config/wisdom_cypher.yaml`` (after merge).
    """
    keys = _SUFFIX_TO_YAML_KEYS.get(ld_suffix)
    if not keys:
        return []
    merged = _load_yaml_cypher_map()
    ld_map = _ld_wisdom_config.get_wisdom_cypher_ld_overlay()
    out: List[Tuple[str, str]] = []
    for k in keys:
        if not (merged.get(k) or "").strip():
            continue
        src = "launchdarkly" if (ld_map.get(k) or "").strip() else "yaml"
        out.append((k, src))
    return out


def get_embedded_cypher_queries_for_suffix(ld_suffix: str) -> List[str]:
    """
    Return ordered non-empty Cypher strings for this suffix from the effective map
    (repo YAML unless ``WISDOM_DISABLE_EMBEDDED_CYPHER``, merged with LaunchDarkly).
    """
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

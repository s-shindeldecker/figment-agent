"""
LaunchDarkly JSON flags for Wisdom Tier-2 Cypher and prompt overlays.

Requires optional dependency: pip install "figment-agent[launchdarkly]"
and a server-side SDK key: ``LD_SDK_KEY`` (preferred) or ``LAUNCHDARKLY_SDK_KEY``.
When the SDK is missing or the key is unset, all functions return empty dicts
and the pipeline uses YAML only.

Optional ``LD_PROJECT_KEY`` and ``LD_ENV_KEY`` are attached as context attributes
(``projectKey``, ``environmentKey``) for LaunchDarkly targeting rules.
"""

from __future__ import annotations

import os
import threading
from typing import Any, Dict, Optional

DEFAULT_FLAG_WISDOM_CYPHER = "figment-wisdom-cypher"
DEFAULT_FLAG_WISDOM_PROMPTS = "figment-wisdom-tier2-prompts"

_init_lock = threading.Lock()
_client_configured = False


def _env_first(*names: str) -> str:
    for n in names:
        v = (os.getenv(n) or "").strip()
        if v:
            return v
    return ""


def _ld_cypher_disabled() -> bool:
    v = (os.getenv("WISDOM_DISABLE_LD_CYPHER") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _sdk_key() -> str:
    return _env_first("LD_SDK_KEY", "LAUNCHDARKLY_SDK_KEY")


def _flag_key_cypher() -> str:
    return (
        _env_first("LD_FLAG_WISDOM_CYPHER", "LAUNCHDARKLY_FLAG_WISDOM_CYPHER")
        or DEFAULT_FLAG_WISDOM_CYPHER
    )


def _flag_key_prompts() -> str:
    return (
        _env_first("LD_FLAG_WISDOM_PROMPTS", "LAUNCHDARKLY_FLAG_WISDOM_PROMPTS")
        or DEFAULT_FLAG_WISDOM_PROMPTS
    )


def _evaluation_context() -> Optional[Any]:
    try:
        from ldclient import Context
    except ImportError:
        return None
    key = (
        _env_first("LD_CONTEXT_KEY", "LAUNCHDARKLY_CONTEXT_KEY") or "figment-e100"
    )
    b = Context.builder(key).kind("service")
    project = _env_first("LD_PROJECT_KEY")
    if project:
        b = b.set("projectKey", project)
    env_name = _env_first("LD_ENV_KEY")
    if env_name:
        b = b.set("environmentKey", env_name)
    return b.build()


def _get_client():
    """Return shared LD client, or None if unavailable."""
    global _client_configured
    key = _sdk_key()
    if not key:
        return None
    try:
        import ldclient
        from ldclient.config import Config
    except ImportError:
        return None
    with _init_lock:
        if not _client_configured:
            ldclient.set_config(Config(key))
            _client_configured = True
        try:
            return ldclient.get()
        except Exception:
            return None


def _normalize_string_map(raw: Any) -> Dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, str] = {}
    for k, v in raw.items():
        sk = str(k).strip()
        if not sk:
            continue
        if isinstance(v, str) and v.strip():
            out[sk] = v.strip()
    return out


def get_wisdom_cypher_map_from_ld() -> Dict[str, str]:
    """
    JSON flag variation: object whose keys match config/wisdom_cypher.yaml.
    Non-string or empty values are skipped.
    """
    if _ld_cypher_disabled():
        return {}
    client = _get_client()
    ctx = _evaluation_context()
    if client is None or ctx is None:
        return {}
    try:
        raw = client.variation(_flag_key_cypher(), ctx, {})
    except Exception:
        return {}
    return _normalize_string_map(raw)


def get_wisdom_prompts_overlay_from_ld() -> Dict[str, Any]:
    """
    JSON flag for wisdom.* overlay. Expected keys (all optional):
    - tier2_prompt_fallback, tier2_prompt_default: strings
    - tier2_prompts: object mapping job_key -> prompt string
    """
    client = _get_client()
    ctx = _evaluation_context()
    if client is None or ctx is None:
        return {}
    try:
        raw = client.variation(_flag_key_prompts(), ctx, {})
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    return raw


def reset_ld_wisdom_client_for_tests() -> None:
    """Close singleton and allow re-init (pytest)."""
    global _client_configured
    with _init_lock:
        _client_configured = False
        try:
            import ldclient

            ldclient._reset_client()
        except Exception:
            pass

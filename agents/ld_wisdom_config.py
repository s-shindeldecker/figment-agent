"""
LaunchDarkly flags for Wisdom Tier-2: Cypher (JSON per map key) and optional log verbosity.

**Cypher:** each flag variation should be a JSON object with a string field, e.g.
``{"cypher": "MATCH ..."}``. Aliases: ``statement``, ``query``.

**Log verbosity:** multivariate string flag (default ``figment-agent-tier2-log-verbosity``)
with values ``off``, ``basic``, ``debug``, ``monitor`` (or JSON ``{"level": "debug"}``).
Gated console detail in ``agents.tier2_enterpret``. Env ``WISDOM_TIER2_LOG_VERBOSITY``
overrides LD when set.

Requires optional dependency: pip install "figment-agent[launchdarkly]"
and ``LD_SDK_KEY`` (or ``LAUNCHDARKLY_SDK_KEY``). When the SDK is missing or the key
is unset, LD returns nothing and the pipeline uses YAML/env only.

Optional ``LD_PROJECT_KEY`` and ``LD_ENV_KEY`` are context attributes for targeting.
"""

from __future__ import annotations

import os
import re
import threading
from typing import Any, Dict, Optional, Tuple

# ---------------------------------------------------------------------------
# Tier-2 pipeline log verbosity (string LD flag + env override)
# ---------------------------------------------------------------------------

# Multivariate string flag; variation values: off, basic, debug, monitor (case-insensitive).
# Or JSON object with string field level | verbosity | value | mode.
DEFAULT_FLAG_TIER2_LOG_VERBOSITY = "figment-agent-tier2-log-verbosity"

TIER2_LOG_OFF = 0
TIER2_LOG_BASIC = 1
TIER2_LOG_DEBUG = 2
TIER2_LOG_MONITOR = 3

_tier2_log_verbosity_cache: Optional[int] = None
_tier2_log_verbosity_lock = threading.Lock()

# Ordered keys matching config/wisdom_cypher.yaml blocks used by Tier-2 jobs.
WISDOM_CYPHER_MAP_KEYS: Tuple[str, ...] = (
    "competitive_displacement_gong",
    "competitive_displacement_zendesk",
    "switching_intent_gong",
    "switching_intent_zendesk",
)

# Default LaunchDarkly flag key per wisdom map key (override via LD_FLAG_CYPHER_<SUFFIX>).
_DEFAULT_LD_FLAG_KEY_BY_MAP_KEY: Dict[str, str] = {
    "competitive_displacement_gong": (
        "figment-wisdom-cypher-competitive-displacement-gong"
    ),
    "competitive_displacement_zendesk": (
        "figment-wisdom-cypher-competitive-displacement-zendesk"
    ),
    "switching_intent_gong": "figment-wisdom-cypher-switching-intent-gong",
    "switching_intent_zendesk": "figment-wisdom-cypher-switching-intent-zendesk",
}

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


def _ld_log_verbosity_disabled() -> bool:
    v = (os.getenv("WISDOM_DISABLE_LD_LOG_VERBOSITY") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def reset_tier2_log_verbosity_cache() -> None:
    """Clear cached Tier-2 log level (pytest / after LD env changes)."""
    global _tier2_log_verbosity_cache
    with _tier2_log_verbosity_lock:
        _tier2_log_verbosity_cache = None


def _verbosity_token_to_level(token: str) -> int:
    t = (token or "").strip().lower()
    if t in ("", "off", "none", "false", "no", "0"):
        return TIER2_LOG_OFF
    if t in ("basic", "info", "normal", "1"):
        return TIER2_LOG_BASIC
    if t in ("debug", "verbose", "trace", "2"):
        return TIER2_LOG_DEBUG
    if t in ("monitor", "metrics", "3"):
        return TIER2_LOG_MONITOR
    return TIER2_LOG_OFF


def _raw_to_verbosity_string(raw: Any) -> str:
    if raw is None:
        return ""
    if isinstance(raw, str):
        return raw
    if isinstance(raw, dict):
        for k in ("level", "verbosity", "value", "mode"):
            v = raw.get(k)
            if isinstance(v, str) and v.strip():
                return v
    return str(raw) if raw else ""


def _resolve_tier2_log_verbosity_level_uncached() -> int:
    """
    Env ``WISDOM_TIER2_LOG_VERBOSITY`` wins when set (local / CI without LD).
    Otherwise evaluate string flag ``LD_FLAG_LOG_VERBOSITY`` or default key.
    """
    env_v = _env_first("WISDOM_TIER2_LOG_VERBOSITY")
    if env_v:
        return _verbosity_token_to_level(env_v)
    if _ld_log_verbosity_disabled():
        return TIER2_LOG_OFF
    client = _get_client()
    ctx = _evaluation_context()
    if client is None or ctx is None:
        return TIER2_LOG_OFF
    flag_key = _env_first("LD_FLAG_LOG_VERBOSITY") or DEFAULT_FLAG_TIER2_LOG_VERBOSITY
    try:
        raw = client.variation(flag_key, ctx, "")
    except Exception:
        return TIER2_LOG_OFF
    return _verbosity_token_to_level(_raw_to_verbosity_string(raw))


def get_tier2_log_verbosity_level() -> int:
    """
    Cached Tier-2 console instrumentation level for this process.

    ``TIER2_LOG_BASIC``: flag keys + MCP row counts per Cypher call.
    ``TIER2_LOG_DEBUG``: per Cypher-flag evaluation detail (types, lengths).
    ``TIER2_LOG_MONITOR``: one JSON summary line at end of Tier-2 (extensible for alerts).
    """
    global _tier2_log_verbosity_cache
    with _tier2_log_verbosity_lock:
        if _tier2_log_verbosity_cache is not None:
            return _tier2_log_verbosity_cache
    level = _resolve_tier2_log_verbosity_level_uncached()
    with _tier2_log_verbosity_lock:
        _tier2_log_verbosity_cache = level
    return level


def tier2_log_verbosity_source_hint() -> str:
    """Short hint for console: where the effective verbosity was chosen."""
    if _env_first("WISDOM_TIER2_LOG_VERBOSITY"):
        return "env WISDOM_TIER2_LOG_VERBOSITY"
    if _ld_log_verbosity_disabled():
        return "WISDOM_DISABLE_LD_LOG_VERBOSITY (LD log flag skipped)"
    if not _sdk_key():
        return "no LD_SDK_KEY (verbosity off)"
    fk = _env_first("LD_FLAG_LOG_VERBOSITY") or DEFAULT_FLAG_TIER2_LOG_VERBOSITY
    return f"LaunchDarkly flag {fk}"


_TIER2_LOG_LEVEL_NAMES = {
    TIER2_LOG_OFF: "off",
    TIER2_LOG_BASIC: "basic",
    TIER2_LOG_DEBUG: "debug",
    TIER2_LOG_MONITOR: "monitor",
}


def tier2_log_level_name(level: int) -> str:
    return _TIER2_LOG_LEVEL_NAMES.get(level, str(level))


def _sdk_key() -> str:
    return _env_first("LD_SDK_KEY", "LAUNCHDARKLY_SDK_KEY")


def _map_key_to_env_suffix(map_key: str) -> str:
    """competitive_displacement_gong -> COMPETITIVE_DISPLACEMENT_GONG"""
    return re.sub(r"[^A-Za-z0-9]+", "_", map_key).upper().strip("_")


def ld_flag_key_for_cypher_map_key(map_key: str) -> str:
    """LaunchDarkly flag key for this wisdom YAML block key."""
    suffix = _map_key_to_env_suffix(map_key)
    env_name = f"LD_FLAG_CYPHER_{suffix}"
    return _env_first(env_name) or _DEFAULT_LD_FLAG_KEY_BY_MAP_KEY.get(
        map_key, f"figment-wisdom-cypher-{map_key.replace('_', '-')}"
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


def cypher_from_ld_variation(raw: Any) -> Optional[str]:
    """
    Extract Cypher string from a LaunchDarkly variation (JSON object or string).
    """
    if raw is None:
        return None
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    if isinstance(raw, dict):
        for name in ("cypher", "statement", "query"):
            v = raw.get(name)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None


def get_ld_cypher_for_map_key(map_key: str) -> Optional[str]:
    """
    Evaluate the dedicated LD flag for this wisdom map key and return Cypher, or None.
    """
    if _ld_cypher_disabled():
        return None
    client = _get_client()
    ctx = _evaluation_context()
    if client is None or ctx is None:
        return None
    flag_key = ld_flag_key_for_cypher_map_key(map_key)
    verbosity = get_tier2_log_verbosity_level()
    raw: Any = None
    try:
        raw = client.variation(flag_key, ctx, None)
    except Exception as ex:
        if verbosity >= TIER2_LOG_DEBUG:
            print(
                f"[Tier2][ld] map_key={map_key} flag={flag_key} "
                f"variation_error={type(ex).__name__}: {ex}"
            )
        return None
    cy = cypher_from_ld_variation(raw)
    if verbosity >= TIER2_LOG_DEBUG:
        rt = type(raw).__name__
        if cy:
            print(
                f"[Tier2][ld] map_key={map_key} flag={flag_key} raw_type={rt} "
                f"cypher_chars={len(cy)}"
            )
        else:
            print(
                f"[Tier2][ld] map_key={map_key} flag={flag_key} raw_type={rt} "
                "cypher_extracted=(empty)"
            )
    return cy


def get_wisdom_cypher_ld_overlay() -> Dict[str, str]:
    """
    Non-empty Cypher strings from LaunchDarkly, keyed like config/wisdom_cypher.yaml.
    One ``variation()`` call per known map key.
    """
    out: Dict[str, str] = {}
    for mk in WISDOM_CYPHER_MAP_KEYS:
        cy = get_ld_cypher_for_map_key(mk)
        if cy:
            out[mk] = cy
    return out


def reset_ld_wisdom_client_for_tests() -> None:
    """Close singleton and allow re-init (pytest)."""
    global _client_configured
    reset_tier2_log_verbosity_cache()
    with _init_lock:
        _client_configured = False
        try:
            import ldclient

            ldclient._reset_client()
        except Exception:
            pass
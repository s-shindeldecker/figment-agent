"""Tier-2 log verbosity: env override and token mapping (LaunchDarkly flag is integration-tested manually)."""

import pytest

from agents import ld_wisdom_config as lc


@pytest.fixture(autouse=True)
def _reset_verbosity_cache():
    lc.reset_tier2_log_verbosity_cache()
    yield
    lc.reset_tier2_log_verbosity_cache()


def test_verbosity_token_mapping():
    assert lc._verbosity_token_to_level("") == lc.TIER2_LOG_OFF
    assert lc._verbosity_token_to_level("OFF") == lc.TIER2_LOG_OFF
    assert lc._verbosity_token_to_level("basic") == lc.TIER2_LOG_BASIC
    assert lc._verbosity_token_to_level("1") == lc.TIER2_LOG_BASIC
    assert lc._verbosity_token_to_level("DEBUG") == lc.TIER2_LOG_DEBUG
    assert lc._verbosity_token_to_level("monitor") == lc.TIER2_LOG_MONITOR
    assert lc._verbosity_token_to_level("3") == lc.TIER2_LOG_MONITOR


def test_verbosity_env_overrides(monkeypatch):
    monkeypatch.setenv("WISDOM_TIER2_LOG_VERBOSITY", "debug")
    lc.reset_tier2_log_verbosity_cache()
    assert lc.get_tier2_log_verbosity_level() == lc.TIER2_LOG_DEBUG
    monkeypatch.delenv("WISDOM_TIER2_LOG_VERBOSITY", raising=False)
    lc.reset_tier2_log_verbosity_cache()


def test_raw_to_verbosity_string():
    assert lc._raw_to_verbosity_string(None) == ""
    assert lc._raw_to_verbosity_string("basic") == "basic"
    assert lc._raw_to_verbosity_string({"level": "debug"}) == "debug"
    assert lc._raw_to_verbosity_string({"verbosity": "monitor"}) == "monitor"


def test_tier2_log_level_name():
    assert lc.tier2_log_level_name(lc.TIER2_LOG_BASIC) == "basic"


def test_source_hint_env(monkeypatch):
    monkeypatch.setenv("WISDOM_TIER2_LOG_VERBOSITY", "basic")
    assert "WISDOM_TIER2_LOG_VERBOSITY" in lc.tier2_log_verbosity_source_hint()
    monkeypatch.delenv("WISDOM_TIER2_LOG_VERBOSITY", raising=False)

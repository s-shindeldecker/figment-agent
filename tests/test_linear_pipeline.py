import pytest

from agents.prioritizer import apply_prioritizer_response, prioritizer_llm_requested
from agents.tier2_enterpret import WisdomMCPError, execute_wisdom_tier2_jobs
from agents.wisdom_prompts import (
    WISDOM_CYPHER_ENV_SUFFIX_BY_FLAG_KEY,
    WISDOM_PROMPT_FLAG_KEYS,
    WISDOM_TIER2_JOB_KEYS,
    tier2_job_keys,
)
from core.merger import merge_and_score
from core.schema import AccountRecord


def test_tier2_job_keys_order_and_count():
    keys = tier2_job_keys()
    assert keys == list(WISDOM_TIER2_JOB_KEYS)
    assert len(keys) == 2


def test_wisdom_tier2_job_keys_alias_matches_flag_keys_tuple():
    assert WISDOM_PROMPT_FLAG_KEYS == WISDOM_TIER2_JOB_KEYS


def test_wisdom_cypher_env_suffixes_align_with_job_keys():
    assert set(WISDOM_CYPHER_ENV_SUFFIX_BY_FLAG_KEY) == set(WISDOM_TIER2_JOB_KEYS)


@pytest.mark.asyncio
async def test_execute_wisdom_tier2_jobs_raises_when_no_cypher(monkeypatch):
    monkeypatch.setenv("WISDOM_AUTH_TOKEN", "fake-token-for-test")
    monkeypatch.setenv("WISDOM_DISABLE_EMBEDDED_CYPHER", "1")
    monkeypatch.delenv("WISDOM_CYPHER_COMPETITIVE_DISPLACEMENT", raising=False)
    monkeypatch.delenv("WISDOM_CYPHER_SWITCHING_INTENT", raising=False)
    monkeypatch.delenv("WISDOM_CYPHER", raising=False)
    monkeypatch.setattr(
        "agents.ld_wisdom_config.get_wisdom_cypher_ld_overlay",
        lambda: {},
    )
    from agents.wisdom_cypher_defaults import reload_wisdom_cypher_defaults_for_tests

    reload_wisdom_cypher_defaults_for_tests()
    with pytest.raises(WisdomMCPError, match="Tier 2 requires Cypher"):
        await execute_wisdom_tier2_jobs(tier2_job_keys(), log_prefix="[Tier2]")


def test_apply_prioritizer_response_sets_ranks():
    accounts = [
        AccountRecord(account_name="Beta", tier=1, source="looker"),
        AccountRecord(account_name="Acme", tier=2, source="enterpret"),
    ]
    rows = [
        {"account_name": "Acme", "priority_rank": 1, "expansion_score": 9.0, "notes": "hot"},
        {"account_name": "Beta", "priority_rank": 2, "expansion_score": 3.0},
    ]
    out = apply_prioritizer_response(accounts, rows)
    assert [a.account_name for a in out] == ["Acme", "Beta"]
    assert out[0].priority_rank == 1 and out[0].expansion_score == 9.0
    assert out[0].notes == "hot"
    assert out[1].priority_rank == 2


def test_prioritizer_llm_requested_default_and_deterministic(monkeypatch):
    monkeypatch.delenv("E100_PRIORITIZER_MODE", raising=False)
    assert prioritizer_llm_requested() is True
    monkeypatch.setenv("E100_PRIORITIZER_MODE", "deterministic")
    assert prioritizer_llm_requested() is False
    monkeypatch.setenv("E100_PRIORITIZER_MODE", "off")
    assert prioritizer_llm_requested() is False


def test_merge_and_score_fallback_without_prioritizer():
    accounts = [
        AccountRecord(account_name="X", tier=1, source="looker", arr=100000, plan="Enterprise"),
    ]
    ranked = merge_and_score(accounts)
    assert len(ranked) == 1
    assert ranked[0].priority_rank == 1

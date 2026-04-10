import pytest

from agents.prioritizer import apply_prioritizer_response
from agents.wisdom_mcp import WisdomMCPError
from agents.wisdom_prompts import (
    WISDOM_CYPHER_ENV_SUFFIX_BY_FLAG_KEY,
    WISDOM_PROMPT_FLAG_KEYS,
    WISDOM_TIER2_JOB_KEYS,
    resolve_wisdom_prompt_jobs,
)
from core.merger import merge_and_score
from core.schema import AccountRecord


def test_resolve_wisdom_prompt_jobs_two_jobs_from_settings(monkeypatch):
    monkeypatch.setattr(
        "agents.wisdom_prompts._read_settings_dict",
        lambda: {
            "wisdom": {
                "tier2_prompt_fallback": "shared instructions for all jobs",
            }
        },
    )
    jobs, src = resolve_wisdom_prompt_jobs()
    assert src == "config/settings.yaml+ld"
    assert len(jobs) == 2
    assert [j[0] for j in jobs] == list(WISDOM_TIER2_JOB_KEYS)
    assert all(j[1] == "shared instructions for all jobs" for j in jobs)


def test_resolve_wisdom_tier2_prompts_override_per_job(monkeypatch):
    keys = list(WISDOM_TIER2_JOB_KEYS)
    monkeypatch.setattr(
        "agents.wisdom_prompts._read_settings_dict",
        lambda: {
            "wisdom": {
                "tier2_prompt_fallback": "default",
                "tier2_prompts": {
                    keys[0]: "competitive only",
                    keys[1]: "",
                },
            }
        },
    )
    jobs, _ = resolve_wisdom_prompt_jobs()
    assert jobs[0][1] == "competitive only"
    assert jobs[1][1] == "default"


def test_resolve_wisdom_missing_prompt_raises(monkeypatch):
    monkeypatch.setattr(
        "agents.wisdom_prompts._read_settings_dict",
        lambda: {"wisdom": {}},
    )
    with pytest.raises(WisdomMCPError, match="tier2_prompt"):
        resolve_wisdom_prompt_jobs()


def test_wisdom_tier2_job_keys_alias_matches_flag_keys_tuple():
    assert WISDOM_PROMPT_FLAG_KEYS == WISDOM_TIER2_JOB_KEYS


def test_wisdom_cypher_env_suffixes_align_with_job_keys():
    assert set(WISDOM_CYPHER_ENV_SUFFIX_BY_FLAG_KEY) == set(WISDOM_TIER2_JOB_KEYS)


def test_resolve_wisdom_ld_prompt_fallback_overrides_yaml(monkeypatch):
    import agents.wisdom_prompts as wp

    monkeypatch.setattr(
        wp,
        "get_wisdom_prompts_overlay_from_ld",
        lambda: {"tier2_prompt_fallback": "instructions from LaunchDarkly"},
    )
    monkeypatch.setattr(
        wp,
        "_read_settings_dict",
        lambda: wp._merge_wisdom_ld_overlay(
            {"wisdom": {"tier2_prompt_fallback": "yaml fallback"}}
        ),
    )
    jobs, src = resolve_wisdom_prompt_jobs()
    assert src == "config/settings.yaml+ld"
    assert all(j[1] == "instructions from LaunchDarkly" for j in jobs)


def test_resolve_wisdom_ld_tier2_prompts_merge_with_yaml(monkeypatch):
    import agents.wisdom_prompts as wp

    keys = list(WISDOM_TIER2_JOB_KEYS)
    monkeypatch.setattr(
        wp,
        "get_wisdom_prompts_overlay_from_ld",
        lambda: {
            "tier2_prompts": {keys[1]: "switching from LD"},
        },
    )
    monkeypatch.setattr(
        wp,
        "_read_settings_dict",
        lambda: wp._merge_wisdom_ld_overlay(
            {
                "wisdom": {
                    "tier2_prompt_fallback": "shared",
                    "tier2_prompts": {keys[0]: "competitive yaml"},
                }
            }
        ),
    )
    jobs, _ = resolve_wisdom_prompt_jobs()
    assert jobs[0][1] == "competitive yaml"
    assert jobs[1][1] == "switching from LD"


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


def test_merge_and_score_fallback_without_prioritizer():
    accounts = [
        AccountRecord(account_name="X", tier=1, source="looker", arr=100000, plan="Enterprise"),
    ]
    ranked = merge_and_score(accounts)
    assert len(ranked) == 1
    assert ranked[0].priority_rank == 1

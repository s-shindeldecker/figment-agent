import pytest

from bootstrap.wisdom_prompt_flag_defaults import assert_keys_align_with_codebase

from agents.prioritizer import apply_prioritizer_response
from agents.wisdom_prompts import WISDOM_PROMPT_FLAG_KEYS, resolve_wisdom_prompt_jobs
from core.merger import merge_and_score
from core.schema import AccountRecord


def test_resolve_wisdom_from_string_flags():
    class MockLD:
        def variation(self, key, context, default):
            mapping = {
                "e100-wisdom-prompt-competitive-displacement": "Query A body",
                "e100-wisdom-prompt-switching-intent": "",
                "e100-wisdom-prompt-eppo-coverage": "Query C body",
            }
            return mapping.get(key, default)

    jobs, src = resolve_wisdom_prompt_jobs(MockLD(), object())
    assert src == "LaunchDarkly string flags"
    assert jobs == [
        ("e100-wisdom-prompt-competitive-displacement", "Query A body"),
        ("e100-wisdom-prompt-eppo-coverage", "Query C body"),
    ]


def test_resolve_wisdom_flag_order_matches_wisdom_prompt_flag_keys():
    """Non-empty flags appear in WISDOM_PROMPT_FLAG_KEYS order."""
    keys = list(WISDOM_PROMPT_FLAG_KEYS)

    class MockLD:
        def variation(self, key, context, default):
            if key == keys[2]:
                return "third"
            if key == keys[0]:
                return "first"
            return ""

    jobs, _ = resolve_wisdom_prompt_jobs(MockLD(), object())
    assert [j[0] for j in jobs] == [keys[0], keys[2]]
    assert jobs[0][1] == "first"
    assert jobs[1][1] == "third"


def test_resolve_wisdom_yaml_fallback(monkeypatch):
    monkeypatch.setattr(
        "agents.wisdom_prompts._load_fallback_prompt_from_yaml",
        lambda: "fallback prompt body",
    )
    jobs, src = resolve_wisdom_prompt_jobs(None, None)
    assert src == "settings.yaml"
    assert jobs == [("settings.yaml", "fallback prompt body")]


def test_resolve_wisdom_all_flags_empty_uses_yaml(monkeypatch):
    monkeypatch.setattr(
        "agents.wisdom_prompts._load_fallback_prompt_from_yaml",
        lambda: "yaml only",
    )

    class MockLD:
        def variation(self, key, context, default):
            return ""

    jobs, src = resolve_wisdom_prompt_jobs(MockLD(), object())
    assert src == "settings.yaml"
    assert jobs == [("settings.yaml", "yaml only")]


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


def test_wisdom_bootstrap_defaults_align_with_wisdom_prompts():
    assert_keys_align_with_codebase()


def test_merge_and_score_fallback_without_prioritizer():
    accounts = [
        AccountRecord(account_name="X", tier=1, source="looker", arr=100000, plan="Enterprise"),
    ]
    ranked = merge_and_score(accounts)
    assert len(ranked) == 1
    assert ranked[0].priority_rank == 1

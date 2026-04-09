import pytest

from agents.tier2_enterpret import _cyphers_for_job_key, _build_deal_context_from_row
from agents.wisdom_cypher_defaults import (
    get_embedded_cypher_queries_for_suffix,
    reload_wisdom_cypher_defaults_for_tests,
)
from agents.tier2_enterpret import Tier2EntrepretAgent


@pytest.fixture(autouse=True)
def _reload_cypher_cache():
    reload_wisdom_cypher_defaults_for_tests()
    yield
    reload_wisdom_cypher_defaults_for_tests()


def test_embedded_cypher_for_competitive_flag(monkeypatch):
    monkeypatch.delenv("WISDOM_CYPHER_COMPETITIVE_DISPLACEMENT", raising=False)
    monkeypatch.delenv("WISDOM_CYPHER_E100_WISDOM_PROMPT_COMPETITIVE_DISPLACEMENT", raising=False)
    monkeypatch.delenv("WISDOM_CYPHER", raising=False)
    monkeypatch.delenv("WISDOM_DISABLE_EMBEDDED_CYPHER", raising=False)
    qs = _cyphers_for_job_key("e100-wisdom-prompt-competitive-displacement")
    assert len(qs) == 2
    assert "MATCH (nli:NaturalLanguageInteraction)" in qs[0]
    assert "LIMIT 50" in qs[0]
    assert "CONTAINS 'Zendesk'" in qs[1]
    assert "zendesksupport_organization_name" in qs[1]


def test_embedded_cypher_for_switching_flag(monkeypatch):
    monkeypatch.delenv("WISDOM_CYPHER_SWITCHING_INTENT", raising=False)
    monkeypatch.delenv("WISDOM_CYPHER", raising=False)
    monkeypatch.delenv("WISDOM_DISABLE_EMBEDDED_CYPHER", raising=False)
    qs = _cyphers_for_job_key("e100-wisdom-prompt-switching-intent")
    assert len(qs) == 2
    assert "CONTAINS 'Gong'" in qs[0]
    assert "switching_intent" in qs[0]  # source_query in Gong block
    assert "CONTAINS 'Zendesk'" in qs[1]
    assert "switching_intent" in qs[1]


def test_embedded_suffix_returns_gong_zendesk_pairs(monkeypatch):
    monkeypatch.delenv("WISDOM_DISABLE_EMBEDDED_CYPHER", raising=False)
    comp = get_embedded_cypher_queries_for_suffix("COMPETITIVE_DISPLACEMENT")
    sw = get_embedded_cypher_queries_for_suffix("SWITCHING_INTENT")
    assert len(comp) == 2 and len(sw) == 2
    assert get_embedded_cypher_queries_for_suffix("EPPO_COVERAGE") == []


def test_embedded_cypher_disabled(monkeypatch):
    monkeypatch.delenv("WISDOM_CYPHER_COMPETITIVE_DISPLACEMENT", raising=False)
    monkeypatch.delenv("WISDOM_CYPHER", raising=False)
    monkeypatch.setenv("WISDOM_DISABLE_EMBEDDED_CYPHER", "1")
    assert _cyphers_for_job_key("e100-wisdom-prompt-competitive-displacement") == []


def test_env_overrides_embedded(monkeypatch):
    monkeypatch.setenv("WISDOM_CYPHER_COMPETITIVE_DISPLACEMENT", "MATCH (n) RETURN n LIMIT 1")
    monkeypatch.delenv("WISDOM_DISABLE_EMBEDDED_CYPHER", raising=False)
    assert _cyphers_for_job_key("e100-wisdom-prompt-competitive-displacement") == [
        "MATCH (n) RETURN n LIMIT 1"
    ]


def test_build_deal_context_from_cypher_row():
    ctx = _build_deal_context_from_row(
        {
            "source_query": "switching_intent",
            "competition_notes": "Evaluating Eppo",
            "source_url": "https://example.com/call",
            "signal_count": 3,
        }
    )
    assert "switching_intent" in ctx
    assert "Evaluating Eppo" in ctx
    assert "signals=3" in ctx


def test_build_deal_context_includes_row_source_column():
    ctx = _build_deal_context_from_row(
        {
            "source_query": "competitive_displacement",
            "source": "Gong",
            "mention_count": 5,
        }
    )
    assert "query=competitive_displacement" in ctx
    assert "source=Gong" in ctx
    assert "mentions=5" in ctx


def test_normalize_tier2_row_passthrough():
    agent = Tier2EntrepretAgent(None, "k", None, graph=None)
    rec = agent._normalize(
        {
            "account_name": "Acme",
            "competitor": "Statsig",
            "source_query": "switching_intent",
            "arr": "1,250,000.50",
            "plan": "Enterprise",
            "renewal_date": "2026-01-01",
            "competition_notes": "Hot",
            "account_id": "acc-1",
        },
        "Acme",
    )
    assert rec.tier == 2
    assert rec.source == "enterpret"
    assert rec.urgency == "active"
    assert rec.arr == 1250000.50
    assert rec.plan == "Enterprise"
    assert rec.sfdc_account_id == "acc-1"
    assert rec.deal_context and "Hot" in rec.deal_context


def test_normalize_primary_competitor_and_zendesk_fields():
    agent = Tier2EntrepretAgent(None, "k", None, graph=None)
    rec = agent._normalize(
        {
            "primary_competitor": "Eppo",
            "source_query": "competitive_displacement",
            "mention_count": 2,
            "support_tier": "Premium",
            "csm_name": "Jordan",
            "customer_region": "EU",
        },
        "Contoso",
    )
    assert rec.competitor == "Eppo"
    assert rec.csm == "Jordan"
    assert rec.geo == "EU"
    assert rec.deal_context and "support_tier=Premium" in rec.deal_context

"""Tier 3 web collector (mocked HTTP)."""

import httpx
import pytest

from agents import tier3_web
from core.deduplicator import merge_accounts
from core.schema import AccountRecord
from core.scorer import score, score_tier3


@pytest.mark.asyncio
async def test_collect_skips_when_disabled(monkeypatch):
    monkeypatch.delenv("TIER3_WEB_ENABLED", raising=False)
    assert await tier3_web.collect() == []


@pytest.mark.asyncio
async def test_collect_keyword_produces_record(monkeypatch):
    monkeypatch.setenv("TIER3_WEB_ENABLED", "1")
    cfg = {
        "fetch": {
            "timeout_seconds": 5,
            "max_response_bytes": 1_000_000,
            "delay_between_requests_seconds": 0,
            "respect_robots_txt": False,
        },
        "keywords": ["feature flag"],
        "competitors": ["Optimizely"],
        "sources": [
            {"url": "https://example.com/jobs", "company_name": "ExCo TestCo"}
        ],
    }
    monkeypatch.setattr(tier3_web, "_load_config", lambda: cfg)

    html = b"<html><head><title>Careers</title></head><body>We use feature flag tools and Optimizely.</body></html>"
    req = httpx.Request("GET", "https://example.com/jobs")

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, url):
            return httpx.Response(
                200,
                content=html,
                request=req,
                headers={"content-type": "text/html; charset=utf-8"},
            )

    monkeypatch.setattr(httpx, "AsyncClient", FakeClient)

    out = await tier3_web.collect()
    assert len(out) == 1
    a = out[0]
    assert a.account_name == "ExCo TestCo"
    assert a.tier == 3
    assert a.competitor == "Optimizely"
    assert a.urgency == "active"
    assert "feature flag" in (a.deal_context or "").lower()
    assert a.tier3_extras.get("source_url")
    assert "feature flag" in (a.tier3_extras.get("matched_keywords") or "").lower()


@pytest.mark.asyncio
async def test_collect_no_hit_returns_empty(monkeypatch):
    monkeypatch.setenv("TIER3_WEB_ENABLED", "1")
    cfg = {
        "fetch": {"timeout_seconds": 5, "delay_between_requests_seconds": 0},
        "keywords": ["zzznomatchzzz"],
        "competitors": [],
        "sources": [{"url": "https://example.com/x", "company_name": "NoHit"}],
    }
    monkeypatch.setattr(tier3_web, "_load_config", lambda: cfg)

    html = b"<html><body>Hello world nothing here</body></html>"
    req = httpx.Request("GET", "https://example.com/x")

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, url):
            return httpx.Response(200, content=html, request=req)

    monkeypatch.setattr(httpx, "AsyncClient", FakeClient)
    assert await tier3_web.collect() == []


def test_score_tier3_positive():
    a = AccountRecord(
        account_name="X",
        tier=3,
        competitor="Optimizely",
        urgency="active",
        tier3_extras={"matched_keywords": "feature flag, split test"},
    )
    s = score_tier3(a)
    assert s > 0
    assert score(a) == s


def test_merge_preserves_tier3_extras():
    t1 = AccountRecord(account_name="Acme", tier=1, source="looker")
    t3 = AccountRecord(
        account_name="acme",
        tier=3,
        source="tier3_web",
        tier3_extras={"source_url": "https://x.com", "matched_keywords": "a/b test"},
    )
    m = merge_accounts([t1, t3])[0]
    assert m.tier3_extras.get("source_url") == "https://x.com"
    assert "a/b test" in (m.tier3_extras.get("matched_keywords") or "")

"""Wide E100: looker_extras, wisdom_extras, merge_accounts, output manifest."""

import pytest

from agents.tier1_looker import EXPORT_COLUMN_MAP, Tier1LookerAgent
from agents.tier2_enterpret import Tier2EntrepretAgent
from core.deduplicator import merge_accounts
from core.schema import AccountRecord
from outputs.e100_manifest import (
    account_to_manifest_row,
    load_e100_output_manifest,
    manifest_headers,
)


def _minimal_looker_row(**overrides):
    row = {h: "" for h in EXPORT_COLUMN_MAP.values()}
    row[EXPORT_COLUMN_MAP["account_name"]] = "Acme Corp"
    row[EXPORT_COLUMN_MAP["exp_events_entitled"]] = "100"
    row[EXPORT_COLUMN_MAP["exp_events_mtd"]] = "50"
    row[EXPORT_COLUMN_MAP["exp_utilisation_rate"]] = "0.5"
    row.update(overrides)
    return row


def test_tier1_unmapped_csv_headers_go_to_looker_extras():
    agent = Tier1LookerAgent(None, "t1", None, graph=None)
    row = _minimal_looker_row()
    row["Open Opportunities"] = "  12  "
    row["Custom Export Column"] = "x"
    rec = agent._normalize_export_row(row)
    assert rec.looker_extras["Open Opportunities"] == "12"
    assert rec.looker_extras["Custom Export Column"] == "x"


def test_tier2_unmapped_wisdom_keys_go_to_wisdom_extras():
    agent = Tier2EntrepretAgent(None, "t2", None, graph=None)
    item = {
        "urgency": "low",
        "arr": "100000",
        "plan": "Enterprise",
        "opp_stage": "Proposal",
        "blockers": "legal",
    }
    rec = agent._normalize(item, "Acme Corp")
    assert rec.wisdom_extras.get("opp_stage") == "Proposal"
    assert rec.wisdom_extras.get("blockers") == "legal"
    assert "arr" not in rec.wisdom_extras


def test_tier2_manifest_signal_columns_in_wisdom_extras():
    """mention_count / signal_count / source_query must appear in wisdom_extras for Sheets."""
    agent = Tier2EntrepretAgent(None, "t2", None, graph=None)
    item = {
        "urgency": "low",
        "mention_count": 3,
        "signal_count": 7,
        "source_query": "competitive_displacement",
    }
    rec = agent._normalize(item, "Acme Corp")
    assert rec.wisdom_extras.get("mention_count") == "3"
    assert rec.wisdom_extras.get("signal_count") == "7"
    assert rec.wisdom_extras.get("source_query") == "competitive_displacement"


def test_merge_accounts_t1_base_t2_overlay():
    t1 = AccountRecord(
        account_name="Acme Corp",
        tier=1,
        source="looker_export",
        arr=1_000_000.0,
        plan="Enterprise",
        looker_extras={"Open Opportunities": "2"},
    )
    t2 = AccountRecord(
        account_name="acme corp",
        tier=2,
        source="enterpret",
        competitor="Optimizely",
        deal_context="switching intent",
        wisdom_extras={"opp_stage": "Negotiation"},
    )
    out = merge_accounts([t1, t2])
    assert len(out) == 1
    m = out[0]
    assert m.arr == 1_000_000.0
    assert m.plan == "Enterprise"
    assert m.competitor == "Optimizely"
    assert "switching intent" in (m.deal_context or "")
    assert m.looker_extras["Open Opportunities"] == "2"
    assert m.wisdom_extras["opp_stage"] == "Negotiation"
    assert m.tier == 1
    assert "looker_export" in (m.source or "")
    assert "enterpret" in (m.source or "")


def test_manifest_wisdom_columns_resolve_from_wisdom_extras():
    columns = load_e100_output_manifest()
    headers = manifest_headers(columns)
    acct = AccountRecord(
        account_name="Gamma",
        tier=2,
        wisdom_extras={
            "mention_count": "5",
            "signal_count": "2",
            "source_query": "switching_intent",
        },
    )
    row = account_to_manifest_row(acct, columns)
    assert row[headers.index("Wisdom: mention_count")] == "5"
    assert row[headers.index("Wisdom: signal_count")] == "2"
    assert row[headers.index("Wisdom: source_query")] == "switching_intent"


def test_manifest_row_matches_headers_and_open_opportunities_column():
    columns = load_e100_output_manifest()
    headers = manifest_headers(columns)
    acct = AccountRecord(
        account_name="Beta",
        tier=1,
        priority_rank=1,
        expansion_score=9.0,
        exp_utilisation_rate=0.25,
        notes="hello",
        looker_extras={"Open Opportunities": "9"},
    )
    row = account_to_manifest_row(acct, columns)
    assert len(row) == len(headers)
    idx = headers.index("Open Opportunities")
    assert row[idx] == "9"
    idx_notes = headers.index("Notes")
    assert row[idx_notes] == "hello"
    idx_util = headers.index("Utilisation Rate")
    assert row[idx_util] == "25.0%"


def test_manifest_rejects_unknown_spec():
    columns = [{"header": "X", "bogus": True}]
    acct = AccountRecord(account_name="A")
    with pytest.raises(ValueError, match="field, looker_extra, or wisdom_extra"):
        account_to_manifest_row(acct, columns)

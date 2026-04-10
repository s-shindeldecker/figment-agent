"""Per-tier Sheet export clones and tier-local ranking."""

from core.merger import (
    clone_accounts_for_sheet_export,
    score_and_rank_for_export,
)
from core.schema import AccountRecord


def test_clone_resets_score_and_rank_without_mutating_original():
    orig = AccountRecord(
        account_name="Acme",
        tier=1,
        expansion_score=99.0,
        priority_rank=5,
    )
    clones = clone_accounts_for_sheet_export([orig])
    assert len(clones) == 1
    c = clones[0]
    assert c is not orig
    assert orig.expansion_score == 99.0
    assert orig.priority_rank == 5
    assert c.expansion_score is None
    assert c.priority_rank is None
    assert c.account_name == "Acme"
    assert c.tier == 1


def test_score_and_rank_for_export_orders_and_ranks_within_tier():
    clones = clone_accounts_for_sheet_export(
        [
            AccountRecord(account_name="Low", tier=2, urgency="watch"),
            AccountRecord(account_name="High", tier=2, urgency="immediate"),
        ]
    )
    out = score_and_rank_for_export(clones)
    assert [a.account_name for a in out] == ["High", "Low"]
    assert [a.priority_rank for a in out] == [1, 2]
    assert out[0].expansion_score >= out[1].expansion_score

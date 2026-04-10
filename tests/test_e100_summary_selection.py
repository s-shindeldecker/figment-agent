"""E100 merged summary: tier quotas (50/25/25 default) plus score-based backfill."""

from core.merger import (
    merge_and_score,
    resolve_e100_summary_list,
    select_e100_summary_list,
)


def _rec(name, tier, score):
    from core.schema import AccountRecord

    return AccountRecord(
        account_name=name,
        tier=tier,
        expansion_score=score,
        source="test",
    )


def test_select_respects_tier_caps_then_backfills_by_global_score_order():
    # Global rank order (high to low): A1, B1, C2, D2, E3, F3, G1 (extra tier1)
    ranked = [
        _rec("A", 1, 100.0),
        _rec("B", 1, 99.0),
        _rec("C", 2, 98.0),
        _rec("D", 2, 97.0),
        _rec("E", 3, 96.0),
        _rec("F", 3, 95.0),
        _rec("G", 1, 94.0),
    ]
    for i, a in enumerate(ranked, start=1):
        a.priority_rank = i

    out = select_e100_summary_list(ranked, tier1_max=2, tier2_max=2, tier3_max=2)
    assert len(out) == 6
    names = {a.account_name for a in out}
    assert names == {"A", "B", "C", "D", "E", "F"}
    assert [a.account_name for a in out] == ["A", "B", "C", "D", "E", "F"]
    assert [a.priority_rank for a in out] == [1, 2, 3, 4, 5, 6]


def test_backfill_when_tier_short_on_accounts():
    ranked = [
        _rec("B", 2, 90.0),
        _rec("C", 2, 89.0),
        _rec("D", 3, 88.0),
        _rec("E", 3, 87.0),
        _rec("F", 3, 86.0),
        _rec("G", 3, 85.0),
    ]
    out = select_e100_summary_list(ranked, tier1_max=2, tier2_max=2, tier3_max=2)
    assert len(out) == 6
    assert {a.account_name for a in out} == {"B", "C", "D", "E", "F", "G"}
    assert [a.account_name for a in out] == ["B", "C", "D", "E", "F", "G"]


def test_resolve_full_merge_env(monkeypatch):
    monkeypatch.setenv("E100_SUMMARY_USE_FULL_MERGE", "1")
    ranked = [_rec("A", 1, 1.0), _rec("B", 2, 2.0)]
    for i, a in enumerate(ranked, start=1):
        a.priority_rank = i
    out = resolve_e100_summary_list(ranked)
    assert out is ranked
    assert len(out) == 2


def test_merge_and_score_then_select_total_cap_100_style():
    rows = []
    for i in range(60):
        rows.append(_rec(f"t1_{i}", 1, float(200 - i)))
    for i in range(40):
        rows.append(_rec(f"t2_{i}", 2, float(100 - i)))
    for i in range(40):
        rows.append(_rec(f"t3_{i}", 3, float(50 - i)))
    combined = rows
    full = merge_and_score(combined)
    summary = select_e100_summary_list(full, 50, 25, 25)
    assert len(summary) == 100
    t1_n = sum(1 for a in summary if a.tier == 1)
    t2_n = sum(1 for a in summary if a.tier == 2)
    t3_n = sum(1 for a in summary if a.tier == 3)
    assert t1_n == 50
    assert t2_n == 25
    assert t3_n == 25

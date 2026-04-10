"""Tests for run-to-run Sheets snapshot and diff helpers."""

from pathlib import Path

from core.schema import AccountRecord
from outputs.e100_manifest import load_e100_output_manifest
from outputs.sheets_run_diff import (
    account_name_column_index,
    build_changelog_sheet_values,
    build_snapshot_payload,
    delta_markers_for_tab,
    diff_snapshots,
    load_snapshot,
    normalize_account_key,
    row_dict_from_accounts,
    save_snapshot,
)


def test_normalize_account_key():
    assert normalize_account_key("  Acme Corp  ") == "acme corp"
    assert normalize_account_key(None) == ""


def test_account_name_column_index_matches_manifest():
    cols = load_e100_output_manifest()
    i = account_name_column_index(cols)
    assert cols[i].get("field") == "account_name"


def test_row_dict_from_accounts():
    cols = load_e100_output_manifest()
    acct = AccountRecord(account_name="Beta Inc", tier=1, expansion_score=5.0)
    m = row_dict_from_accounts([acct], cols)
    assert "beta inc" in m
    assert len(m["beta inc"]) == len(cols)


def test_diff_snapshots_added_removed_field_change():
    headers = ["Account Name", "Score"]
    titles = {"tier1": "E100 Tier 1"}
    prev = {"tier1": {"a": ["A", "1"], "b": ["B", "2"]}}
    curr = {"tier1": {"a": ["A", "9"], "c": ["C", "3"]}}
    ch = diff_snapshots(prev, curr, headers, titles)
    kinds = {(c.account_display, c.kind, c.field) for c in ch}
    assert ("C", "added", "") in {(c.account_display, c.kind, c.field) for c in ch}
    assert ("B", "removed", "") in {(c.account_display, c.kind, c.field) for c in ch}
    field_changes = [c for c in ch if c.kind == "field_change"]
    assert any(c.field == "Score" and c.before == "1" and c.after == "9" for c in field_changes)


def test_diff_snapshots_pads_short_rows():
    headers = ["H0", "H1", "H2"]
    titles = {"tier1": "T1"}
    prev = {"tier1": {"x": ["X", ""]}}
    curr = {"tier1": {"x": ["X", "", "new"]}}
    ch = diff_snapshots(prev, curr, headers, titles)
    assert any(c.field == "H2" for c in ch)


def test_diff_snapshots_tier3_uses_per_tab_headers():
    headers = ["Account Name", "ARR"]
    headers_t3 = ["Account Name", "Est. company revenue (ZoomInfo)"]
    titles = {"tier3": "E100 Tier 3"}
    prev = {"tier3": {"a": ["A", "100"]}}
    curr = {"tier3": {"a": ["A", "200"]}}
    ch = diff_snapshots(
        prev,
        curr,
        headers,
        titles,
        headers_by_logical_tab={"tier3": headers_t3},
    )
    fc = [c for c in ch if c.kind == "field_change"]
    assert len(fc) == 1
    assert fc[0].field == "Est. company revenue (ZoomInfo)"


def test_delta_markers_for_tab():
    ncol = 2
    prev = {"a": ["1", "2"], "c": ["0", "1"]}
    curr = {"a": ["1", "2"], "b": ["3", "4"], "c": ["0", "0"]}
    m = delta_markers_for_tab(prev, curr, ncol)
    assert m["a"] == ""
    assert m["b"] == "new"
    assert m["c"] == "changed"


def test_build_changelog_first_run():
    rows = build_changelog_sheet_values(None, "2026-01-01T00:00:00+00:00", [], first_run=True)
    flat = "\n".join(str(c) for r in rows for c in r)
    assert "No prior run snapshot" in flat


def test_build_changelog_with_changes():
    from outputs.sheets_run_diff import SheetChange

    changes = [
        SheetChange(
            logical_tab="tier1",
            tab_title="E100 Tier 1",
            account_key="x",
            account_display="Acme",
            kind="field_change",
            field="Score",
            before="1",
            after="2",
        )
    ]
    rows = build_changelog_sheet_values(
        "2025-12-01T00:00:00+00:00",
        "2026-01-01T00:00:00+00:00",
        changes,
        first_run=False,
    )
    header_row = [r for r in rows if r and r[0] == "Tab"][0]
    assert header_row == ["Tab", "Account", "Change", "Field", "Before", "After"]
    assert any("Acme" in str(r) for r in rows)


def test_snapshot_roundtrip(tmp_path: Path):
    p = tmp_path / "snap.json"
    payload = build_snapshot_payload(
        "sheet123",
        {"tier1": {"acme": ["1", "Acme"]}},
    )
    save_snapshot(payload, path=p)
    loaded = load_snapshot(path=p)
    assert loaded is not None
    assert loaded["sheet_id"] == "sheet123"
    assert loaded["tabs"]["tier1"]["acme"] == ["1", "Acme"]


def test_load_snapshot_missing(tmp_path: Path):
    assert load_snapshot(path=tmp_path / "nope.json") is None

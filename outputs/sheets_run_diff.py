"""
Run-to-run diff for E100 Google Sheets export: snapshot on disk + changelog content.

Snapshots use logical tab keys (tier1, tier2, tier3, merged) mapping normalized
account name -> manifest row values as strings (same order as columns).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from core.schema import AccountRecord
from outputs.e100_manifest import account_to_manifest_row, manifest_headers

_REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SNAPSHOT_PATH = _REPO_ROOT / "data" / "e100_last_sheet_snapshot.json"


def snapshot_path() -> Path:
    raw = (os.getenv("E100_SHEET_SNAPSHOT_PATH") or "").strip()
    if raw:
        p = Path(raw)
        return p if p.is_absolute() else _REPO_ROOT / p
    return DEFAULT_SNAPSHOT_PATH


def account_name_column_index(columns: List[Dict[str, Any]]) -> int:
    for i, c in enumerate(columns):
        if c.get("field") == "account_name":
            return i
    raise ValueError("Manifest must include a column with field: account_name")


def normalize_account_key(name: Any) -> str:
    if name is None:
        return ""
    s = str(name).strip().casefold()
    return s


def _stringify_cell(v: Any) -> str:
    if v is None:
        return ""
    return str(v)


def row_dict_from_accounts(
    accounts: List[AccountRecord],
    columns: List[Dict[str, Any]],
) -> Dict[str, List[str]]:
    """Map normalized account name -> manifest row as string cells."""
    idx = account_name_column_index(columns)
    out: Dict[str, List[str]] = {}
    for acct in accounts:
        row = account_to_manifest_row(acct, columns)
        key = normalize_account_key(row[idx] if idx < len(row) else "")
        if not key:
            continue
        out[key] = [_stringify_cell(x) for x in row]
    return out


def build_snapshot_payload(
    sheet_id: str,
    tabs: Mapping[str, Dict[str, List[str]]],
) -> Dict[str, Any]:
    return {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "sheet_id": sheet_id,
        "tabs": dict(tabs),
    }


def load_snapshot(path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    p = path or snapshot_path()
    if not p.is_file():
        return None
    try:
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    tabs = data.get("tabs")
    if not isinstance(tabs, dict):
        return None
    return data


def save_snapshot(payload: Dict[str, Any], path: Optional[Path] = None) -> None:
    p = path or snapshot_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
        f.write("\n")


@dataclass(frozen=True)
class SheetChange:
    logical_tab: str
    tab_title: str
    account_key: str
    account_display: str
    kind: str  # "added" | "removed" | "field_change"
    field: str
    before: str
    after: str


def _display_name_for_key(
    row: Optional[List[str]], headers: List[str], key: str
) -> str:
    if not row:
        return key
    try:
        i = headers.index("Account Name")
        if i < len(row) and row[i].strip():
            return row[i].strip()
    except ValueError:
        pass
    return key


def diff_snapshots(
    prev_tabs: Mapping[str, Mapping[str, List[str]]],
    curr_tabs: Mapping[str, Mapping[str, List[str]]],
    headers: List[str],
    tab_titles: Mapping[str, str],
) -> List[SheetChange]:
    changes: List[SheetChange] = []
    all_logical = set(prev_tabs.keys()) | set(curr_tabs.keys())
    for logical in sorted(all_logical):
        title = tab_titles.get(logical, logical)
        old_m = prev_tabs.get(logical) or {}
        new_m = curr_tabs.get(logical) or {}
        old_keys = set(old_m.keys())
        new_keys = set(new_m.keys())
        for k in sorted(new_keys - old_keys):
            row = new_m[k]
            disp = _display_name_for_key(row, headers, k)
            changes.append(
                SheetChange(
                    logical_tab=logical,
                    tab_title=title,
                    account_key=k,
                    account_display=disp,
                    kind="added",
                    field="",
                    before="",
                    after="",
                )
            )
        for k in sorted(old_keys - new_keys):
            row = old_m[k]
            disp = _display_name_for_key(row, headers, k)
            changes.append(
                SheetChange(
                    logical_tab=logical,
                    tab_title=title,
                    account_key=k,
                    account_display=disp,
                    kind="removed",
                    field="",
                    before="",
                    after="",
                )
            )
        for k in sorted(old_keys & new_keys):
            o = old_m[k]
            n = new_m[k]
            nh = len(headers)
            o = (o + [""] * nh)[:nh]
            n = (n + [""] * nh)[:nh]
            if o == n:
                continue
            disp = _display_name_for_key(n, headers, k)
            for i in range(nh):
                if o[i] != n[i]:
                    changes.append(
                        SheetChange(
                            logical_tab=logical,
                            tab_title=title,
                            account_key=k,
                            account_display=disp,
                            kind="field_change",
                            field=headers[i],
                            before=o[i],
                            after=n[i],
                        )
                    )
    return changes


def delta_markers_for_tab(
    prev_tab: Mapping[str, List[str]],
    curr_tab: Mapping[str, List[str]],
    ncol: int,
) -> Dict[str, str]:
    """For each key in curr_tab: 'new', 'changed', or ''."""
    out: Dict[str, str] = {}
    for k, new_row in curr_tab.items():
        n = (new_row + [""] * ncol)[:ncol]
        if k not in prev_tab:
            out[k] = "new"
        else:
            o = (prev_tab[k] + [""] * ncol)[:ncol]
            out[k] = "changed" if o != n else ""
    return out


def build_changelog_sheet_values(
    prev_saved_at: Optional[str],
    now_iso: str,
    changes: List[SheetChange],
    first_run: bool,
) -> List[List[Any]]:
    """2D values for the E100 Changelog worksheet."""
    rows: List[List[Any]] = []
    rows.append(
        [
            f"E100 run diff — current run {now_iso}"
            + (
                f" vs prior snapshot {prev_saved_at}"
                if prev_saved_at
                else " (no prior snapshot)"
            ),
        ]
    )
    rows.append([])

    if first_run:
        rows.append(
            [
                "No prior run snapshot on disk; baseline was saved after this write. "
                "The next run will show adds, removes, and field changes."
            ]
        )
        rows.append([])
    elif not changes:
        rows.append(["No changes vs prior snapshot (same accounts and cell values per tab)."])
        rows.append([])

    # Prose block
    prose_lines = _prose_lines_from_changes(changes)
    if prose_lines:
        rows.append(["— Summary (prose) —"])
        for line in prose_lines:
            rows.append([line])
        rows.append([])

    # Table
    rows.append(["Tab", "Account", "Change", "Field", "Before", "After"])
    for c in changes:
        kind_label = c.kind
        if c.kind == "field_change":
            kind_label = "changed"
        rows.append(
            [
                c.tab_title,
                c.account_display,
                kind_label,
                c.field,
                c.before,
                c.after,
            ]
        )
    return rows


def _prose_lines_from_changes(changes: List[SheetChange]) -> List[str]:
    """One bullet per account with grouped field deltas."""
    from collections import defaultdict

    by_tab_acct: Dict[Tuple[str, str], List[SheetChange]] = defaultdict(list)
    for c in changes:
        by_tab_acct[(c.tab_title, c.account_key)].append(c)

    lines: List[str] = []
    for (tab_title, _key), group in sorted(by_tab_acct.items()):
        disp = group[0].account_display
        parts: List[str] = []
        for c in group:
            if c.kind == "added":
                parts.append("added to sheet")
            elif c.kind == "removed":
                parts.append("removed from sheet")
            elif c.kind == "field_change" and c.field:
                b, a = c.before, c.after
                parts.append(f"{c.field}: {b!r} → {a!r}")
        if parts:
            uniq = []
            seen = set()
            for p in parts:
                if p not in seen:
                    seen.add(p)
                    uniq.append(p)
            lines.append(f"• {tab_title} — {disp}: " + "; ".join(uniq) + ".")
    return lines

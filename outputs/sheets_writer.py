import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import gspread
import yaml
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials

from core.schema import AccountRecord
from outputs.e100_manifest import (
    account_to_manifest_row,
    load_e100_output_manifest,
    manifest_headers,
)
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

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_CRED = _REPO_ROOT / "config" / "google_service_account.json"
_SETTINGS_PATH = _REPO_ROOT / "config" / "settings.yaml"


def _google_service_account_path() -> Path:
    """
    Credentials file: GOOGLE_SERVICE_ACCOUNT_FILE or GOOGLE_APPLICATION_CREDENTIALS
    (relative paths are resolved from repo root), else config/google_service_account.json.
    """
    raw = (os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE") or "").strip()
    if not raw:
        raw = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
    if raw:
        p = Path(raw)
        if not p.is_absolute():
            p = _REPO_ROOT / p
        return p
    return _DEFAULT_CRED


def _load_output_settings() -> Dict[str, Any]:
    try:
        with open(_SETTINGS_PATH, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return data.get("output") or {}
    except Exception:
        return {}


def worksheet_titles() -> Dict[str, str]:
    """Tab titles: env overrides, then settings.yaml, then defaults."""
    out = _load_output_settings()
    return {
        "tier1": (os.getenv("GOOGLE_SHEET_TAB_TIER1") or "").strip()
        or out.get("sheets_tab_tier1")
        or "E100 Tier 1",
        "tier2": (os.getenv("GOOGLE_SHEET_TAB_TIER2") or "").strip()
        or out.get("sheets_tab_tier2")
        or "E100 Tier 2",
        "tier3": (os.getenv("GOOGLE_SHEET_TAB_TIER3") or "").strip()
        or out.get("sheets_tab_tier3")
        or "E100 Tier 3",
        "merged": (os.getenv("GOOGLE_SHEET_TAB") or "").strip()
        or out.get("sheets_tab")
        or "E100 Master",
        "changelog": (os.getenv("GOOGLE_SHEET_TAB_CHANGELOG") or "").strip()
        or out.get("sheets_tab_changelog")
        or "E100 Changelog",
    }


def _env_truthy(name: str) -> bool:
    return (os.getenv(name) or "").strip().lower() in ("1", "true", "yes", "on")


def write_merged_master_enabled() -> bool:
    """When true, ``write_to_sheets_by_tier`` also writes the merged ranked list."""
    return _env_truthy("E100_WRITE_MERGED_MASTER")


def sheet_mark_changes_enabled() -> bool:
    """When true, append a Δ column (new/changed) on each exported tier tab."""
    return _env_truthy("E100_SHEET_MARK_CHANGES")


def _authorize_spreadsheet(sheet_id: str):
    cred_path = _google_service_account_path()
    if not cred_path.is_file():
        raise FileNotFoundError(
            "Google Sheets service account key missing. Either:\n"
            f"  • Save the JSON key to: {_DEFAULT_CRED}\n"
            "  • Or set GOOGLE_SERVICE_ACCOUNT_FILE (or GOOGLE_APPLICATION_CREDENTIALS) "
            "to its path, e.g. config/figment-e100-writer-….json (relative to repo root)\n"
            f"Currently resolved path: {cred_path}\n"
            "Share your spreadsheet with the key's client_email (Editor)."
        )
    creds = Credentials.from_service_account_file(
        str(cred_path),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(sheet_id)


def _get_or_create_worksheet(
    spreadsheet,
    title: str,
    min_rows: int,
    min_cols: int,
):
    try:
        sheet = spreadsheet.worksheet(title)
    except WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(
            title=title,
            rows=max(2000, min_rows),
            cols=max(30, min_cols),
        )
    return sheet


def _write_manifest_to_worksheet(
    sheet,
    accounts: List[AccountRecord],
    columns: List[Dict[str, Any]],
    delta_by_key: Optional[Dict[str, str]] = None,
) -> None:
    headers = list(manifest_headers(columns))
    name_i = account_name_column_index(columns) if delta_by_key else None
    data_rows: List[List[Any]] = []
    for acct in accounts:
        row = list(account_to_manifest_row(acct, columns))
        if delta_by_key is not None and name_i is not None:
            key = normalize_account_key(row[name_i] if name_i < len(row) else "")
            row.append(delta_by_key.get(key, ""))
        data_rows.append(row)
    if delta_by_key is not None:
        headers.append("Δ")
    values = [headers] + data_rows
    sheet.clear()
    sheet.update(
        range_name="A1",
        values=values,
        value_input_option="USER_ENTERED",
    )


def _pad_rectangular(values: List[List[Any]]) -> List[List[Any]]:
    if not values:
        return values
    w = max(len(r) for r in values)
    return [list(r) + [""] * (w - len(r)) for r in values]


def write_to_sheets_by_tier(
    tier1_accounts: List[AccountRecord],
    tier2_accounts: List[AccountRecord],
    tier3_accounts: List[AccountRecord],
    merged_accounts: Optional[List[AccountRecord]] = None,
    sheet_id: Optional[str] = None,
) -> None:
    """
    Write three worksheets (Tier 1 / 2 / 3). If ``E100_WRITE_MERGED_MASTER`` is set,
    also write ``merged_accounts`` to the merged tab title (GOOGLE_SHEET_TAB / settings).
    Writes ``E100 Changelog`` (tab name configurable) and persists
    ``data/e100_last_sheet_snapshot.json`` for the next run's diff.
    """
    if sheet_id is None:
        sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise ValueError("GOOGLE_SHEET_ID not set")

    columns = load_e100_output_manifest()
    ncol = len(columns)
    titles = worksheet_titles()
    spreadsheet = _authorize_spreadsheet(sheet_id)

    headers = manifest_headers(columns)
    curr_tabs: Dict[str, Dict[str, List[str]]] = {
        "tier1": row_dict_from_accounts(tier1_accounts, columns),
        "tier2": row_dict_from_accounts(tier2_accounts, columns),
        "tier3": row_dict_from_accounts(tier3_accounts, columns),
    }
    if write_merged_master_enabled() and merged_accounts is not None:
        curr_tabs["merged"] = row_dict_from_accounts(merged_accounts, columns)

    prev = load_snapshot()
    first_run = prev is None
    prev_tabs: Dict[str, Any] = (
        prev.get("tabs") if isinstance(prev, dict) and isinstance(prev.get("tabs"), dict) else {}
    )
    prev_saved_at = prev.get("saved_at") if isinstance(prev, dict) else None
    now_iso = datetime.now(timezone.utc).isoformat()

    changes_for_changelog = (
        []
        if first_run
        else diff_snapshots(prev_tabs, curr_tabs, headers, titles)
    )

    mark = sheet_mark_changes_enabled()
    extra_col = 1 if mark else 0

    for label, rows in (
        ("tier1", tier1_accounts),
        ("tier2", tier2_accounts),
        ("tier3", tier3_accounts),
    ):
        tab = titles[label]
        sheet = _get_or_create_worksheet(
            spreadsheet,
            tab,
            min_rows=len(rows) + 10,
            min_cols=ncol + 2 + extra_col,
        )
        delta = None
        if mark:
            delta = delta_markers_for_tab(
                prev_tabs.get(label, {}),
                curr_tabs[label],
                ncol,
            )
        _write_manifest_to_worksheet(sheet, rows, columns, delta_by_key=delta)

    if write_merged_master_enabled() and merged_accounts is not None:
        tab = titles["merged"]
        sheet = _get_or_create_worksheet(
            spreadsheet,
            tab,
            min_rows=len(merged_accounts) + 10,
            min_cols=ncol + 2 + extra_col,
        )
        delta = None
        if mark:
            delta = delta_markers_for_tab(
                prev_tabs.get("merged", {}),
                curr_tabs["merged"],
                ncol,
            )
        _write_manifest_to_worksheet(sheet, merged_accounts, columns, delta_by_key=delta)

    changelog_title = titles["changelog"]
    changelog_values = build_changelog_sheet_values(
        prev_saved_at if isinstance(prev_saved_at, str) else None,
        now_iso,
        changes_for_changelog,
        first_run,
    )
    changelog_sheet = _get_or_create_worksheet(
        spreadsheet,
        changelog_title,
        min_rows=len(changelog_values) + 5,
        min_cols=7,
    )
    changelog_sheet.clear()
    changelog_sheet.update(
        range_name="A1",
        values=_pad_rectangular(changelog_values),
        value_input_option="USER_ENTERED",
    )

    save_snapshot(build_snapshot_payload(sheet_id, curr_tabs))


def write_to_sheets(accounts: list[AccountRecord], sheet_id: Optional[str] = None):
    """Write a single worksheet (merged tab title). For per-tier export use ``write_to_sheets_by_tier``."""
    if sheet_id is None:
        sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise ValueError("GOOGLE_SHEET_ID not set")

    columns = load_e100_output_manifest()
    ncol = len(columns)
    tab_title = worksheet_titles()["merged"]
    spreadsheet = _authorize_spreadsheet(sheet_id)
    sheet = _get_or_create_worksheet(
        spreadsheet, tab_title, len(accounts) + 10, ncol + 2
    )
    ranked = sorted(accounts, key=lambda x: x.expansion_score or 0, reverse=True)
    _write_manifest_to_worksheet(sheet, ranked, columns)

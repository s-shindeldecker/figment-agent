import os
from pathlib import Path
from typing import Optional

import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials

from core.schema import AccountRecord
from outputs.e100_manifest import (
    account_to_manifest_row,
    load_e100_output_manifest,
    manifest_headers,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_CRED = _REPO_ROOT / "config" / "google_service_account.json"


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


def write_to_sheets(accounts: list[AccountRecord], sheet_id: Optional[str] = None):
    if sheet_id is None:
        sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise ValueError("GOOGLE_SHEET_ID not set")

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

    columns = load_e100_output_manifest()
    tab_title = (os.getenv("GOOGLE_SHEET_TAB") or "E100 Master").strip()
    ncol = len(columns)

    creds = Credentials.from_service_account_file(
        str(cred_path),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(sheet_id)
    try:
        sheet = spreadsheet.worksheet(tab_title)
    except WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(
            title=tab_title,
            rows=max(2000, len(accounts) + 10),
            cols=max(ncol + 2, 30),
        )

    headers = manifest_headers(columns)

    ranked = sorted(accounts, key=lambda x: x.expansion_score or 0, reverse=True)
    data_rows = [account_to_manifest_row(acct, columns) for acct in ranked]
    values = [headers] + data_rows

    sheet.clear()
    sheet.update(
        range_name="A1",
        values=values,
        value_input_option="USER_ENTERED",
    )

import os
import gspread
from google.oauth2.service_account import Credentials
from core.schema import AccountRecord


SHEET_COLUMNS = [
    "Priority Rank", "Account Name", "Tier", "Urgency",
    "ARR", "Plan", "Geo", "Industry", "Expansion Score",
    "AE", "CSM", "Competitor", "Competitor Spend",
    "Exp Events MTD", "Entitlement", "Utilisation Rate",
    "Days Since Last Experiment", "Active Experiments",
    "Renewal Date", "Open Opportunities", "Deal Context",
    "Source", "Override", "Notes", "Last Updated",
]


def write_to_sheets(accounts: list[AccountRecord], sheet_id: str = None):
    if sheet_id is None:
        sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise ValueError("GOOGLE_SHEET_ID not set")

    creds = Credentials.from_service_account_file(
        "config/google_service_account.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(sheet_id).worksheet("E100 Master")

    # Clear existing data below header
    sheet.resize(rows=1)

    rows = []
    for i, acct in enumerate(
        sorted(accounts, key=lambda x: x.expansion_score or 0, reverse=True), start=1
    ):
        rows.append([
            i,
            acct.account_name,
            acct.tier,
            acct.urgency or "active",
            acct.arr,
            acct.plan,
            acct.geo,
            acct.industry,
            acct.expansion_score,
            acct.ae,
            acct.csm,
            acct.competitor,
            acct.competitor_spend,
            acct.exp_events_mtd,
            acct.exp_events_entitled,
            f"{(acct.exp_utilisation_rate or 0):.1%}",
            acct.days_since_last_iteration,
            acct.active_experiments,
            acct.renewal_date,
            acct.notes,
            acct.deal_context,
            acct.source,
            acct.override_action,
            acct.notes,
            acct.last_updated,
        ])

    sheet.append_rows(rows)

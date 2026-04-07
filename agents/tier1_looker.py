import csv
import os
import requests
from agents.base import AgentService
from core.schema import AccountRecord


# ---------------------------------------------------------------------------
# Column map: Looker CSV export header → normalized field
#
# Mapped against the actual export headers. If you regenerate the Look
# and column names change, update the values here to match.
#
# Note: the export has two columns both named "Name" (AE and CSM).
# The loader renames them to "Name_ae" and "Name_csm" automatically.
# ---------------------------------------------------------------------------
EXPORT_COLUMN_MAP = {
    "account_name":              "SFDC Account Name",
    "sfdc_account_id":           "SFDC Account ID",
    "ld_account_id":             "LD Account ID",
    "arr":                       "ARR",
    "plan":                      "Plan",
    "rating":                    "Rating",
    "geo":                       "Geo",
    "industry":                  "Industry",
    "renewal_date":              "Renewal Date",
    "ae":                        "Name_ae",       # First "Name" column (AE)
    "csm":                       "Name_csm",      # Second "Name" column (CSM)
    "is_using_exp_90d":          "Is Using Experimentation 90d (Yes / No)",
    "exp_events_mtd":            "Experimentation Events Received Mtd",
    "exp_events_entitled":       "Experimentation Events Entitled To",
    "exp_utilisation_rate":      "Exp Utilization Rate",
    "days_since_last_iteration": "By Account (Avg)",
    "active_experiments":        "Accounts With Active Experiments",
}


class Tier1LookerAgent(AgentService):
    """
    Collects Tier 1 accounts from Looker.

    Supports two modes, controlled by the LOOKER_EXPORT_PATH env var:

      File mode (current):
        Set LOOKER_EXPORT_PATH to the path of a CSV exported from the
        Experimentation Usage Look. Drop the file in data/ and point
        the env var at it.

      API mode (future):
        Unset LOOKER_EXPORT_PATH and set LOOKER_CLIENT_ID,
        LOOKER_CLIENT_SECRET, and LOOKER_BASE_URL instead.
    """

    LOOKER_BASE_URL = os.getenv("LOOKER_BASE_URL", "https://launchdarkly.cloud.looker.com")

    async def run(self) -> list[AccountRecord]:
        # Instructions available via self.get_instructions() for edge case handling
        export_path = os.getenv("LOOKER_EXPORT_PATH")
        if export_path:
            raw = self._load_from_file(export_path)
            return [self._normalize_export_row(row) for row in raw]
        else:
            raw = self._query_looker()
            return [self._normalize_api_row(row) for row in raw]

    # ------------------------------------------------------------------
    # File mode
    # ------------------------------------------------------------------

    def _load_from_file(self, path: str) -> list[dict]:
        """
        Read a CSV exported directly from the Looker Look.

        Handles two quirks of this specific export:
          1. Two columns both named "Name" (AE and CSM) — renamed to
             Name_ae and Name_csm based on their position in the header row.
          2. ARR formatted as "$1,234,567.89" — stripped in _normalize.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"Looker export not found: {path}")

        with open(path, newline="", encoding="utf-8-sig") as f:
            raw_headers = next(csv.reader(f))

        # Rename duplicate "Name" columns by position
        seen: dict[str, int] = {}
        deduped_headers = []
        for col in raw_headers:
            if col == "Name":
                count = seen.get(col, 0)
                deduped_headers.append("Name_ae" if count == 0 else "Name_csm")
                seen[col] = count + 1
            else:
                deduped_headers.append(col)
                seen[col] = seen.get(col, 0) + 1

        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, fieldnames=deduped_headers)
            next(reader)  # skip original header row
            rows = list(reader)

        if not rows:
            raise ValueError(f"Looker export is empty: {path}")

        print(f"[Tier1] Loaded {len(rows)} rows from {path}")
        return rows

    def _normalize_export_row(self, row: dict) -> AccountRecord:
        """
        Normalize a row from a CSV export using EXPORT_COLUMN_MAP.
        If a column header in the map doesn't match your export exactly,
        update EXPORT_COLUMN_MAP at the top of this file.
        """
        def get(field: str):
            col = EXPORT_COLUMN_MAP.get(field)
            if col is None:
                return None
            val = row.get(col, "").strip()
            return val if val not in ("", "null", "NULL", "N/A") else None

        def to_float(field: str):
            val = get(field)
            if val is None:
                return None
            try:
                # Strip $ and commas from values like "$1,464,857.21"
                return float(str(val).replace("$", "").replace(",", "").strip())
            except (ValueError, TypeError):
                return None

        entitled = to_float("exp_events_entitled") or 0
        received = to_float("exp_events_mtd") or 0
        # Use pre-calculated utilisation rate from export if available,
        # otherwise calculate it from events received / entitled
        utilisation = to_float("exp_utilisation_rate")
        if utilisation is None:
            utilisation = (received / entitled) if entitled > 0 else None

        return AccountRecord(
            account_name=get("account_name"),
            sfdc_account_id=get("sfdc_account_id"),
            ld_account_id=get("ld_account_id"),
            arr=to_float("arr"),
            plan=get("plan"),
            rating=get("rating"),
            geo=get("geo"),
            industry=get("industry"),
            renewal_date=get("renewal_date"),
            ae=get("ae"),
            csm=get("csm"),
            tier=1,
            source="looker_export",
            exp_events_mtd=received,
            exp_events_entitled=entitled,
            exp_utilisation_rate=utilisation,
            is_using_exp_90d=False,     # Export is already filtered to No
            days_since_last_iteration=to_float("days_since_last_iteration"),
            active_experiments=int(to_float("active_experiments") or 0) or None,
        )

    # ------------------------------------------------------------------
    # API mode (future — requires LOOKER_CLIENT_ID + LOOKER_CLIENT_SECRET)
    # ------------------------------------------------------------------

    def _query_looker(self) -> list[dict]:
        token = self._get_looker_token()
        headers = {"Authorization": f"Bearer {token}"}

        payload = {
            "model": "your_looker_model",       # Update to match your instance
            "view": "ld_account_experimentation_usage_daily",
            "fields": [
                "salesforce_accounts.sfdc_account_name",
                "salesforce_accounts.sfdc_account_id",
                "salesforce_accounts.ld_account_id",
                "salesforce_accounts.plan",
                "salesforce_accounts.arr",
                "salesforce_accounts.rating",
                "salesforce_accounts.geo",
                "salesforce_accounts.industry",
                "salesforce_accounts.renewal_date",
                "salesforce_accounts.open_opportunities",
                "account_owner.name",
                "customer_success_manager.name",
                "ld_account_experimentation_usage_daily.is_using_experimentation_90d",
                "ld_account_experimentation_usage_daily.experimentation_events_received_mtd",
                "active_customer_entitlement.experimentation_events_entitled_to",
                "ld_experiments_daily.accounts_with_active_gses",
                "ld_experiments_daily.days_since_most_recent_iteration_start_by_account_avg",
                "salesforce_accounts.arr_sum",
            ],
            "filters": {
                "salesforce_accounts.type": "Customer",
                "salesforce_accounts.plan": "Enterprise,Enterprise 2023,Guardian",
                "salesforce_accounts.arr": ">50000",
                "ld_account_experimentation_usage_daily.is_using_experimentation_90d": "No",
                "all_dates.ld_calendar_month": "last month",
            },
            "sorts": ["salesforce_accounts.arr_sum desc"],
            "limit": 500,
        }

        response = requests.post(
            f"{self.LOOKER_BASE_URL}/api/4.0/queries/run/json",
            headers=headers,
            json=payload,
        )
        response.raise_for_status()
        return response.json()

    def _get_looker_token(self) -> str:
        response = requests.post(
            f"{self.LOOKER_BASE_URL}/api/4.0/login",
            data={
                "client_id": os.getenv("LOOKER_CLIENT_ID"),
                "client_secret": os.getenv("LOOKER_CLIENT_SECRET"),
            },
        )
        response.raise_for_status()
        return response.json()["access_token"]

    def _normalize_api_row(self, row: dict) -> AccountRecord:
        entitled = row.get("active_customer_entitlement.experimentation_events_entitled_to") or 0
        received = row.get("ld_account_experimentation_usage_daily.experimentation_events_received_mtd") or 0
        utilisation = (received / entitled) if entitled > 0 else None

        return AccountRecord(
            account_name=row.get("salesforce_accounts.sfdc_account_name"),
            sfdc_account_id=row.get("salesforce_accounts.sfdc_account_id"),
            ld_account_id=row.get("salesforce_accounts.ld_account_id"),
            arr=row.get("salesforce_accounts.arr"),
            plan=row.get("salesforce_accounts.plan"),
            rating=row.get("salesforce_accounts.rating"),
            geo=row.get("salesforce_accounts.geo"),
            industry=row.get("salesforce_accounts.industry"),
            renewal_date=row.get("salesforce_accounts.renewal_date"),
            ae=row.get("account_owner.name"),
            csm=row.get("customer_success_manager.name"),
            tier=1,
            source="looker",
            exp_events_mtd=received,
            exp_events_entitled=entitled,
            exp_utilisation_rate=utilisation,
            is_using_exp_90d=False,
            days_since_last_iteration=row.get(
                "ld_experiments_daily.days_since_most_recent_iteration_start_by_account_avg"
            ),
        )

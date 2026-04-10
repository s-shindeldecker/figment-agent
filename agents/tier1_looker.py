import csv
import os
from datetime import date, timedelta
from typing import Optional

import requests
from agents.base import AgentService
from core.schema import AccountRecord


# ---------------------------------------------------------------------------
# Snowflake direct-query SQL
#
# Derived from the Looker-generated SQL for the Experimentation Usage look.
# Simplifications vs. the raw Looker SQL:
#   • MD5 symmetric aggregates replaced with MAX() — same result after GROUP BY
#   • Date filter is parameterised (%(calendar_month)s) and generated at runtime
#   • Column aliases map 1:1 to AccountRecord fields, so _normalize_snowflake_row
#     is trivial
#
# If column or table names differ in your Snowflake instance, update the SQL
# here; no other code needs to change.
# ---------------------------------------------------------------------------
_SNOWFLAKE_SQL = """
SELECT
    MAX(sa.SFDC_ACCOUNT_NAME)                                              AS account_name,
    MAX(sa.SFDC_ACCOUNT_ID)                                                AS sfdc_account_id,
    MAX(sa.LD_ACCOUNT_ID)                                                  AS ld_account_id,
    MAX(sa.ARR)                                                            AS arr,
    MAX(sa.PLAN)                                                           AS plan,
    MAX(sa.RATING)                                                         AS rating,
    MAX(sa.GEO)                                                            AS geo,
    MAX(sa.INDUSTRY)                                                       AS industry,
    CAST(MAX(sa.RENEWAL_DATE) AS VARCHAR)                                  AS renewal_date,
    MAX(ao.NAME)                                                           AS ae,
    MAX(csm_u.NAME)                                                        AS csm,
    MAX(usage.EXPERIMENTATION_EVENTS_RECEIVED_MTD)                         AS exp_events_mtd,
    MAX(ent.EXPERIMENTATION_EVENTS_ENTITLED_TO)                            AS exp_events_entitled,
    MAX(exp.DAYS_SINCE_MOST_RECENT_ITERATION_START_BY_ACCOUNT_AVG)         AS days_since_last_iteration,
    MAX(exp.ACCOUNTS_WITH_ACTIVE_GSES)                                     AS active_experiments
FROM DATAMART.LD_ACCOUNT_EXPERIMENTATION_USAGE_DAILY usage
JOIN DATAMART.SALESFORCE_ACCOUNTS sa
    ON usage.LD_ACCOUNT_ID = sa.LD_ACCOUNT_ID
JOIN STG.ALL_DATES ad
    ON usage.DATE = ad.DATE_DAY
LEFT JOIN SALESFORCE_SRC.USER ao
    ON sa.ACCOUNT_OWNER_USER_ID = ao.ID
LEFT JOIN SALESFORCE_SRC.USER csm_u
    ON sa.CSM_USER_ID = csm_u.ID
LEFT JOIN DATAMART.ACTIVE_CUSTOMER_ENTITLEMENT ent
    ON sa.SFDC_ACCOUNT_ID = ent.SFDC_ACCOUNT_ID
LEFT JOIN DATAMART.LD_EXPERIMENTS_DAILY exp
    ON usage.LD_ACCOUNT_ID = exp.LD_ACCOUNT_ID
    AND usage.DATE = exp.DATE
WHERE
    sa.TYPE = 'Customer'
    AND sa.PLAN IN ('Enterprise', 'Enterprise 2023', 'Guardian')
    AND sa.ARR > 50000
    AND ad.LD_CALENDAR_MONTH = %(calendar_month)s
GROUP BY sa.LD_ACCOUNT_ID
HAVING MAX(usage.IS_USING_EXPERIMENTATION_90D) = 'No'
ORDER BY MAX(sa.ARR) DESC NULLS LAST
LIMIT 500
"""


def _snowflake_calendar_month() -> str:
    """
    Return the LD_CALENDAR_MONTH value for the previous calendar month,
    e.g. 'CY2026-M03' when today is any day in April 2026.
    """
    today = date.today()
    last_month = today.replace(day=1) - timedelta(days=1)
    return f"CY{last_month.year}-M{last_month.month:02d}"


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

# CSV headers that map into AccountRecord typed fields (all others → looker_extras).
_EXPORT_MAPPED_HEADERS = frozenset(EXPORT_COLUMN_MAP.values())


def _csv_cell_to_extra_str(val) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    if s in ("", "null", "NULL", "N/A"):
        return None
    return s


def _looker_extras_from_csv_row(row: dict) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, raw in row.items():
        if key in _EXPORT_MAPPED_HEADERS:
            continue
        s = _csv_cell_to_extra_str(raw)
        if s is not None:
            out[key] = s
    return out


# API JSON keys consumed by _normalize_api_row (remaining keys → looker_extras).
_LOOKER_API_MAPPED_KEYS = frozenset(
    {
        "salesforce_accounts.sfdc_account_name",
        "salesforce_accounts.sfdc_account_id",
        "salesforce_accounts.ld_account_id",
        "salesforce_accounts.arr",
        "salesforce_accounts.plan",
        "salesforce_accounts.rating",
        "salesforce_accounts.geo",
        "salesforce_accounts.industry",
        "salesforce_accounts.renewal_date",
        "account_owner.name",
        "customer_success_manager.name",
        "active_customer_entitlement.experimentation_events_entitled_to",
        "ld_account_experimentation_usage_daily.experimentation_events_received_mtd",
        "ld_experiments_daily.days_since_most_recent_iteration_start_by_account_avg",
    }
)


def _stringify_looker_api_value(val) -> Optional[str]:
    if val is None:
        return None
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, (int, float)):
        return str(val)
    s = str(val).strip()
    return s if s else None


def _looker_extras_from_api_row(row: dict) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, raw in row.items():
        if key in _LOOKER_API_MAPPED_KEYS:
            continue
        s = _stringify_looker_api_value(raw)
        if s is not None:
            out[key] = s
    return out


class Tier1LookerAgent(AgentService):
    """
    Collects Tier 1 accounts from Looker / Snowflake.

    Mode priority (first match wins):

      1. File mode  — set LOOKER_EXPORT_PATH to a CSV exported from the Look.
         Drop the file in data/ and point the env var at it.

      2. Snowflake mode — set SNOWFLAKE_ACCOUNT (+ USER/PASSWORD/WAREHOUSE/
         DATABASE). Executes _SNOWFLAKE_SQL directly against the data warehouse
         and normalises the result without touching Looker at all.
         Optional: SNOWFLAKE_SCHEMA (default DATAMART), SNOWFLAKE_ROLE.

      3. Looker API mode (future) — set LOOKER_CLIENT_ID + LOOKER_CLIENT_SECRET.
    """

    LOOKER_BASE_URL = os.getenv("LOOKER_BASE_URL", "https://launchdarkly.cloud.looker.com")

    async def run(self) -> list[AccountRecord]:
        self.log_graph_binding()

        export_path = os.getenv("LOOKER_EXPORT_PATH")
        if export_path:
            raw = self._load_from_file(export_path)
            return [self._normalize_export_row(row) for row in raw]

        if os.getenv("SNOWFLAKE_ACCOUNT"):
            raw = self._query_snowflake()
            return [self._normalize_snowflake_row(row) for row in raw]

        # Fall through to Looker API mode
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
            looker_extras=_looker_extras_from_csv_row(row),
        )

    # ------------------------------------------------------------------
    # Snowflake mode (SNOWFLAKE_ACCOUNT + credentials)
    # ------------------------------------------------------------------

    def _query_snowflake(self) -> list[dict]:
        """
        Execute _SNOWFLAKE_SQL directly against Snowflake and return a list
        of dicts keyed by the SQL column aliases (account_name, arr, ae, …).

        Required env vars:
            SNOWFLAKE_ACCOUNT   — account identifier, e.g. xy12345.us-east-1
            SNOWFLAKE_USER      — login name
            SNOWFLAKE_PASSWORD  — password (or use key-pair; see connector docs)
            SNOWFLAKE_WAREHOUSE — virtual warehouse name
            SNOWFLAKE_DATABASE  — database containing DATAMART schema

        Optional env vars:
            SNOWFLAKE_SCHEMA    — override default schema (default: DATAMART)
            SNOWFLAKE_ROLE      — role to assume on connect
        """
        try:
            import snowflake.connector
            from snowflake.connector import DictCursor
        except ImportError as exc:
            raise ImportError(
                "snowflake-connector-python is required for Snowflake mode. "
                "Run: pip install snowflake-connector-python"
            ) from exc

        calendar_month = _snowflake_calendar_month()
        print(f"[Tier1/Snowflake] Querying for calendar month: {calendar_month}")

        connect_kwargs = dict(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
            schema=os.getenv("SNOWFLAKE_SCHEMA", "DATAMART"),
        )
        role = os.getenv("SNOWFLAKE_ROLE")
        if role:
            connect_kwargs["role"] = role

        # Auth: key-pair takes priority over password
        private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
        if private_key_path:
            # Load PEM-encoded PKCS8 private key (encrypted or unencrypted)
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization

            passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
            passphrase_bytes = passphrase.encode() if passphrase else None

            with open(private_key_path, "rb") as f:
                private_key = serialization.load_pem_private_key(
                    f.read(),
                    password=passphrase_bytes,
                    backend=default_backend(),
                )
            connect_kwargs["private_key"] = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        else:
            connect_kwargs["password"] = os.environ["SNOWFLAKE_PASSWORD"]

        conn = snowflake.connector.connect(**connect_kwargs)
        try:
            cur = conn.cursor(DictCursor)
            cur.execute(_SNOWFLAKE_SQL, {"calendar_month": calendar_month})
            rows = cur.fetchall()
        finally:
            conn.close()

        print(f"[Tier1/Snowflake] {len(rows)} rows returned")
        return rows

    def _normalize_snowflake_row(self, row: dict) -> AccountRecord:
        """
        Normalise a row returned by _query_snowflake().

        The SQL aliases map 1:1 to AccountRecord field names, so this is
        straightforward — just type-coerce and handle nulls.
        """
        def get(key: str):
            val = row.get(key)
            if val is None:
                return None
            s = str(val).strip()
            return s if s not in ("", "None", "N/A") else None

        def to_float(key: str):
            val = row.get(key)
            if val is None:
                return None
            try:
                return float(str(val).replace(",", "").replace("$", "").strip())
            except (ValueError, TypeError):
                return None

        entitled = to_float("exp_events_entitled") or 0.0
        received = to_float("exp_events_mtd") or 0.0
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
            source="snowflake",
            exp_events_mtd=received,
            exp_events_entitled=entitled,
            exp_utilisation_rate=utilisation,
            is_using_exp_90d=False,     # HAVING clause already filters to No
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
            looker_extras=_looker_extras_from_api_row(row),
        )

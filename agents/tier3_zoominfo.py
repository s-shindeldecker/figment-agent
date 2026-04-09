"""
Tier 3 — ZoomInfo static export agent.

Current mode: reads two CSV exports from data/.
Future mode:  replace _load_csv() with a ZoomInfo API call once
              credentials are available (see TODO below).

Files:
  data/figment_t3_competitor_tech.csv   — companies using competitor / adjacent tech
  data/figment_t3_analytic_cms.csv      — companies using analytics / CMS platforms

Both files share the same ZoomInfo column schema (verified against actual exports).
"""

import os
import pandas as pd
from agents.base import AgentService
from core.schema import AccountRecord

# ---------------------------------------------------------------------------
# Paths — override via env vars if you move the files
# ---------------------------------------------------------------------------
COMPETITOR_TECH_PATH = os.getenv(
    "ZOOMINFO_COMPETITOR_TECH_PATH",
    "data/figment_t3_competitor_tech.csv",
)
ANALYTIC_CMS_PATH = os.getenv(
    "ZOOMINFO_ANALYTIC_CMS_PATH",
    "data/figment_t3_analytic_cms.csv",
)

# ---------------------------------------------------------------------------
# Column map: ZoomInfo export header → AccountRecord field
#
# Verified against actual export headers (both files share the same schema):
#   ZoomInfo Company ID, Company Name, Website, Founded Year,
#   Company HQ Phone, Fax, Ticker, Revenue (in 000s USD),
#   Revenue Range (in USD), Employees, Employee Range,
#   SIC Code 1, SIC Code 2, SIC Codes, NAICS Code 1, NAICS Code 2,
#   NAICS Codes, Primary Industry, Primary Sub-Industry,
#   All Industries, All Sub-Industries, Industry Hierarchical Category,
#   Secondary Industry Hierarchical Category, Alexa Rank,
#   ZoomInfo Company Profile URL, LinkedIn Company Profile URL,
#   Facebook Company Profile URL, Twitter Company Profile URL,
#   Ownership Type, Business Model, Certified Active Company,
#   Certification Date, Total Funding Amount (in 000s USD),
#   Recent Funding Amount (in 000s USD), Recent Funding Round,
#   Recent Funding Date, Recent Investors, All Investors,
#   Company Street Address, Company City, Company State,
#   Company Zip Code, Company Country, Full Address,
#   Number of Locations, Company Is Acquired,
#   Company ID (Ultimate Parent), Entity Name (Ultimate Parent),
#   Company ID (Immediate Parent), Entity Name (Immediate Parent),
#   Relationship (Immediate Parent), Query Name
# ---------------------------------------------------------------------------
COLUMN_MAP = {
    "account_name": "Company Name",
    "industry":     "Primary Industry",
    "geo":          "Company Country",
    "arr":          "Revenue (in 000s USD)",   # in thousands — multiplied ×1000 in normalize
    "notes":        "Query Name",              # the ZoomInfo search/list that produced this row
}


class Tier3ZoomInfoAgent(AgentService):
    """
    Tier 3 agent: ZoomInfo prospect data (competitor tech + analytics/CMS).

    Reads two CSV exports, normalises each row to AccountRecord, and
    returns a combined list with tier=3.
    """

    async def run(self) -> list[AccountRecord]:
        records: list[AccountRecord] = []

        competitor_df, analytic_df = _load_csvs()

        print(f"[Tier3/ZoomInfo] competitor_tech columns: {list(competitor_df.columns)}")
        print(f"[Tier3/ZoomInfo] analytic_cms columns:    {list(analytic_df.columns)}")

        for _, row in competitor_df.iterrows():
            records.append(_normalize(row, source="zoominfo_competitor_tech"))

        for _, row in analytic_df.iterrows():
            records.append(_normalize(row, source="zoominfo_analytic_cms"))

        print(f"[Tier3/ZoomInfo] {len(records)} total records "
              f"({len(competitor_df)} competitor_tech + {len(analytic_df)} analytic_cms)")
        return records


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_csvs() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load both ZoomInfo CSV exports.

    TODO: Replace this function with ZoomInfo API calls once credentials
    are available. The API endpoint is:
        GET https://api.zoominfo.com/search/company
    Auth: OAuth2 client_credentials using ZOOMINFO_CLIENT_ID + ZOOMINFO_CLIENT_SECRET.
    The response schema maps to the same COLUMN_MAP fields above.
    See: https://api-docs.zoominfo.com/
    """
    for path in (COMPETITOR_TECH_PATH, ANALYTIC_CMS_PATH):
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"ZoomInfo export not found: {path}\n"
                f"Drop the CSV into data/ or set the env var pointing to its location."
            )

    competitor_df = pd.read_csv(COMPETITOR_TECH_PATH, dtype=str)
    analytic_df   = pd.read_csv(ANALYTIC_CMS_PATH,   dtype=str)

    return competitor_df, analytic_df


def _normalize(row: pd.Series, source: str) -> AccountRecord:
    """Map a single ZoomInfo row to an AccountRecord."""

    def get(col: str):
        val = row.get(col, "")
        if pd.isna(val) or str(val).strip() in ("", "nan", "N/A", "-"):
            return None
        return str(val).strip()

    def to_float(col: str):
        val = get(col)
        if val is None:
            return None
        try:
            return float(val.replace(",", "").replace("$", ""))
        except (ValueError, TypeError):
            return None

    # Revenue is stored in thousands — convert to full dollars to match ARR units
    revenue_thousands = to_float(COLUMN_MAP["arr"])
    arr = revenue_thousands * 1000 if revenue_thousands is not None else None

    return AccountRecord(
        account_name=get(COLUMN_MAP["account_name"]),
        industry=get(COLUMN_MAP["industry"]),
        geo=get(COLUMN_MAP["geo"]),
        arr=arr,
        tier=3,
        source=source,
        notes=get(COLUMN_MAP["notes"]),
    )

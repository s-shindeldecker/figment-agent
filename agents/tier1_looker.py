import os
import requests
from agents.base import AgentService
from core.schema import AccountRecord


class Tier1LookerAgent(AgentService):
    """
    Queries Looker API for the Experimentation Usage explore.
    Equivalent to the saved Look built manually:
      - Enterprise customers
      - ARR > $50K
      - Is Using Experimentation 90d = No
      - Most recent month
    Returns normalized AccountRecord list.
    """

    LOOKER_BASE_URL = os.getenv("LOOKER_BASE_URL", "https://your-instance.looker.com")

    async def run(self) -> list[AccountRecord]:
        config = self.get_config()
        # config.config contains model + instructions from LD AI Config
        # For Tier 1, the "agent" is really a structured API call
        # The AI Config instructions define how to interpret edge cases

        raw = self._query_looker()
        return [self._normalize(row) for row in raw]

    def _query_looker(self) -> list[dict]:
        """
        Run inline query against Experimentation Usage explore.
        Uses Looker API v4. Requires LOOKER_CLIENT_ID + LOOKER_CLIENT_SECRET.
        """
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

    def _normalize(self, row: dict) -> AccountRecord:
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
            is_using_exp_90d=False,     # Filtered to No above
            days_since_last_iteration=row.get(
                "ld_experiments_daily.days_since_most_recent_iteration_start_by_account_avg"
            ),
        )

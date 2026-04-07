from agents.base import AgentService
from core.schema import AccountRecord


ENTERPRET_PROMPTS = {
    "competitor_discovery": """
        Which customer accounts have the most mentions of Statsig, Optimizely,
        Eppo, or other A/B testing or experimentation tools in Gong calls from
        the last 12 months? List the top accounts by mention frequency and
        summarize whether they are evaluating, frustrated, or actively using
        a competitor alongside LaunchDarkly.
        Return as JSON array: [{account_name, competitor, urgency, context, quote}]
    """,
    "switching_intent": """
        Of the accounts currently evaluating or running Statsig or Eppo
        alongside or instead of LaunchDarkly, which ones have the strongest
        signals of dissatisfaction or active switching intent?
        Return as JSON array: [{account_name, competitor, urgency, deal_context,
        competitor_spend, renewal_window_months}]
    """,
    "eppo_coverage": """
        Which accounts mention Eppo in Gong calls or Zendesk tickets in the
        last 12 months? For each account, note whether Eppo is expanding
        inside an existing LD customer relationship.
        Return as JSON array: [{account_name, competitor, urgency, context,
        is_existing_ld_customer}]
    """,
}


class Tier2EntrepretAgent(AgentService):
    """
    Queries Enterpret via MCP for competitive intelligence.
    Uses the three prompts validated manually during prototype phase.
    Parses structured JSON output into AccountRecord list.
    """

    async def run(self) -> list[AccountRecord]:
        config = self.get_config()
        # AI Config instructions in LD define how to interpret
        # ambiguous competitor mentions and edge cases

        all_accounts: dict[str, AccountRecord] = {}

        for prompt_key, prompt_text in ENTERPRET_PROMPTS.items():
            raw = await self._query_enterpret(prompt_text)
            for item in raw:
                name = item.get("account_name")
                if not name:
                    continue
                if name not in all_accounts:
                    all_accounts[name] = self._normalize(item)
                else:
                    # Merge signals from multiple prompts
                    existing = all_accounts[name]
                    if item.get("urgency") == "immediate":
                        existing.urgency = "immediate"
                    if item.get("deal_context"):
                        existing.deal_context = item["deal_context"]

        return list(all_accounts.values())

    async def _query_enterpret(self, prompt: str) -> list[dict]:
        """
        Call Enterpret via MCP server.
        Returns parsed JSON list of account objects.

        Replace with your actual MCP client implementation.
        """
        # TODO: Replace with your actual MCP client call
        # Example pattern:
        # from your_mcp_client import call_mcp_tool
        # response = await call_mcp_tool(
        #     server="enterpret",
        #     tool="wisdom_query",
        #     params={"prompt": prompt, "return_format": "json"}
        # )
        # return response.get("accounts", [])
        raise NotImplementedError("MCP client not yet configured — see _query_enterpret docstring")

    def _normalize(self, item: dict) -> AccountRecord:
        urgency_map = {
            "immediate": "immediate",
            "active": "active",
            "watch": "watch",
            "high": "immediate",
            "medium": "active",
            "low": "watch",
        }
        return AccountRecord(
            account_name=item.get("account_name"),
            tier=2,
            source="enterpret",
            competitor=item.get("competitor"),
            competitor_spend=item.get("competitor_spend"),
            renewal_window_months=item.get("renewal_window_months"),
            urgency=urgency_map.get(
                str(item.get("urgency", "")).lower(), "watch"
            ),
            deal_context=item.get("context") or item.get("deal_context"),
        )

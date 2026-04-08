import asyncio
import os
from typing import List, Optional, Tuple

from agents.base import AgentService
from agents.wisdom_prompts import (
    WISDOM_CYPHER_ENV_SUFFIX_BY_FLAG_KEY,
    resolve_wisdom_prompt_jobs,
)
from agents.wisdom_mcp import (
    DEFAULT_WISDOM_MCP_URL,
    WisdomMCPClient,
    WisdomMCPError,
    run_wisdom_query,
    wisdom_warmup_if_available,
)
from core.schema import AccountRecord

def _sanitize_for_env_key(key: str) -> str:
    out = []
    for c in key.upper():
        if c.isalnum():
            out.append(c)
        else:
            out.append("_")
    s = "".join(out).strip("_")
    while "__" in s:
        s = s.replace("__", "_")
    return s


def _cypher_for_job_key(job_key: str) -> Optional[str]:
    """
    Cypher for this Tier-2 job. LD flags use ``WISDOM_CYPHER_ENV_SUFFIX_BY_FLAG_KEY``;
    other keys (e.g. ``settings.yaml``) use ``WISDOM_CYPHER_<SANITIZED_KEY>``.
    Fallback: ``WISDOM_CYPHER``.
    """
    mapped = WISDOM_CYPHER_ENV_SUFFIX_BY_FLAG_KEY.get(job_key)
    if mapped:
        v = (os.getenv(f"WISDOM_CYPHER_{mapped}") or "").strip()
        if v:
            return v
    suffix = _sanitize_for_env_key(job_key)
    if suffix:
        v = (os.getenv(f"WISDOM_CYPHER_{suffix}") or "").strip()
        if v:
            return v
    return (os.getenv("WISDOM_CYPHER") or "").strip() or None


def _tier2_parallel_enabled() -> bool:
    v = (os.getenv("WISDOM_TIER2_PARALLEL") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


async def execute_wisdom_prompt_jobs(
    ai_client,
    context,
    prompt_jobs: List[Tuple[str, str]],
    *,
    log_prefix: str = "[Tier2]",
) -> List[AccountRecord]:
    """
    Run one or more Wisdom MCP queries and return merged AccountRecords.

    ``prompt_jobs`` is (flag_key_or_job_key, instruction_body); Cypher env uses the same key
    (see ``_cypher_for_job_key``).
    Uses ``Tier2EntrepretAgent`` for row normalization. Requires WISDOM_AUTH_TOKEN.
    """
    token = (os.getenv("WISDOM_AUTH_TOKEN") or "").strip()
    if not token:
        raise WisdomMCPError(
            "WISDOM_AUTH_TOKEN is not set. Generate a token under "
            "Enterpret Settings → Wisdom MCP."
        )

    if not prompt_jobs:
        return []

    agent = Tier2EntrepretAgent(
        ai_client,
        prompt_jobs[0][0],
        context,
        graph=None,
    )

    labels = ", ".join(pid for pid, _ in prompt_jobs)
    print(f"{log_prefix} {len(prompt_jobs)} Wisdom quer(y|ies) — configs: {labels}")

    base_url = (os.getenv("WISDOM_SERVER_URL") or DEFAULT_WISDOM_MCP_URL).strip().rstrip("/")
    tool_override = (os.getenv("WISDOM_TIER2_TOOL") or "").strip() or None

    all_accounts: dict[str, AccountRecord] = {}

    use_parallel = _tier2_parallel_enabled() and len(prompt_jobs) > 1
    if use_parallel:
        print(f"{log_prefix} WISDOM_TIER2_PARALLEL=1 — one MCP session per query")
        chunks = await _gather_wisdom_jobs_parallel(
            base_url, token, tool_override, prompt_jobs
        )
        for raw in chunks:
            _merge_wisdom_rows_into(agent, all_accounts, raw)
    else:
        async with WisdomMCPClient(base_url, token) as client:
            tools = await client.list_tools()
            await wisdom_warmup_if_available(client, tools)
            for job_key, body in prompt_jobs:
                cypher = _cypher_for_job_key(job_key)
                raw = await run_wisdom_query(
                    client,
                    tools,
                    body,
                    cypher=cypher,
                    tool_override=tool_override,
                )
                _merge_wisdom_rows_into(agent, all_accounts, raw)

    return list(all_accounts.values())


def _merge_wisdom_rows_into(
    agent: "Tier2EntrepretAgent",
    all_accounts: dict[str, AccountRecord],
    raw: List[dict],
) -> None:
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = _resolve_account_name(item)
        if not name:
            continue
        if name not in all_accounts:
            all_accounts[name] = agent._normalize(item, name)
        else:
            existing = all_accounts[name]
            if str(item.get("urgency", "")).lower() in ("immediate", "high"):
                existing.urgency = "immediate"
            ctx = item.get("deal_context") or item.get("context")
            if ctx:
                existing.deal_context = ctx


async def _gather_wisdom_jobs_parallel(
    base_url: str,
    token: str,
    tool_override: Optional[str],
    prompt_jobs: List[Tuple[str, str]],
) -> List[List[dict]]:
    """One WisdomMCPClient session per job (MCP session is not safely concurrent)."""

    async def _one(job: Tuple[str, str]) -> List[dict]:
        job_key, body = job
        async with WisdomMCPClient(base_url, token) as client:
            tools = await client.list_tools()
            await wisdom_warmup_if_available(client, tools)
            cypher = _cypher_for_job_key(job_key)
            return await run_wisdom_query(
                client,
                tools,
                body,
                cypher=cypher,
                tool_override=tool_override,
            )

    return list(await asyncio.gather(*[_one(j) for j in prompt_jobs]))


def _resolve_account_name(item: dict) -> str:
    for key in (
        "account_name",
        "accountName",
        "customer_name",
        "customerName",
        "entity_name",
        "entityName",
        "title",
        "label",
        "name",
        "account",
        "company",
        "company_name",
    ):
        v = item.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


class Tier2EntrepretAgent(AgentService):
    """
    Enterpret Wisdom via MCP.

    Prompts come from LaunchDarkly **string flags** (see ``agents.wisdom_prompts``) or
    ``wisdom.tier2_prompt_fallback`` in ``config/settings.yaml`` when LD is unavailable.

    **Parallel:** ``WISDOM_TIER2_PARALLEL=1`` runs multiple queries on separate MCP sessions.

    Env: WISDOM_AUTH_TOKEN, WISDOM_SERVER_URL, WISDOM_TIER2_PARALLEL, WISDOM_TIER2_TOOL,
         WISDOM_CYPHER, WISDOM_CYPHER_* (see wisdom_prompts.WISDOM_CYPHER_ENV_SUFFIX_BY_FLAG_KEY)
    """

    def __init__(self, ai_client, config_key: str, context, graph=None, ld_client=None):
        super().__init__(ai_client, config_key, context, graph)
        self.ld_client = ld_client

    async def run(self) -> list[AccountRecord]:
        prompt_jobs, prompt_source = resolve_wisdom_prompt_jobs(self.ld_client, self.context)
        print(f"[Tier2] Prompt source: {prompt_source}")
        return await execute_wisdom_prompt_jobs(
            self.ai_client,
            self.context,
            prompt_jobs,
            log_prefix="[Tier2]",
        )

    def _normalize(self, item: dict, account_name: str) -> AccountRecord:
        urgency_map = {
            "immediate": "immediate",
            "active": "active",
            "watch": "watch",
            "high": "immediate",
            "medium": "active",
            "low": "watch",
        }
        return AccountRecord(
            account_name=account_name,
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

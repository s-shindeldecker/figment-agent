import asyncio
import json
import os
from typing import Dict, List, Optional

from agents.base import AgentService
from agents import ld_wisdom_config as _ld_wisdom
from agents.wisdom_cypher_defaults import (
    describe_embedded_cypher_key_sources,
    get_embedded_cypher_queries_for_suffix,
)
from agents.wisdom_prompts import (
    WISDOM_CYPHER_ENV_SUFFIX_BY_FLAG_KEY,
    tier2_job_keys,
)
from agents.wisdom_mcp import (
    DEFAULT_WISDOM_MCP_URL,
    WisdomMCPClient,
    WisdomMCPError,
    run_wisdom_query,
    wisdom_warmup_if_available,
)
from core.schema import AccountRecord


def _parse_arr_value(val) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, bool):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        s = val.strip().replace("$", "").replace(",", "")
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


# Keys consumed by _resolve_account_name, _normalize, or _build_deal_context_from_row.
_WISDOM_ACCOUNT_NAME_KEYS = frozenset(
    {
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
    }
)

# Note: mention_count, signal_count, source_query are intentionally omitted here so they
# land in wisdom_extras for config/e100_output_columns.yaml. _build_deal_context_from_row
# still reads them from the raw item dict.
_WISDOM_CORE_KEYS = frozenset(
    {
        "urgency",
        "context",
        "deal_context",
        "competitor",
        "primary_competitor",
        "competitor_spend",
        "renewal_window_months",
        "account_id",
        "arr",
        "plan",
        "renewal_date",
        "customer_region",
        "geo",
        "csm_name",
        "csm",
        "source",
        "competition_notes",
        "source_url",
        "support_tier",
        "gong_count",
        "zendesk_count",
    }
) | _WISDOM_ACCOUNT_NAME_KEYS

_WISDOM_EXTRA_MAX_LEN = 10000


def _stringify_wisdom_extra_value(val) -> str:
    if val is None:
        return ""
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, str):
        return val.strip()[:_WISDOM_EXTRA_MAX_LEN]
    if isinstance(val, (list, dict)):
        try:
            s = json.dumps(val, default=str)
        except TypeError:
            s = str(val)
        return s[:_WISDOM_EXTRA_MAX_LEN]
    s = str(val)
    return s[:_WISDOM_EXTRA_MAX_LEN]


def _wisdom_extras_from_item(item: dict) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for key, raw in item.items():
        if key in _WISDOM_CORE_KEYS or key.startswith("_"):
            continue
        s = _stringify_wisdom_extra_value(raw)
        if s:
            out[key] = s
    return out


def _string_field(item: dict, key: str) -> Optional[str]:
    v = item.get(key)
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None


def _build_deal_context_from_row(item: dict) -> Optional[str]:
    """Summarize Cypher RETURN columns into deal_context."""
    parts: List[str] = []
    sq = _string_field(item, "source_query")
    if sq:
        parts.append(f"query={sq}")
    src = _string_field(item, "source")
    if src:
        parts.append(f"source={src}")
    notes = _string_field(item, "competition_notes")
    if notes:
        parts.append(notes)
    url = _string_field(item, "source_url")
    if url:
        parts.append(f"url={url}")
    if item.get("mention_count") is not None:
        parts.append(f"mentions={item['mention_count']}")
    if item.get("signal_count") is not None:
        parts.append(f"signals={item['signal_count']}")
    st = _string_field(item, "support_tier")
    if st:
        parts.append(f"support_tier={st}")
    gong, zendesk = item.get("gong_count"), item.get("zendesk_count")
    if gong is not None or zendesk is not None:
        parts.append(f"gong={gong} zendesk={zendesk}")
    if parts:
        return " | ".join(parts)
    return _string_field(item, "deal_context") or _string_field(item, "context")


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


def _cyphers_for_job_key(job_key: str) -> List[str]:
    """
    Cypher string(s) for this Tier-2 job. Order: env ``WISDOM_CYPHER_<SUFFIX>`` (one string
    replaces the whole job), env per sanitized job key, merged ``config/wisdom_cypher.yaml``
    + per-key LaunchDarkly flags (unless ``WISDOM_DISABLE_EMBEDDED_CYPHER`` /
    ``WISDOM_DISABLE_LD_CYPHER``), then global ``WISDOM_CYPHER``. Empty list is invalid
    for Tier-2 runs (validated before MCP calls).
    """
    mapped = WISDOM_CYPHER_ENV_SUFFIX_BY_FLAG_KEY.get(job_key)
    if mapped:
        v = (os.getenv(f"WISDOM_CYPHER_{mapped}") or "").strip()
        if v:
            return [v]
    suffix = _sanitize_for_env_key(job_key)
    if suffix:
        v = (os.getenv(f"WISDOM_CYPHER_{suffix}") or "").strip()
        if v:
            return [v]
    if mapped:
        emb = get_embedded_cypher_queries_for_suffix(mapped)
        if emb:
            return emb
    glo = (os.getenv("WISDOM_CYPHER") or "").strip()
    if glo:
        return [glo]
    return []


def _tier2_parallel_enabled() -> bool:
    v = (os.getenv("WISDOM_TIER2_PARALLEL") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _log_tier2_cypher_sources(
    job_keys: List[str],
    *,
    log_prefix: str = "[Tier2]",
    verbosity: int = 0,
) -> None:
    """Log whether each job's Cypher comes from env, LaunchDarkly, or repo YAML."""
    for job_key in job_keys:
        mapped = WISDOM_CYPHER_ENV_SUFFIX_BY_FLAG_KEY.get(job_key)
        if mapped:
            v = (os.getenv(f"WISDOM_CYPHER_{mapped}") or "").strip()
            if v:
                print(
                    f"{log_prefix} {job_key}: Cypher from environment "
                    f"WISDOM_CYPHER_{mapped} (single query; skips YAML/LD merge)"
                )
                continue
        suffix = _sanitize_for_env_key(job_key)
        if suffix:
            v = (os.getenv(f"WISDOM_CYPHER_{suffix}") or "").strip()
            if v:
                print(
                    f"{log_prefix} {job_key}: Cypher from environment "
                    f"WISDOM_CYPHER_{suffix}"
                )
                continue
        if mapped:
            pairs = describe_embedded_cypher_key_sources(mapped)
            if pairs:
                parts: List[str] = []
                for k, src in pairs:
                    if src == "launchdarkly" and verbosity >= _ld_wisdom.TIER2_LOG_BASIC:
                        fk = _ld_wisdom.ld_flag_key_for_cypher_map_key(k)
                        parts.append(f"{k}=LaunchDarkly({fk})")
                    else:
                        parts.append(
                            f"{k}={'LaunchDarkly' if src == 'launchdarkly' else 'repo YAML'}"
                        )
                detail = ", ".join(parts)
                print(
                    f"{log_prefix} {job_key}: Cypher from merged map ({detail})"
                )
                continue
        glo = (os.getenv("WISDOM_CYPHER") or "").strip()
        if glo:
            print(
                f"{log_prefix} {job_key}: Cypher from environment WISDOM_CYPHER "
                "(global; single query)"
            )
            continue
        print(
            f"{log_prefix} {job_key}: no Cypher resolved (should not happen after validation)"
        )


def _wisdom_mcp_calls_per_job(job_key: str) -> int:
    """One execute_cypher_query call per Cypher string for this job."""
    return len(_cyphers_for_job_key(job_key))


def _validate_tier2_cypher_for_jobs(job_keys: List[str]) -> None:
    """Tier 2 does not fall back to search_knowledge_graph; Cypher is required."""
    missing = [jk for jk in job_keys if not _cyphers_for_job_key(jk)]
    if missing:
        raise WisdomMCPError(
            "Tier 2 requires Cypher for every job (no prose / search fallback). "
            f"No Cypher for: {', '.join(missing)}. "
            "Provide config/wisdom_cypher.yaml, LaunchDarkly flags per map key "
            "(see agents.ld_wisdom_config), or WISDOM_CYPHER_* / WISDOM_CYPHER."
        )


async def execute_wisdom_tier2_jobs(
    job_keys: Optional[List[str]] = None,
    *,
    log_prefix: str = "[Tier2]",
) -> List[AccountRecord]:
    """
    Run Wisdom MCP ``execute_cypher_query`` for each Tier-2 job and merge rows.

    Cypher resolution: env overrides, then merged ``wisdom_cypher.yaml`` + per-key
    LaunchDarkly flags. Requires WISDOM_AUTH_TOKEN. No ``search_knowledge_graph`` path.

    Extra console lines are gated by LaunchDarkly string flag
    ``figment-agent-tier2-log-verbosity`` (or env ``WISDOM_TIER2_LOG_VERBOSITY``); see
    ``agents.ld_wisdom_config.get_tier2_log_verbosity_level``.
    """
    token = (os.getenv("WISDOM_AUTH_TOKEN") or "").strip()
    if not token:
        raise WisdomMCPError(
            "WISDOM_AUTH_TOKEN is not set. Generate a token under "
            "Enterpret Settings → Wisdom MCP."
        )

    keys = list(job_keys) if job_keys is not None else tier2_job_keys()
    if not keys:
        return []

    # Resolve before validation so YAML/LD merge logs see the same cached verbosity
    # as get_ld_cypher_for_map_key (debug lines on Cypher flags).
    log_verbosity = _ld_wisdom.get_tier2_log_verbosity_level()

    _validate_tier2_cypher_for_jobs(keys)

    agent = Tier2EntrepretAgent(None, keys[0], None, graph=None)

    labels = ", ".join(keys)
    total_calls = sum(_wisdom_mcp_calls_per_job(jk) for jk in keys)
    print(f"{log_prefix} Cypher-only Tier-2: {len(keys)} job(s) — {labels}")
    if log_verbosity >= _ld_wisdom.TIER2_LOG_BASIC:
        print(
            f"{log_prefix}[log] verbosity={_ld_wisdom.tier2_log_level_name(log_verbosity)} "
            f"(level={log_verbosity}) — {_ld_wisdom.tier2_log_verbosity_source_hint()}"
        )
    _log_tier2_cypher_sources(
        keys, log_prefix=log_prefix, verbosity=log_verbosity
    )
    print(
        f"{log_prefix} {len(keys)} Tier-2 job(s), {total_calls} Wisdom MCP "
        f"execute_cypher_query call(s) — job keys: {labels}"
    )

    base_url = (os.getenv("WISDOM_SERVER_URL") or DEFAULT_WISDOM_MCP_URL).strip().rstrip("/")
    tool_override = (os.getenv("WISDOM_TIER2_TOOL") or "").strip() or None

    all_accounts: dict[str, AccountRecord] = {}

    use_parallel = _tier2_parallel_enabled() and len(keys) > 1
    if use_parallel:
        print(
            f"{log_prefix} WISDOM_TIER2_PARALLEL=1 — one MCP session per Tier-2 job "
            f"(each job may run multiple Cypher calls in that session)"
        )
        chunks = await _gather_wisdom_jobs_parallel(
            base_url,
            token,
            tool_override,
            keys,
            log_prefix=log_prefix,
            log_verbosity=log_verbosity,
        )
        for raw in chunks:
            _merge_wisdom_rows_into(agent, all_accounts, raw)
    else:
        async with WisdomMCPClient(base_url, token) as client:
            tools = await client.list_tools()
            await wisdom_warmup_if_available(client, tools)
            for job_key in keys:
                cyphers = _cyphers_for_job_key(job_key)
                for idx, cy in enumerate(cyphers, start=1):
                    if len(cyphers) > 1:
                        print(
                            f"{log_prefix} {job_key} — Cypher {idx}/{len(cyphers)}"
                        )
                    raw = await run_wisdom_query(
                        client,
                        tools,
                        "",
                        cypher=cy,
                        tool_override=tool_override,
                    )
                    if log_verbosity >= _ld_wisdom.TIER2_LOG_BASIC:
                        print(
                            f"{log_prefix}[log] {job_key} execute_cypher_query "
                            f"{idx}/{len(cyphers)}: {len(raw)} row(s) (pre-merge)"
                        )
                    _merge_wisdom_rows_into(agent, all_accounts, raw)

    out = list(all_accounts.values())
    if log_verbosity >= _ld_wisdom.TIER2_LOG_MONITOR:
        print(
            json.dumps(
                {
                    "figment_event": "tier2_wisdom_summary",
                    "tier2_job_count": len(keys),
                    "wisdom_mcp_cypher_calls": total_calls,
                    "unique_accounts_loaded": len(out),
                },
                separators=(",", ":"),
            )
        )
    if not out and keys:
        print(
            f"{log_prefix} Wisdom returned no account rows for any query. "
            "Common causes: Enterpret ServiceError, or Cypher returning no rows. "
            "Check config/wisdom_cypher.yaml, LD flags, or WISDOM_CYPHER_* / WISDOM_CYPHER."
        )
    return out


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
            if item.get("source_query") == "switching_intent" and existing.urgency != "immediate":
                existing.urgency = "active"
            new_ctx = _build_deal_context_from_row(item)
            if new_ctx:
                if existing.deal_context and existing.deal_context.strip() != new_ctx.strip():
                    existing.deal_context = f"{existing.deal_context}\n---\n{new_ctx}"
                elif not existing.deal_context:
                    existing.deal_context = new_ctx
            for ek, ev in _wisdom_extras_from_item(item).items():
                existing.wisdom_extras[ek] = ev


async def _gather_wisdom_jobs_parallel(
    base_url: str,
    token: str,
    tool_override: Optional[str],
    job_keys: List[str],
    *,
    log_prefix: str = "[Tier2]",
    log_verbosity: int = 0,
) -> List[List[dict]]:
    """One WisdomMCPClient session per job (MCP session is not safely concurrent)."""

    async def _one(job_key: str) -> List[dict]:
        cyphers = _cyphers_for_job_key(job_key)
        async with WisdomMCPClient(base_url, token) as client:
            tools = await client.list_tools()
            await wisdom_warmup_if_available(client, tools)
            combined: List[dict] = []
            for idx, cy in enumerate(cyphers, start=1):
                if len(cyphers) > 1:
                    print(
                        f"{log_prefix} {job_key} — Cypher {idx}/{len(cyphers)}"
                    )
                chunk = await run_wisdom_query(
                    client,
                    tools,
                    "",
                    cypher=cy,
                    tool_override=tool_override,
                )
                if log_verbosity >= _ld_wisdom.TIER2_LOG_BASIC:
                    print(
                        f"{log_prefix}[log] {job_key} execute_cypher_query "
                        f"{idx}/{len(cyphers)}: {len(chunk)} row(s) (pre-merge)"
                    )
                combined.extend(chunk)
            return combined

    return list(await asyncio.gather(*[_one(jk) for jk in job_keys]))


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
    Enterpret Wisdom via MCP (``execute_cypher_query`` only).

    Cypher from env, ``config/wisdom_cypher.yaml``, and per-key LaunchDarkly flags.

    **Parallel:** ``WISDOM_TIER2_PARALLEL=1`` runs multiple jobs on separate MCP sessions.

    Env: WISDOM_AUTH_TOKEN, WISDOM_SERVER_URL, WISDOM_TIER2_PARALLEL, WISDOM_TIER2_TOOL,
         WISDOM_CYPHER, WISDOM_CYPHER_*, WISDOM_DISABLE_EMBEDDED_CYPHER,
         WISDOM_DISABLE_LD_CYPHER, LD_SDK_KEY, LD_FLAG_CYPHER_*,
         WISDOM_TIER2_LOG_VERBOSITY, WISDOM_DISABLE_LD_LOG_VERBOSITY, LD_FLAG_LOG_VERBOSITY.
    """

    async def run(self) -> list[AccountRecord]:
        return await execute_wisdom_tier2_jobs(tier2_job_keys(), log_prefix="[Tier2]")

    def _normalize(self, item: dict, account_name: str) -> AccountRecord:
        urgency_map = {
            "immediate": "immediate",
            "active": "active",
            "watch": "watch",
            "high": "immediate",
            "medium": "active",
            "low": "watch",
        }
        raw_u = str(item.get("urgency", "")).lower()
        if raw_u in urgency_map:
            urgency = urgency_map[raw_u]
        elif item.get("source_query") == "switching_intent":
            urgency = "active"
        else:
            urgency = "watch"
        ctx = _build_deal_context_from_row(item)
        if not ctx:
            ctx = item.get("context") or item.get("deal_context")
        comp = _string_field(item, "competitor") or _string_field(
            item, "primary_competitor"
        )
        return AccountRecord(
            account_name=account_name,
            tier=2,
            source="enterpret",
            sfdc_account_id=_string_field(item, "account_id"),
            arr=_parse_arr_value(item.get("arr")),
            plan=_string_field(item, "plan"),
            renewal_date=_string_field(item, "renewal_date"),
            geo=_string_field(item, "customer_region") or _string_field(item, "geo"),
            csm=_string_field(item, "csm_name") or _string_field(item, "csm"),
            competitor=comp,
            competitor_spend=_parse_arr_value(item.get("competitor_spend")),
            renewal_window_months=item.get("renewal_window_months")
            if isinstance(item.get("renewal_window_months"), int)
            else None,
            urgency=urgency,
            deal_context=ctx if isinstance(ctx, str) else None,
            wisdom_extras=_wisdom_extras_from_item(item),
        )

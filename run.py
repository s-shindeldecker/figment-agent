import asyncio
import os
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

from agents.prioritizer import prioritize_with_ai_config, prioritizer_llm_requested
from agents.tier1_looker import Tier1LookerAgent
from agents.tier2_enterpret import WisdomMCPError, execute_wisdom_prompt_jobs
from agents.tier3_stub import collect as collect_tier3
from agents.wisdom_prompts import resolve_wisdom_prompt_jobs
from core.deduplicator import deduplicate
from core.merger import merge_and_score

# LaunchDarkly context key for flag / AI Config evaluation
LD_RUN_CONTEXT_KEY = "e100-agent"


def _print_results(accounts: list):
    """Print ranked account list to console."""
    print(f"\n{'='*70}")
    print(f"  E100 RESULTS — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  {len(accounts)} accounts")
    print(f"{'='*70}\n")

    print(f"{'#':<4} {'Account':<45} {'Tier':<5} {'Score':<7} {'ARR':<14} {'AE'}")
    print(f"{'-'*4} {'-'*45} {'-'*5} {'-'*7} {'-'*14} {'-'*20}")

    for acct in accounts:
        arr = f"${acct.arr:,.0f}" if acct.arr else "N/A"
        print(
            f"{acct.priority_rank or '?':<4} "
            f"{(acct.account_name or 'Unknown')[:44]:<45} "
            f"T{acct.tier or '?':<4} "
            f"{acct.expansion_score or 0:<7.1f} "
            f"{arr:<14} "
            f"{acct.ae or ''}"
        )
    print()


async def run_e100_refresh():
    ld_sdk_key = os.getenv("LD_SDK_KEY")
    ai_client = None
    ld_client = None

    if ld_sdk_key:
        from agents.base import init_ld_clients
        ld_client, ai_client = init_ld_clients(ld_sdk_key)
        print("[LD] Connected to LaunchDarkly")
    else:
        print("[LD] LD_SDK_KEY not set — running in local mode")

    if ld_client:
        import ldclient
        context = ldclient.Context.builder(LD_RUN_CONTEXT_KEY) \
            .kind("user") \
            .name("E100 Weekly Refresh") \
            .set("run_date", datetime.now().isoformat()) \
            .build()
    else:
        context = None

    # ---- Tier 1: Looker ---------------------------------------------------
    tier1_agent = Tier1LookerAgent(ai_client, "e100-tier1-looker", context, graph=None)
    tier1_accounts = await tier1_agent.run()
    print(f"[Tier1] {len(tier1_accounts)} accounts loaded")

    combined: list = list(tier1_accounts)

    # ---- Tier 2: Wisdom (string flags + YAML fallback) --------------------
    wisdom_token = (os.getenv("WISDOM_AUTH_TOKEN") or "").strip()
    if wisdom_token:
        try:
            prompt_jobs, _src = resolve_wisdom_prompt_jobs(ld_client, context)
            tier2_accounts = await execute_wisdom_prompt_jobs(
                ai_client,
                context,
                prompt_jobs,
                log_prefix="[Tier2]",
            )
            print(f"[Tier2] {len(tier2_accounts)} accounts loaded")
            combined.extend(tier2_accounts)
        except WisdomMCPError as e:
            print(f"[Tier2] Wisdom MCP error — {e}")
            raise
    else:
        print("[Tier2] Skipping — WISDOM_AUTH_TOKEN not set")

    # ---- Tier 3: stub ------------------------------------------------------
    tier3_accounts = await collect_tier3(ai_client, context)
    combined.extend(tier3_accounts)

    # ---- Deterministic dedupe ---------------------------------------------
    deduped = deduplicate(combined)
    print(f"[Merge] {len(combined)} raw → {len(deduped)} after dedupe")

    # ---- Prioritizer LLM (optional) or heuristic merge_and_score ----------
    if prioritizer_llm_requested():
        final_list = await prioritize_with_ai_config(ai_client, context, deduped)
    else:
        print(
            "[Prioritizer] E100_PRIORITIZER_MODE=deterministic — "
            "skipping LLM; using heuristic merge_and_score (core/scorer.py)"
        )
        final_list = None
    if final_list is None:
        final_list = merge_and_score(deduped)

    # ---- Outputs ---------------------------------------------------------
    _print_results(final_list)

    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if sheet_id and sheet_id != "...":
        from outputs.sheets_writer import write_to_sheets
        write_to_sheets(final_list, sheet_id)
        print(f"[Sheets] Written to Google Sheet {sheet_id}")
    else:
        print("[Sheets] Skipping — GOOGLE_SHEET_ID not set")

    slack_url = os.getenv("SLACK_WEBHOOK_URL")
    if slack_url:
        from outputs.slack_notifier import send_digest
        send_digest(final_list, slack_url)
        print("[Slack] Digest sent")
    else:
        print("[Slack] Skipping — SLACK_WEBHOOK_URL not set")

    print(f"\nE100 refresh complete — {len(final_list)} accounts ranked.")

    if ld_client:
        ld_client.flush()
        ld_client.close()


if __name__ == "__main__":
    asyncio.run(run_e100_refresh())

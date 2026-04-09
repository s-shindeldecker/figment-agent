import asyncio
import os
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

from agents.tier1_looker import Tier1LookerAgent
from agents.tier2_enterpret import WisdomMCPError, execute_wisdom_prompt_jobs
from agents.tier3_web import collect as collect_tier3_web
from agents.tier3_zoominfo import Tier3ZoomInfoAgent, COMPETITOR_TECH_PATH, ANALYTIC_CMS_PATH
from agents.wisdom_prompts import resolve_wisdom_prompt_jobs
from core.merger import merge_and_score


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
    # ---- Tier 1: Looker ---------------------------------------------------
    tier1_agent = Tier1LookerAgent(None, "e100-tier1-looker", None, graph=None)
    tier1_accounts = await tier1_agent.run()
    print(f"[Tier1] {len(tier1_accounts)} accounts loaded")

    combined: list = list(tier1_accounts)

    # ---- Tier 2: Wisdom (settings.yaml prompts + wisdom_cypher.yaml) -------
    wisdom_token = (os.getenv("WISDOM_AUTH_TOKEN") or "").strip()
    if wisdom_token:
        try:
            prompt_jobs, _src = resolve_wisdom_prompt_jobs()
            tier2_accounts = await execute_wisdom_prompt_jobs(
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

    # ---- Tier 3a: ZoomInfo exports (competitor tech + analytics/CMS) -------
    zi_files_present = os.path.exists(COMPETITOR_TECH_PATH) or os.path.exists(ANALYTIC_CMS_PATH)
    if zi_files_present:
        zi_agent = Tier3ZoomInfoAgent(None, "e100-tier3-zoominfo", None)
        tier3_zi_accounts = await zi_agent.run()
        print(f"[Tier3/ZoomInfo] {len(tier3_zi_accounts)} accounts loaded")
        combined.extend(tier3_zi_accounts)
    else:
        print("[Tier3/ZoomInfo] Skipping — no export files found in data/")

    # ---- Tier 3b: allowlisted web (TIER3_WEB_ENABLED=1, config/tier3_sources.yaml)
    tier3_web_accounts = await collect_tier3_web()
    combined.extend(tier3_web_accounts)

    # ---- Merge by account + rank -----------------------------------------
    print("[Prioritizer] Deterministic merge_and_score (core/scorer.py)")
    final_list = merge_and_score(combined)
    print(f"[Merge] {len(combined)} raw → {len(final_list)} after merge")

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


if __name__ == "__main__":
    asyncio.run(run_e100_refresh())

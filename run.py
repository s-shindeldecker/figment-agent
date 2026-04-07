import asyncio
import os
from datetime import datetime

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
    # ---- LD setup (optional for local testing) --------------------------
    ld_sdk_key = os.getenv("LD_SDK_KEY")
    ai_client = None
    ld_client = None

    if ld_sdk_key:
        from agents.base import init_ld_clients
        ld_client, ai_client = init_ld_clients(ld_sdk_key)
        print("[LD] Connected to LaunchDarkly")
    else:
        print("[LD] LD_SDK_KEY not set — skipping LaunchDarkly, running in local mode")

    # LD context — identifies this run for targeting/observability
    context = {
        "kind": "user",
        "key": "e100-agent",
        "name": "E100 Weekly Refresh",
        "run_date": datetime.now().isoformat(),
    }

    # ---- Tier 1: Looker --------------------------------------------------
    from agents.tier1_looker import Tier1LookerAgent
    tier1_agent = Tier1LookerAgent(ai_client, "e100-tier1-looker", context)
    tier1_accounts = await tier1_agent.run()
    print(f"[Tier1] {len(tier1_accounts)} accounts loaded")

    all_accounts = list(tier1_accounts)

    # ---- Tier 2: Enterpret (skip if not configured) ----------------------
    enterpret_url = os.getenv("WISDOM_SERVER_URL")
    if enterpret_url:
        from agents.tier2_enterpret import Tier2EntrepretAgent
        tier2_agent = Tier2EntrepretAgent(ai_client, "e100-tier2-enterpret", context)
        try:
            tier2_accounts = await tier2_agent.run()
            print(f"[Tier2] {len(tier2_accounts)} accounts loaded")
            all_accounts.extend(tier2_accounts)
        except NotImplementedError:
            print("[Tier2] Skipping — Wisdom MCP client not yet implemented")
    else:
        print("[Tier2] Skipping — WISDOM_SERVER_URL not set")

    # ---- Score, deduplicate, rank ----------------------------------------
    final_list = merge_and_score(all_accounts)

    # ---- Outputs ---------------------------------------------------------
    _print_results(final_list)

    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if sheet_id:
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
        ld_client.close()


if __name__ == "__main__":
    asyncio.run(run_e100_refresh())

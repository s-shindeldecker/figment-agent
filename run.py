import asyncio
import os
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

from agents.prioritizer import (
    PRIORITIZER_AI_CONFIG_KEY,
    prioritizer_llm_requested,
    prioritize_with_ai_config,
)
from agents.tier1_looker import Tier1LookerAgent
from agents.tier2_enterpret import WisdomMCPError, execute_wisdom_tier2_jobs
from agents.tier3_web import collect as collect_tier3_web
from agents.tier3_zoominfo import Tier3ZoomInfoAgent, COMPETITOR_TECH_PATH, ANALYTIC_CMS_PATH
from agents.wisdom_prompts import tier2_job_keys
from core.deduplicator import merge_accounts
from core.merger import (
    clone_accounts_for_sheet_export,
    extract_save_accounts,
    merge_and_score,
    merge_only,
    resolve_e100_summary_list,
    score_and_rank_for_export,
)


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

    # ---- Tier 2: Wisdom (Cypher only — wisdom_cypher.yaml + per-key LD flags) ---
    tier2_accounts: list = []
    wisdom_token = (os.getenv("WISDOM_AUTH_TOKEN") or "").strip()
    if wisdom_token:
        try:
            tier2_accounts = await execute_wisdom_tier2_jobs(
                tier2_job_keys(),
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
    tier3_zi_accounts: list = []
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

    # ---- Per-tier Sheets export (clones; does not mutate combined rows) ----
    tier3_for_sheet = merge_accounts(tier3_zi_accounts + tier3_web_accounts)
    tier1_sheet_rows = score_and_rank_for_export(
        clone_accounts_for_sheet_export(tier1_accounts)
    )
    tier2_sheet_rows = score_and_rank_for_export(
        clone_accounts_for_sheet_export(tier2_accounts)
    )
    tier3_sheet_rows = score_and_rank_for_export(
        clone_accounts_for_sheet_export(tier3_for_sheet)
    )

    # ---- Merge by account + rank (console, Slack, optional merged tab) ----
    # For the LLM path: merge only, no pre-scoring. Clean records let the model
    # reason from raw signals rather than anchoring on deterministic scores.
    # For the deterministic fallback path: merge_and_score runs as before.
    deduped = merge_only(combined)
    print(f"[Merge] {len(combined)} raw → {len(deduped)} accounts after merge-by-name")

    ranking_source = "deterministic (merge_and_score / core/scorer.py)"
    final_list = None

    if prioritizer_llm_requested():
        final_list = await prioritize_with_ai_config(deduped)
        if final_list is not None:
            ranking_source = (
                f"llm (LaunchDarkly agent AI Config {PRIORITIZER_AI_CONFIG_KEY!r} + Anthropic)"
            )
        else:
            ranking_source = "deterministic (fallback after LLM path skipped or failed)"
    else:
        print(
            "[Prioritizer] E100_PRIORITIZER_MODE=deterministic — skipping LLM; "
            "using merge_and_score only"
        )

    if final_list is None:
        # Deterministic fallback: run full merge_and_score from combined (includes scoring)
        final_list = merge_and_score(combined)

    print(f"[Prioritizer] Ranking source: {ranking_source}")

    summary_list = resolve_e100_summary_list(final_list)

    save_list = extract_save_accounts(final_list)
    if save_list:
        print(
            f"[Save] {len(save_list)} T1+T2 cross-tier accounts identified "
            f"(existing customers with competitive intelligence)"
        )
    else:
        print("[Save] No T1+T2 cross-tier accounts found in this run")

    if len(summary_list) != len(final_list):
        print(
            f"[Summary] Quota list {len(summary_list)} accounts "
            f"(tiers + backfill; full merge has {len(final_list)})"
        )

    # ---- Outputs ---------------------------------------------------------
    _print_results(summary_list)

    if save_list:
        print(
            f"\n[Save] {len(save_list)} accounts in Save tab "
            f"(T1+T2 cross-tier — existing customers with competitive signal):"
        )
        for a in save_list[:10]:
            competitor = f" | competitor: {a.competitor}" if a.competitor else ""
            urgency = f" | {a.urgency}" if a.urgency else ""
            arr = f"${a.arr:,.0f}" if a.arr else "N/A"
            print(
                f"  {a.priority_rank or '?':>3}. {(a.account_name or '')[:45]:<45} "
                f"{arr:<14}{urgency}{competitor}"
            )

    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if sheet_id and sheet_id != "...":
        from outputs.sheets_writer import write_merged_master_enabled, write_to_sheets_by_tier

        write_to_sheets_by_tier(
            tier1_sheet_rows,
            tier2_sheet_rows,
            tier3_sheet_rows,
            merged_accounts=summary_list,
            save_accounts=save_list,
            sheet_id=sheet_id,
        )
        extra = " + merged master tab" if write_merged_master_enabled() else ""
        print(f"[Sheets] Written per-tier tabs (E100 Tier 1–3){extra} → {sheet_id}")
    else:
        print("[Sheets] Skipping — GOOGLE_SHEET_ID not set")

    slack_url = os.getenv("SLACK_WEBHOOK_URL")
    if slack_url:
        from outputs.slack_notifier import send_digest
        send_digest(summary_list, slack_url)
        print("[Slack] Digest sent")
    else:
        print("[Slack] Skipping — SLACK_WEBHOOK_URL not set")

    print(
        f"\nE100 refresh complete — {len(summary_list)} accounts in summary output "
        f"({len(final_list)} after merge).\n"
        f"  Ranking: {ranking_source}"
    )


if __name__ == "__main__":
    asyncio.run(run_e100_refresh())

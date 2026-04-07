import asyncio
import os
from datetime import datetime

from agents.base import init_ld_clients
from agents.tier1_looker import Tier1LookerAgent
from agents.tier2_enterpret import Tier2EntrepretAgent
from core.merger import merge_and_score
from outputs.sheets_writer import write_to_sheets
from outputs.slack_notifier import send_digest


async def run_e100_refresh():
    ld_client, ai_client = init_ld_clients(os.getenv("LD_SDK_KEY"))

    # LD context — identifies this run for targeting/observability
    context = {
        "kind": "user",
        "key": "e100-agent",
        "name": "E100 Weekly Refresh",
        "run_date": datetime.now().isoformat(),
    }

    # Run collection agents in parallel
    tier1_agent = Tier1LookerAgent(ai_client, "e100-tier1-looker", context)
    tier2_agent = Tier2EntrepretAgent(ai_client, "e100-tier2-enterpret", context)

    tier1_accounts, tier2_accounts = await asyncio.gather(
        tier1_agent.run(),
        tier2_agent.run(),
    )

    all_accounts = list(tier1_accounts) + list(tier2_accounts)

    # Score, deduplicate, and rank
    final_list = merge_and_score(all_accounts)

    # Output
    write_to_sheets(final_list)
    send_digest(final_list)

    print(f"E100 refresh complete — {len(final_list)} accounts written.")
    ld_client.close()


if __name__ == "__main__":
    asyncio.run(run_e100_refresh())

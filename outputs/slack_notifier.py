import os
import json
import urllib.request
from core.schema import AccountRecord


def send_digest(accounts: list[AccountRecord], webhook_url: str = None):
    """
    Send weekly E100 digest to Slack.
    Posts a summary of top accounts by tier.
    """
    if webhook_url is None:
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        raise ValueError("SLACK_WEBHOOK_URL not set")

    top_accounts = accounts[:10]  # Top 10 for digest

    lines = ["*E100 Weekly Refresh — Top Accounts*\n"]
    for acct in top_accounts:
        tier_label = f"T{acct.tier}" if acct.tier else "?"
        urgency = f" | _{acct.urgency}_" if acct.urgency else ""
        competitor = f" | competitor: {acct.competitor}" if acct.competitor else ""
        arr = f"${acct.arr:,.0f}" if acct.arr else "N/A"
        lines.append(
            f"• *{acct.account_name}* [{tier_label}] — {arr} ARR{urgency}{competitor}"
        )

    lines.append(f"\n_{len(accounts)} total accounts in this run._")

    payload = {"text": "\n".join(lines)}
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        webhook_url, data=data, headers={"Content-Type": "application/json"}
    )
    urllib.request.urlopen(req)

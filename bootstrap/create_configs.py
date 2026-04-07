"""
Bootstrap script — creates AI Configs in LaunchDarkly via API.
Run once to set up configs, or re-run to update instructions.

Usage:
    python bootstrap/create_configs.py
"""
import os
import json
import urllib.request
from dotenv import load_dotenv
load_dotenv()
import urllib.error


LD_API_KEY = os.getenv("LD_API_KEY")
LD_PROJECT_KEY = os.getenv("LD_PROJECT_KEY", "default")
LD_ENV_KEY = os.getenv("LD_ENV_KEY", "production")

AI_CONFIGS = [
    {
        "key": "e100-orchestrator",
        "name": "E100 Orchestrator",
        "description": "Root agent — coordinates run, routes to collection agents",
        "model": "claude-sonnet-4-20250514",
        "instructions": (
            "You are the E100 orchestration agent. "
            "Your job is to coordinate the weekly E100 list refresh. "
            "Route to tier1-looker and tier2-enterpret agents for data collection, "
            "then to scorer-merger for final list assembly."
        ),
    },
    {
        "key": "e100-tier1-looker",
        "name": "E100 Tier 1 — Looker",
        "description": "Looker data collection and normalization",
        "model": "claude-sonnet-4-20250514",
        "instructions": (
            "You are the Tier 1 data collection agent. "
            "Query Looker for enterprise accounts not using Experimentation in the last 90 days. "
            "Normalize results to the AccountRecord schema. "
            "Flag any ambiguous or incomplete records in the notes field."
        ),
    },
    {
        "key": "e100-tier2-enterpret",
        "name": "E100 Tier 2 — Enterpret",
        "description": "Enterpret competitive intelligence queries and normalization",
        "model": "claude-sonnet-4-20250514",
        "instructions": (
            "You are the Tier 2 competitive intelligence agent. "
            "Query Enterpret for accounts mentioning competitors (Statsig, Optimizely, Eppo). "
            "Normalize urgency as: immediate (active switching intent), active (evaluating), "
            "watch (mentioned but no action). Normalize results to the AccountRecord schema."
        ),
    },
    {
        "key": "e100-scorer-merger",
        "name": "E100 Scorer and Merger",
        "description": "Scoring, deduplication, and final list assembly",
        "model": "claude-sonnet-4-20250514",
        "instructions": (
            "You are the scorer and merger agent. "
            "Deduplicate accounts across tiers, apply scoring weights, and assemble the final ranked list. "
            "Flag accounts that appear in both Tier 1 and Tier 2 as dual-motion opportunities."
        ),
    },
]


def create_ai_config(config: dict):
    url = f"https://app.launchdarkly.com/api/v2/projects/{LD_PROJECT_KEY}/ai-configs"
    payload = json.dumps({
        "key": config["key"],
        "name": config["name"],
        "description": config.get("description", ""),
        "tags": ["e100", "agent"],
        "defaultVariation": {
            "key": "default",
            "name": "Default",
            "messages": [
                {"role": "system", "content": config["instructions"]}
            ],
            "model": {"modelName": config["model"]},
        },
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization": LD_API_KEY,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            print(f"Created: {config['key']} ({resp.status})")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        if e.code == 409:
            print(f"Already exists (skipping): {config['key']}")
        else:
            print(f"Error creating {config['key']}: {e.code} — {body}")


if __name__ == "__main__":
    if not LD_API_KEY:
        raise SystemExit("LD_API_KEY environment variable not set")

    for config in AI_CONFIGS:
        create_ai_config(config)

    print("\nDone. Next step: build the Agent Graph in the LaunchDarkly UI.")
    print("AI > Agent Graphs > New Graph > e100-weekly-refresh")

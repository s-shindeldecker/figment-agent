"""
Bootstrap script — creates AI Configs in LaunchDarkly via API.
Run once to set up configs, or re-run to delete and recreate them.

Usage:
    python bootstrap/create_configs.py
"""
import os
import json
import urllib.request
import urllib.error
from dotenv import load_dotenv
load_dotenv()


LD_API_KEY = os.getenv("LD_API_KEY")
LD_PROJECT_KEY = os.getenv("LD_PROJECT_KEY", "default")

AGENT_GRAPH_NAME = "figment-e-100-weekly-refresh"

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


def ld_request(method: str, path: str, payload: dict = None) -> tuple[int, dict]:
    """Make an authenticated request to the LD API. Returns (status, body)."""
    url = f"https://app.launchdarkly.com{path}"
    data = json.dumps(payload).encode("utf-8") if payload else None
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": LD_API_KEY,
            "Content-Type": "application/json",
        },
        method=method,
    )
    try:
        with urllib.request.urlopen(req) as resp:
            body = json.loads(resp.read().decode()) if resp.length != 0 else {}
            return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        try:
            body = json.loads(body)
        except Exception:
            pass
        return e.code, body


def delete_ai_config(key: str):
    status, body = ld_request("DELETE", f"/api/v2/projects/{LD_PROJECT_KEY}/ai-configs/{key}")
    if status == 204:
        print(f"  Deleted: {key}")
    elif status == 404:
        print(f"  Not found (skipping delete): {key}")
    else:
        print(f"  Error deleting {key}: {status} — {body}")


def create_ai_config(config: dict):
    status, body = ld_request("POST", f"/api/v2/projects/{LD_PROJECT_KEY}/ai-configs", {
        "key": config["key"],
        "name": config["name"],
        "mode": "agent",
        "tags": ["e100", "agent"],
        "defaultVariation": {
            "key": "default",
            "name": "Default",
            "instructions": config["instructions"],
            "model": {"modelName": config["model"]},
        },
    })
    if status == 201:
        print(f"  Created: {config['key']}")
    else:
        print(f"  Error creating {config['key']}: {status} — {body}")


if __name__ == "__main__":
    if not LD_API_KEY:
        raise SystemExit("LD_API_KEY environment variable not set")

    print("Step 1: Deleting existing AI Configs...")
    for config in AI_CONFIGS:
        delete_ai_config(config["key"])

    print("\nStep 2: Recreating AI Configs in Agent mode...")
    for config in AI_CONFIGS:
        create_ai_config(config)

    print(f"\nDone. Next step: build the Agent Graph in the LaunchDarkly UI.")
    print(f"AI > Agent Graphs > '{AGENT_GRAPH_NAME}'")
    print(f"\nNodes to add (in order):")
    for config in AI_CONFIGS:
        root = " ← set as Root node" if config["key"] == "e100-orchestrator" else ""
        print(f"  - {config['key']}{root}")

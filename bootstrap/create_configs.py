"""
Bootstrap script — creates the E100 prioritizer AI Config in LaunchDarkly via API.

Wisdom prompts are **string feature flags** (create those separately in the LD UI or Flags API).

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

# String flag keys for Wisdom MCP prompt bodies (create as string variations in LaunchDarkly).
WISDOM_STRING_FLAG_KEYS = (
    "e100-wisdom-prompt-competitive-displacement",
    "e100-wisdom-prompt-switching-intent",
    "e100-wisdom-prompt-eppo-coverage",
)

AI_CONFIGS = [
    {
        "key": "e100-prioritizer",
        "name": "E100 List prioritizer",
        "description": (
            "Single LLM step after Looker + Wisdom + Tier3 merge: interpret signals and assign ranks."
        ),
        "model": "claude-sonnet-4-20250514",
        "instructions": (
            "You are a GTM prioritization assistant for an enterprise SaaS expansion list.\n"
            "You receive JSON account records (Looker usage, competitive intel, notes).\n"
            "Assess commercial potential, urgency, and fit; assign expansion_score (higher = more priority).\n"
            "The user message will ask for a JSON array only. Each element must include:\n"
            "  account_name (exact match to input),\n"
            "  priority_rank (integer, 1 = top priority),\n"
            "  expansion_score (number),\n"
            "  notes (optional string with rationale).\n"
            "Include every input account. Output JSON array only, no markdown fences."
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


def update_ai_config_variation(config: dict):
    """PATCH the default variation to set Anthropic provider and update instructions."""
    status, body = ld_request(
        "PATCH",
        f"/api/v2/projects/{LD_PROJECT_KEY}/ai-configs/{config['key']}/variations/default",
        {
            "instructions": config["instructions"],
            "model": {
                "modelName": config["model"],
                "provider": "anthropic",
            },
        },
    )
    if status == 200:
        print(f"  Updated: {config['key']}")
    else:
        print(f"  Error updating {config['key']}: {status} — {body}")


def config_exists(key: str) -> bool:
    status, _ = ld_request("GET", f"/api/v2/projects/{LD_PROJECT_KEY}/ai-configs/{key}")
    return status == 200


def create_ai_config(config: dict):
    if config_exists(config["key"]):
        print(f"  Already exists, updating: {config['key']}")
        update_ai_config_variation(config)
        return

    status, body = ld_request("POST", f"/api/v2/projects/{LD_PROJECT_KEY}/ai-configs", {
        "key": config["key"],
        "name": config["name"],
        "mode": "agent",
        "tags": ["e100", "agent"],
        "defaultVariation": {
            "key": "default",
            "name": "Default",
            "instructions": config["instructions"],
            "model": {
                "modelName": config["model"],
                "provider": "anthropic",
            },
        },
    })
    if status == 201:
        print(f"  Created: {config['key']}")
    else:
        print(f"  Error creating {config['key']}: {status} — {body}")


if __name__ == "__main__":
    if not LD_API_KEY:
        raise SystemExit("LD_API_KEY environment variable not set")

    print("Upserting E100 prioritizer AI Config...")
    for config in AI_CONFIGS:
        create_ai_config(config)

    print("\nDone.")
    print("\nNext: create the three Wisdom **string** multivariate flags (or run):")
    print("  python bootstrap/create_wisdom_string_flags.py")
    print("Flag keys (two variations each: empty + production prompt):")
    for key in WISDOM_STRING_FLAG_KEYS:
        print(f"  - {key}")
    print("\nOptional Cypher overrides (Enterpret), names in agents.wisdom_prompts:")
    print("  WISDOM_CYPHER_COMPETITIVE_DISPLACEMENT, WISDOM_CYPHER_SWITCHING_INTENT,")
    print("  WISDOM_CYPHER_EPPO_COVERAGE, or WISDOM_CYPHER for all.")
    print(f"\nPrioritizer AI Config key: {AI_CONFIGS[0]['key']} (override with E100_PRIORITIZER_AI_CONFIG_KEY).")
    print("Disable that AI Config in LD to use deterministic merge_and_score instead of the LLM.")

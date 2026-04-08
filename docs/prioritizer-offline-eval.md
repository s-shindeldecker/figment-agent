# Prioritizer model contract (LaunchDarkly offline evaluations)

Use this when testing the **E100 prioritizer** AI Config in [LaunchDarkly offline evaluations](https://launchdarkly.com/docs/home/ai-configs/offline-evaluations) (Playground).

## Roles

| Role | Source |
|------|--------|
| **System** | Your AI Config **instructions** (the agent prompt you pasted in LaunchDarkly). |
| **User** | Built by the app: fixed intro + indented JSON array of accounts + fixed output instructions. |

The application **does not** put your long agent prompt in the user message; it uses the AI Config `instructions` field as **system** and the payload below as **user** (see `agents/prioritizer.py`).

## User message shape (dataset `input`)

Exact assembly:

1. **`PRIORITIZER_USER_INTRO`** (literal):

   ```text
   Merged account records (JSON). Assess and rank for GTM priority.

   ```

2. **`accounts_json`** — `json.dumps(..., indent=2)` of an array of objects. Each object is `AccountRecord` with **only non-null fields** included (`dataclasses.asdict` then drop `None`). In production this usually includes **`last_updated`** (ISO timestamp); the generator’s `--omit-last-updated` flag drops it for cleaner eval fixtures.

3. **`_USER_PAYLOAD_SUFFIX`** (literal):

   ```text

   Respond with ONLY a JSON array (no markdown), same length as input accounts, one object per account you are ranking, ordered by final priority (best first). Each object must include: "account_name" (string), "priority_rank" (integer starting at 1), "expansion_score" (number), optional "notes" (string). Include every account from the input; use exact account_name values.
   ```

The canonical builder in code is **`build_prioritizer_user_message(accounts)`** in `agents/prioritizer.py`.

## Account object fields (`AccountRecord`)

Any subset may appear (only non-null keys are serialized). Common fields:

- **Required for parsing:** `account_name` (string)
- **Tier / source:** `tier` (1–3), `source` (`looker` | `enterpret` | `external`)
- **Commercial:** `arr`, `plan`, `rating`, `geo`, `industry`, `renewal_date`, `sfdc_account_id`, `ld_account_id`
- **Ownership:** `ae`, `csm`
- **Tier 1 signals:** `exp_events_mtd`, `exp_events_entitled`, `exp_utilisation_rate`, `is_using_exp_90d`, `days_since_last_iteration`, `active_experiments`
- **Tier 2 signals:** `competitor`, `competitor_spend`, `renewal_window_months`, `urgency`, `deal_context`
- **Overrides / meta:** `override_action`, `override_reason`, `override_by`, `notes`, `last_updated`, `expansion_score`, `priority_rank`

Full definition: `core/schema.py`.

## Expected model output

The app parses **one JSON array** of objects from the model text (including Markdown-fenced JSON code blocks). Each object should have:

- `account_name` (or `accountName`)
- `priority_rank` (integer)
- `expansion_score` (number)
- optional `notes`

See `apply_prioritizer_response` in `agents/prioritizer.py`.

## LaunchDarkly dataset

- **JSONL (recommended):** one object per line. Minimal row:

  ```json
  {"input": "<full user message string from build_prioritizer_user_message>"}
  ```

  Optional: `expected_output` (string or JSON) for judges; `metadata` for your own labels.

- **CSV:** same idea: an `input` column whose cell is the **entire** user message. Multiline cells are awkward in CSV; prefer JSONL.

## Generate rows from this repo

1. Create a JSON file containing **one array** of account objects (see `docs/examples/*.json`).
2. Run:

   ```bash
   python bootstrap/generate_prioritizer_eval_dataset.py \
     docs/examples/prioritizer_eval_scenario_small.json \
     docs/examples/prioritizer_eval_scenario_tier1_only.json \
     > my_dataset.jsonl
   ```

   Use `--omit-last-updated` so account JSON matches what you’d upload without runtime timestamps (the checked-in `prioritizer_eval.sample.jsonl` was built with this flag).

3. Upload `my_dataset.jsonl` (or the checked-in `docs/examples/prioritizer_eval.sample.jsonl`) to LaunchDarkly.

The generator copies the **exact** intro, JSON formatting, and suffix from `agents/prioritizer.py` so datasets stay aligned with production.

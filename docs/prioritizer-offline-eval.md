# Prioritizer offline eval

**Runtime (`run.py`):** With `E100_PRIORITIZER_MODE=llm` (default), the pipeline evaluates the **agent-mode** LaunchDarkly AI Config (`E100_PRIORITIZER_AI_CONFIG_KEY`, default `e100-prioritizer`), calls the model (ldai provider if available, else Anthropic via httpx + `ANTHROPIC_API_KEY`), parses JSON, and applies `agents/prioritizer.apply_prioritizer_response`. On failure or disabled config, it falls back to deterministic `merge_and_score` / `core/scorer.py`. Set `E100_PRIORITIZER_MODE=deterministic` to skip the LLM entirely.

**Completion-mode smoke test:** `scripts/ld_prioritizer_smoke.py` + `LD_PRIORITIZER_AI_CONFIG_KEY` and template `{{accounts_json}}`.

JSONL examples under `docs/examples/` were for historical LD playground testing; `build_prioritizer_user_message()` matches the user payload used for offline dataset `input` when the AI Config carries system/agent instructions.

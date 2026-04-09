# Prioritizer offline eval (deprecated)

The LaunchDarkly AI Config + LLM prioritizer path has been removed. The pipeline ranks accounts with **deterministic** `merge_and_score` / `core/scorer.py` using weights from `config/settings.yaml`.

The JSONL examples under `docs/examples/` were for historical LD playground testing and are no longer wired into `run.py`.

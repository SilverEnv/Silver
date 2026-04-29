# wire-backtest-metadata-registry

Objective:
Wire the durable backtest metadata registry so every accepted backtest result is
reproducible from its run identity.

User Value:
Michael can review a backtest claim and see the exact code, feature set,
training window, random seed, execution assumptions, and point-in-time policy
versions needed to reproduce or reject it.

Why Now:
Silver's product contract already makes reproducibility metadata a day-one law,
and Phase 2 starts the durable `model_runs` and `backtest_runs` registry. This
Objective coordinates the schema, runner, report, and validation work before it
is split into implementation tickets.

Done When:
- Backtest metadata contracts are durable in schema and docs.
- Model run records capture code SHA, feature set hash, training window,
  random seed, execution assumptions, and available-at policy version set.
- Backtest run records reference model runs and capture universe, horizon,
  costs, baselines, metrics, and reproducibility metadata.
- Falsifier or walk-forward reports expose the metadata required to reproduce
  the run.
- Validation proves a run can be traced from a reported result back to the
  frozen inputs that produced it.

Out Of Scope:
- No new text features.
- No portfolio or paper-trading execution.
- No vendor fetch expansion.
- No Linear, GitHub, migration, or steward automation.
- No Arrow code, schema imports, or analyst-facing views.

Guardrails:
- No feature value may be used without an `available_at` rule.
- No prediction may be written without frozen feature, model, and prompt
  versions.
- No backtest result may be reported without costs, baselines, and
  reproducibility metadata.
- Keep model, prompt, feature, and execution-assumption versions immutable once
  referenced by a run.
- Keep schema work isolated to one migration-owner ticket before dependent
  runner/report tickets proceed.
- Do not commit `.env`, API keys, vendor secrets, or local credentials.

Expected Tickets:
- Confirm the metadata registry contract across `SPEC.md`, architecture docs,
  and migration ownership.
- Add or complete the model/backtest run schema for durable metadata.
- Wire walk-forward or falsifier runners to create `model_runs` records.
- Wire backtest result writing to create `backtest_runs` records with costs,
  baselines, metrics, and metadata.
- Surface reproducibility metadata in falsifier reports.
- Add replay or traceability validation from a reported result to its
  `model_run_id`.

Validation:
- `git diff --check`
- `python -m pytest`
- `ruff check .`
- `python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed`
- Reproducibility evidence showing the same reported run can be traced to the
  same frozen metadata inputs.

Conflict Zones:
- `db/migrations/`
- `src/silver/backtest/`
- `src/silver/models/`
- `scripts/run_falsifier.py`
- `reports/falsifier/`
- `tests/`
- `SPEC.md`
- `docs/ARCHITECTURE.md`
- `docs/TESTING.md`

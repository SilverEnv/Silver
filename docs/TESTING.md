# Testing And Validation

Quiver values falsification over optimistic backtests. Tests should make false
confidence hard.

## Validation Ladder

Run the narrowest meaningful check while iterating, then broaden before handoff.

```bash
git diff --check
python -m pytest
ruff check .
python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 30 --universe falsifier_seed
```

Not all commands exist at repository bootstrap. If a command does not exist yet,
record that it was unavailable.

## Required Test Classes

- Unit tests for calendar math, `available_at` policy logic, costs, and labels
- Integration tests for raw ingest through normalized rows
- Backtest tests for walk-forward splits, label-scramble, costs, and baselines
- Reproducibility tests proving repeated runs produce identical outputs

## Phase 1 Gate

Phase 1 is complete only when a repeatable command reproduces 12-1 momentum on
the seed universe with realistic costs and emits a report.

Target command:

```bash
python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 30 --universe falsifier_seed
```

The integration test should assert positive net Sharpe for the tiny seed
universe and verify the run carries reproducibility metadata.

## Reporting

Backtest reports must include gross and net metrics, baseline comparison,
regime breakdown, label-scramble result, and the exact model/run metadata used
to reproduce the output.

# Silver / Quiver

Silver is the repository for Quiver: a point-in-time prediction and backtesting
system for testing whether AI-extracted text features improve US equity forward
return prediction over numeric baselines, net of costs.

The product contract is [`SPEC.md`](SPEC.md). The agent harness is intentionally
small: [`AGENTS.md`](AGENTS.md) is the map, and `docs/` is the system of record.

## Current State

- Git remote: `https://github.com/peakyragnar/Silver.git`
- Build status: harness scaffold only
- Primary milestone: Phase 1 foundation from `SPEC.md`

## Agent Entry Points

- [`AGENTS.md`](AGENTS.md): rules every coding agent should read first
- [`docs/index.md`](docs/index.md): documentation map
- [`docs/exec-plans/active/phase-1-foundation.md`](docs/exec-plans/active/phase-1-foundation.md):
  first implementation plan
- [`WORKFLOW.md`](WORKFLOW.md): Symphony workflow configuration and prompt

## Local Secrets

Use `.env` for local secrets and `.env.example` for shared placeholders. `.env`
is ignored by Git.

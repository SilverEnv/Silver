# autonomous-post-todo-operation

Objective:
Reset Symphony operation so `Todo` means approved for autonomous build, repair,
merge, and completion within the ticket's stated scope.

User Value:
Michael approves direction and safety exceptions instead of manually moving
routine PRs through review and merge states.

Why Now:
ARR-36 and ARR-37 created an Objective store and taught the planning steward to
read active Objective files. The remaining bottleneck is a legacy post-build
approval stop that can leave merged issues active or require manual traffic
control after work was already approved into `Todo`.

Done When:
- `WORKFLOW.md` says safe completed tickets post proof packets and move to
  `Merging`, not a routine approval stop.
- `docs/Symphony-Operation.md` and `docs/SYMPHONY.md` describe the clean state
  machine: Objective approval and Safety Review are the only human gates.
- Proof packets are defined as audit receipts and steward inputs, not routine
  approval requests.
- Merge steward ownership remains `Merging` to `Done` or `Rework`.
- Safety Review is reserved for destructive, semantic, paid/live, security, or
  scope-drift exceptions.

Out Of Scope:
- No schema migration.
- No changes to Silver product, model, feature, label, or backtest behavior.
- No capacity steward implementation.
- No paid/live vendor calls.
- No destructive Linear or GitHub automation beyond the approved workflow-state
  rename.

Guardrails:
- Moving a ticket to `Todo` is approval to build and merge only within the
  ticket's stated scope.
- Do not silently resolve semantic conflicts.
- Do not delete data, change PIT semantics, alter secret handling, or expand
  paid/live service usage without Safety Review.
- Keep `Merging` non-active for Symphony workers; the merge steward owns it.
- Keep `Safety Review` non-active for Symphony workers.

Expected Tickets:
- Rewrite Symphony workflow docs for autonomous post-`Todo` operation.
- Rename the routine review lane to Safety Review in Linear and docs.
- Add stale merged-issue reconciliation to the merge steward.
- Add safety-gate checks for scope drift, destructive changes, and semantic
  conflicts before auto-merge.

Validation:
- `git diff --check`
- `python scripts/planning_steward.py --check`
- `python scripts/merge_steward.py --check`
- Documentation search shows no routine post-build human approval path remains.

Conflict Zones:
- `WORKFLOW.md`
- `docs/Symphony-Operation.md`
- `docs/SYMPHONY.md`
- `scripts/merge_steward.py`
- Linear workflow states

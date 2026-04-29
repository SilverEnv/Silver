# Silver Symphony Operation

This document defines how Silver uses Symphony to run agentic work. It is a
Silver operating policy, not a definition of Symphony core behavior.

For upstream Symphony concepts, see OpenAI's Symphony article:
[`An open-source spec for Codex orchestration: Symphony`](https://openai.com/index/open-source-codex-orchestration-symphony/).

For local setup commands, see [`SYMPHONY.md`](SYMPHONY.md).

## Operating Model

Symphony core watches Linear, creates isolated workspaces, starts Codex agents,
and keeps active issues moving. Silver's operation layer decides what work is
safe, useful, and ready.

Use this mental model:

```text
Goal
  -> Objective
      -> Work Packet
          -> Linear Tickets
              -> PRs
                  -> Human Review
                      -> Merging
                          -> Done or Rework
```

Michael should mostly review Objectives and proof packets. The system should
handle ticket decomposition, execution, and routine merge shepherding.

## Roles

| Role | Responsibility |
|---|---|
| Michael | Approves Objectives, reviews proof packets, moves approved work to `Merging`. |
| Symphony core | Runs agents for active Linear tickets. |
| Codex builder | Implements one scoped ticket in one workspace. |
| Planning steward | Proposes Objectives and creates guarded tickets. |
| Capacity steward | Keeps `Todo` filled within approved limits. |
| Migration allocator | Serializes and reserves schema migration work. |
| Merge steward | Queues approved green PRs and marks landed work `Done`. |
| Conflict steward | Repairs stale/conflicting PRs or routes semantic conflicts to `Rework`. |

Some steward roles may start as manual scripts or manual Codex sessions before
they become unattended automation.

## Current Lane

The current Silver loop is batch automation:

1. Michael and Codex choose the next useful batch.
2. Tickets are created in Linear.
3. Michael moves selected tickets to `Todo`.
4. Symphony assigns agents.
5. Agents implement, open PRs, and post proof packets.
6. Michael reviews and moves approved tickets to `Merging`.
7. Merge steward queues clean PRs and marks landed tickets `Done`.
8. Failed checks, conflicts, or review issues move to `Rework`.

This is useful, but it still depends on Michael asking for each batch. The next
unlock is continuous Objective-driven work generation.

## Target Lane

The target Silver loop is continuous but bounded:

1. Planning steward proposes the next Objectives from `SPEC.md`, active plans,
   Linear state, GitHub PR state, and repo status.
2. Michael approves one or more Objectives.
3. Planning steward decomposes approved Objectives into Linear tickets in
   `Backlog`.
4. Capacity steward promotes safe, unblocked tickets to `Todo`.
5. Symphony builds tickets up to the configured concurrency limit.
6. Merge steward lands approved PRs.
7. Conflict steward repairs mechanical conflicts and routes semantic conflicts
   to `Rework`.
8. Planning steward keeps the queue full from approved Objectives.

The rule is: keep the system full of coherent Objectives, not just busy agents.

## Objective

An Objective is the user-facing unit of work. It should be large enough to be
meaningful and small enough to prove.

Objective files live in [`objectives/`](objectives/). Use
[`objectives/TEMPLATE.md`](objectives/TEMPLATE.md) for new Objectives, keep
approved or ready Objectives in [`objectives/active/`](objectives/active/), and
move completed handoffs to [`objectives/completed/`](objectives/completed/).

Good Objective:

```text
Objective:
Prepare the database with identifiers and normalized format for FMP ingestion.

User Value:
Silver can ingest vendor data against durable security identities instead of
fragile ticker-only assumptions.

Done When:
A clean database can store securities, vendor identifiers, raw FMP responses,
and normalized daily prices with point-in-time `available_at` metadata.
```

Poor Objective:

```text
Create five tickets.
```

The second form hides the larger user value and makes it hard for Michael to
know whether the system is moving in the right direction.

## Objective Template

Every Objective should use this template:

```text
Objective:
One clear user-facing outcome.

User Value:
Who benefits and how.

Why Now:
Why this is the next useful chunk.

Done When:
Concrete observable completion criteria.

Out Of Scope:
Work that must not be included.

Guardrails:
Project laws, data safety, permissions, and irreversible actions to avoid.

Expected Tickets:
Likely implementation slices.

Validation:
Commands, artifacts, or evidence required before approval.

Conflict Zones:
Files, tables, docs, or workflows likely to collide with parallel work.
```

## Example Objective

```text
Objective:
Prepare database identifiers and normalized format for FMP ingestion.

User Value:
Silver can ingest prices and future FMP artifacts against durable security
identities with point-in-time metadata.

Why Now:
Reliable identifiers are required before scaling ingestion or creating more
features from vendor data.

Done When:
- Identifier schema exists and is migrated.
- Seed reference config can populate the identifier rows.
- FMP price ingest can resolve securities through the durable identifiers.
- Offline checks prove the schema, seed config, and ingest wiring are valid.

Out Of Scope:
- No backtest changes.
- No text features.
- No paper trading.
- No Arrow schema imports.

Guardrails:
- No feature value may be used without an `available_at` rule.
- Do not commit `.env` or vendor secrets.
- Keep schema work in one migration-owner ticket.

Expected Tickets:
- Reserve and add identifier migration.
- Update seed reference config and seeding code.
- Add repository helper for identifier lookup.
- Update FMP ingest to use identifier lookup.
- Add checks/tests/docs for the new ingestion contract.

Validation:
- `git diff --check`
- `python scripts/apply_migrations.py --check`
- `python scripts/seed_reference_data.py --check`
- `python scripts/ingest_fmp_prices.py --check`
- `python -m pytest`
- `ruff check .`

Conflict Zones:
- `db/migrations/`
- `config/seed_reference_data.yaml`
- `scripts/seed_reference_data.py`
- `scripts/ingest_fmp_prices.py`
- `src/silver/reference/`
- `src/silver/ingest/`
```

## Ticket Shape

Tickets are implementation slices created from an approved Objective. A ticket
should be scoped enough that one Codex builder can finish it, validate it, and
produce a reviewable PR.

Each ticket should include:

```text
Purpose:
What this ticket makes true.

Parent Objective:
Link or title.

Objective Impact:
One or two user-facing sentences explaining how this work moves the parent
Objective forward.

Technical Summary:
The implementation mechanism, using precise technical language where useful.

Acceptance Criteria:
Concrete checklist.

Owns:
Files, modules, tables, or scripts the agent may edit.

Do Not Touch:
Files or layers intentionally outside scope.

Dependencies:
Tickets or migrations that must land first.

Conflict Zones:
Shared areas that need care.

Validation Required:
Commands and expected artifact paths.

Proof Packet Requirements:
What Michael needs in order to approve.
```

Tickets should not say "improve ingestion" without ownership and acceptance
criteria. That creates vague work and harder reviews.

Good Objective Impact:

```text
Objective Impact:
This standardizes SQL input formatting so FMP extraction rows can be normalized
consistently. It reduces ambiguity before price ingestion writes PIT rows.

Technical Summary:
Adds explicit SQL parameter formatting and validation for normalized FMP daily
price extraction inputs before repository upsert.
```

Poor Objective Impact:

```text
Objective Impact:
Refactor SQL stuff.
```

The goal is not to remove technical detail. The goal is to make every ticket
explain, in Michael-readable language, how a narrow change advances the larger
Objective.

## Linear State Machine

Silver uses Linear as the control plane. These states are policy, not just UI.

| State | Symphony active? | Meaning |
|---|---:|---|
| `Backlog` | No | Planned work. Do not start automatically. |
| `Todo` | Yes | Approved and ready for an agent. |
| `In Progress` | Yes | Agent is implementing. |
| `Rework` | Yes | Agent should repair review feedback, CI failure, or conflict. |
| `Human Review` | No | Waiting for Michael's review. |
| `Merging` | No | Approved by Michael; merge steward owns it. |
| `Done` | No | Landed and complete. |
| `Canceled` / `Duplicate` | No | Terminal non-work state. |

Agents should not mark implementation work `Done`. They stop at `Human Review`
with evidence. Michael moves approved work to `Merging`.

## Planning Steward

The planning steward is the Objective and ticket factory. Its job is to create
useful work, not to maximize ticket count.

Inputs:

```text
SPEC.md
docs/index.md
docs/exec-plans/active/*
docs/objectives/active/*
Linear issues
GitHub PRs
repo status
recent proof packets
```

Outputs:

```text
Recommended Objectives
Objective packets
Linear tickets in Backlog
dependency notes
conflict-zone notes
migration reservation requests
```

Initial safe mode:

```text
scripts/planning_steward.py --propose
```

The initial implementation is local and propose-only. It reads repository
signals and prints Objective packets; it does not write to Linear, GitHub, the
database, or vendors.

Validate local proposal wiring:

```text
scripts/planning_steward.py --check
```

Next mode:

```text
scripts/planning_steward.py --create-backlog --objective <objective-id>
```

Later mode:

```text
scripts/planning_steward.py --top-up-todo --max-active 5 --todo-buffer 8
```

Do not begin with unattended ticket creation. First prove that proposed
Objectives and tickets are useful.

## Capacity Steward

The capacity steward keeps enough safe tickets available for Symphony.

Policy:

```text
If active agents are below target
and Todo count is below buffer
and approved Objective has unblocked safe tickets
then promote tickets from Backlog to Todo.
```

Default operating target:

```text
max active agents: 5
Todo buffer: 5 to 10
```

Do not promote tickets that share a high-risk conflict zone unless sequencing is
explicit in the Objective.

## Migration Lane

Migration handling is deterministic at the coordination layer and agentic at the
schema-design layer.

Deterministic:

```text
who may create a migration
which migration number/name is reserved
which tickets are blocked by the migration
which checks must pass
how number conflicts are repaired
```

Agentic:

```text
what schema shape is correct
which constraints preserve point-in-time safety
how to migrate existing data safely
how repositories, tests, and docs should adapt
```

Default rule:

```text
Only one active Todo ticket may own `db/migrations/`.
```

When an Objective needs schema work:

1. Planning steward identifies schema work.
2. Migration allocator reserves the next migration number and name.
3. One schema-owner ticket is created.
4. Dependent tickets remain in `Backlog` or are explicitly blocked.
5. Schema-owner PR lands before dependent implementation tickets enter `Todo`,
   unless the Objective states a safe parallel contract.

Example reservation:

```text
Reserved migration:
005_fmp_identifiers.sql

Owner ticket:
ARR-41 Prepare FMP identifier schema

Blocked tickets:
ARR-42 Add identifier repository helper
ARR-43 Update FMP ingest normalization
ARR-44 Add identifier seed validation
```

Number-only migration conflicts may be repaired automatically by renumbering to
the next available migration and updating references. Semantic schema conflicts
must be routed to `Rework` with a summary.

Semantic conflict examples:

```text
two PRs define different meanings for the same table
one PR adds a column another PR removes or renames
constraints change PIT behavior
available_at rules conflict
data-retention behavior changes
```

## Conflict Steward

The conflict steward handles stale branches, failed merge queue attempts, and
merge conflicts.

Mechanical conflicts may be repaired automatically:

```text
documentation context conflicts
formatting-only conflicts
test import ordering
migration number-only conflicts
lockfile refresh caused by accepted dependency change
```

Semantic conflicts must go to `Rework`:

```text
schema meaning
point-in-time behavior
label availability
feature definition semantics
backtest metric definitions
data deletion or retention
security/secrets behavior
```

Conflict repair flow:

1. Detect conflict or failed merge queue attempt.
2. Identify PR, ticket, changed files, and conflict files.
3. Classify mechanical vs semantic.
4. For mechanical conflicts, update branch, repair, run validation, and refresh
   the proof packet.
5. For semantic conflicts, move the ticket to `Rework` with a clear summary.

Rework summary should include:

```text
Objective:
Ticket:
PR:
Conflict files:
Likely cause:
Why this is mechanical or semantic:
Allowed repair scope:
Validation required:
```

## Merge Steward

The merge steward owns `Merging`.

It should:

1. Read Linear issues in `Merging`.
2. Find the matching GitHub PR.
3. Confirm the PR still matches the approved scope.
4. Confirm required checks are passing.
5. Add the PR to the GitHub merge queue.
6. Mark the issue `Done` after merge.
7. Move failed checks or conflicts to `Rework`.

It should not:

```text
bypass the merge queue
rewrite history
approve its own implementation
change product scope
silently resolve semantic conflicts
```

Current command references live in [`SYMPHONY.md`](SYMPHONY.md).

## Proof Packets

Every implementation ticket must end with a Linear comment headed:

```text
## Proof Packet
```

Required contents:

```text
PR link
parent Objective
Objective Impact summary
changed files summary
acceptance criteria status
validation commands and outcomes
CI status or link
risks, assumptions, and known gaps
generated artifact path or link, when relevant
exact blocker, when incomplete
```

Michael should reject proof packets that only make prose claims.

The Objective Impact summary should answer:

```text
What part of the Objective did this ticket advance?
What can Silver do now, or do more safely, because this landed?
What remains for the Objective after this ticket?
```

Example:

```text
Objective Impact:
This ticket gives the FMP ingestion Objective a stable SQL input contract for
normalized daily-price extraction. The next ticket can use this contract to
persist normalized rows without redefining extraction shape.
```

Silver-specific proof usually includes:

```text
git diff --check
python scripts/bootstrap_database.py --check
python scripts/apply_migrations.py --check
python scripts/seed_available_at_policies.py --check
python scripts/seed_reference_data.py --check
python scripts/seed_trading_calendar.py --check
python scripts/check_falsifier_inputs.py --check
python -m pytest
ruff check .
```

Tickets should run the narrowest meaningful checks while iterating and broader
checks before handoff.

## Automation Ladder

Do not jump straight to overnight autonomy. Increase automation only after the
previous rung produces good evidence.

1. Manual Objective writing.
2. Planning steward proposes Objectives and tickets.
3. Planning steward creates tickets in `Backlog`.
4. Capacity steward promotes low-risk tickets to `Todo`.
5. Merge steward continuously handles approved PRs.
6. Conflict steward repairs mechanical conflicts.
7. Overnight mode keeps `Todo` topped up from approved Objectives.
8. Human review focuses on Objective outcomes and exception paths.

Human approval remains required for:

```text
new Objective approval
semantic schema conflicts
PIT rule changes
data deletion
secret handling changes
expanding live or paid external service usage
moving unproven automation to a higher rung
```

## Objective Store

When Objective flow becomes active, use a small file-based store:

```text
docs/objectives/active/
docs/objectives/completed/
```

Each Objective file should use the Objective template above. Linear tickets can
link back to the Objective file or parent issue.

Do not create a large planning database until the file-based flow is painful.

## First Build For This System

Recommended first Objective:

```text
Objective:
Build an Objective-driven planning steward that proposes guarded Linear tickets
from Silver's build plan.

User Value:
Michael can approve coherent Objectives instead of manually asking for batches
of five tickets.

Done When:
- A proposal command reads repo docs and current Linear/GitHub state.
- It outputs 1 to 3 candidate Objectives.
- It decomposes an approved Objective into draft tickets with ownership,
  dependencies, conflict zones, and validation.
- It runs in propose-only mode without writing to Linear by default.

Out Of Scope:
- No unattended overnight mode.
- No automatic Todo promotion.
- No migration writes.
- No automatic PR conflict repair.
```

This builds the control surface before increasing autonomy.

## User Checklist

When operating Silver through Symphony, Michael should ask:

1. Is the Objective clear enough to approve?
2. Does it say what is out of scope?
3. Are conflict zones visible?
4. Does each ticket explain how it advances the Objective?
5. Is there at most one active migration-owner ticket?
6. Are low-risk tickets the only ones promoted automatically?
7. Do proof packets contain commands, outcomes, and Objective impact?
8. Are semantic conflicts routed to `Rework` instead of patched silently?
9. Is the system producing useful progress, not just more tickets?

If the answer to any of these is no, pause automation at the current rung and
fix the operating policy before adding more agents.

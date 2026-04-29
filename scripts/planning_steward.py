#!/usr/bin/env python
"""Propose Objective-driven Silver work without writing to Linear."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


ROOT = Path(__file__).resolve().parents[1]
ACTIVE_PLAN_DIR = Path("docs/exec-plans/active")
OBJECTIVE_ACTIVE_DIR = Path("docs/objectives/active")
OBJECTIVE_COMPLETED_DIR = Path("docs/objectives/completed")

OutputFormat = Literal["markdown", "json"]


class PlanningStewardError(RuntimeError):
    """Raised when the planning steward cannot produce reliable output."""


@dataclass(frozen=True, slots=True)
class RepoSignal:
    name: str
    value: str


@dataclass(frozen=True, slots=True)
class TicketProposal:
    title: str
    purpose: str
    objective_impact: str
    technical_summary: str
    owns: tuple[str, ...]
    do_not_touch: tuple[str, ...]
    dependencies: tuple[str, ...]
    conflict_zones: tuple[str, ...]
    validation: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ObjectiveProposal:
    objective_id: str
    objective: str
    user_value: str
    why_now: str
    done_when: tuple[str, ...]
    out_of_scope: tuple[str, ...]
    guardrails: tuple[str, ...]
    expected_tickets: tuple[TicketProposal, ...]
    validation: tuple[str, ...]
    conflict_zones: tuple[str, ...]
    migration_lane: str
    evidence: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PlanningContext:
    root: Path
    active_plan_files: tuple[Path, ...]
    unchecked_plan_items: tuple[str, ...]
    migration_files: tuple[Path, ...]
    next_migration_number: int | None
    has_operation_doc: bool
    has_workflow_objective_impact: bool
    has_objective_store: bool
    has_planning_steward: bool
    has_backtest_metadata_migration: bool
    has_backtest_metadata_code_writes: bool
    git_branch: str
    git_dirty: bool


@dataclass(frozen=True, slots=True)
class PlanningProposal:
    status: str
    signals: tuple[RepoSignal, ...]
    objectives: tuple[ObjectiveProposal, ...]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--propose",
        action="store_true",
        help="print proposed Objectives and ticket packets; this is the default",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate local docs and proposal wiring without network writes",
    )
    parser.add_argument(
        "--format",
        choices=("markdown", "json"),
        default="markdown",
        help="output format for proposals",
    )
    parser.add_argument(
        "--max-objectives",
        type=int,
        default=3,
        help="maximum number of Objectives to print",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT,
        help="repository root; intended for tests",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        proposal = propose_plan(
            root=args.root,
            max_objectives=args.max_objectives,
        )
        if args.check:
            _run_check(proposal)
            return 0
        print(render_proposal(proposal, output_format=args.format))
    except PlanningStewardError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


def propose_plan(
    *,
    root: Path = ROOT,
    max_objectives: int = 3,
) -> PlanningProposal:
    """Build a local, deterministic proposal from repository state."""
    if isinstance(max_objectives, bool) or max_objectives < 1:
        raise PlanningStewardError("max_objectives must be a positive integer")

    context = collect_context(root)
    objectives = build_objective_proposals(context)
    signals = build_signals(context)
    return PlanningProposal(
        status="PROPOSE_ONLY",
        signals=signals,
        objectives=objectives[:max_objectives],
    )


def collect_context(root: Path = ROOT) -> PlanningContext:
    """Read the minimum repository state needed for Objective proposals."""
    repo_root = root.resolve()
    if not repo_root.exists():
        raise PlanningStewardError(f"repository root does not exist: {repo_root}")

    active_plan_files = tuple(
        sorted(
            path.relative_to(repo_root)
            for path in (repo_root / ACTIVE_PLAN_DIR).glob("*.md")
        )
    )
    unchecked_plan_items = tuple(
        item
        for plan_path in active_plan_files
        for item in _unchecked_items(repo_root / plan_path)
    )
    migration_files = tuple(
        sorted(
            path.relative_to(repo_root)
            for path in (repo_root / "db" / "migrations").glob("*.sql")
        )
    )
    migration_numbers = tuple(
        number
        for path in migration_files
        for number in [_migration_number(path)]
        if number is not None
    )
    next_migration_number = (
        max(migration_numbers) + 1 if migration_numbers else None
    )
    operation_doc = _read_optional(repo_root / "docs" / "Symphony-Operation.md")
    workflow_doc = _read_optional(repo_root / "WORKFLOW.md")

    return PlanningContext(
        root=repo_root,
        active_plan_files=active_plan_files,
        unchecked_plan_items=unchecked_plan_items,
        migration_files=migration_files,
        next_migration_number=next_migration_number,
        has_operation_doc=(
            "Planning Steward" in operation_doc
            and "Objective Impact" in operation_doc
        ),
        has_workflow_objective_impact="Objective Impact" in workflow_doc,
        has_objective_store=(
            (repo_root / OBJECTIVE_ACTIVE_DIR).is_dir()
            and (repo_root / OBJECTIVE_COMPLETED_DIR).is_dir()
        ),
        has_planning_steward=(repo_root / "scripts" / "planning_steward.py").exists(),
        has_backtest_metadata_migration=(
            repo_root / "db" / "migrations" / "004_backtest_metadata.sql"
        ).exists(),
        has_backtest_metadata_code_writes=_has_code_token(
            repo_root,
            (
                "INSERT INTO silver." + "model_runs",
                "INSERT INTO silver." + "backtest_runs",
            ),
            search_dirs=("src", "scripts"),
        ),
        git_branch=_git_branch(repo_root),
        git_dirty=_git_dirty(repo_root),
    )


def build_signals(context: PlanningContext) -> tuple[RepoSignal, ...]:
    return (
        RepoSignal("Branch", context.git_branch or "unknown"),
        RepoSignal("Dirty worktree", "yes" if context.git_dirty else "no"),
        RepoSignal("Active plan files", str(len(context.active_plan_files))),
        RepoSignal("Unchecked active-plan items", str(len(context.unchecked_plan_items))),
        RepoSignal("Migration files", str(len(context.migration_files))),
        RepoSignal(
            "Next migration number",
            "n/a"
            if context.next_migration_number is None
            else f"{context.next_migration_number:03d}",
        ),
        RepoSignal(
            "Objective store",
            "present" if context.has_objective_store else "missing",
        ),
        RepoSignal(
            "Backtest metadata code writes",
            "present" if context.has_backtest_metadata_code_writes else "missing",
        ),
        RepoSignal(
            "Operation proof-packet policy",
            "present"
            if context.has_workflow_objective_impact
            else "missing Objective Impact",
        ),
    )


def build_objective_proposals(
    context: PlanningContext,
) -> tuple[ObjectiveProposal, ...]:
    """Return prioritized Objective proposals supported by local evidence."""
    proposals: list[ObjectiveProposal] = []
    if (
        context.has_backtest_metadata_migration
        and not context.has_backtest_metadata_code_writes
    ):
        proposals.append(_backtest_metadata_objective(context))
    if context.has_operation_doc and not context.has_objective_store:
        proposals.append(_objective_store_objective(context))
    if context.unchecked_plan_items:
        proposals.append(_phase1_plan_reconciliation_objective(context))
    if context.has_operation_doc and context.has_planning_steward:
        proposals.append(_linear_ticket_factory_objective(context))

    return tuple(_dedupe_objectives(proposals))


def render_proposal(
    proposal: PlanningProposal,
    *,
    output_format: OutputFormat = "markdown",
) -> str:
    if output_format == "json":
        return json.dumps(_proposal_dict(proposal), indent=2, sort_keys=True)
    if output_format != "markdown":
        raise PlanningStewardError("output_format must be markdown or json")
    return _render_markdown(proposal)


def _run_check(proposal: PlanningProposal) -> None:
    if not proposal.objectives:
        raise PlanningStewardError(
            "no Objective proposals were produced; inspect docs and active plans"
        )
    print(
        "OK: planning steward proposal check passed "
        f"({len(proposal.objectives)} Objective proposal(s))"
    )


def _backtest_metadata_objective(context: PlanningContext) -> ObjectiveProposal:
    return ObjectiveProposal(
        objective_id="wire-backtest-metadata-registry",
        objective="Wire durable backtest metadata into the falsifier run path.",
        user_value=(
            "Michael can trust that falsifier reports are backed by persisted "
            "model and backtest run metadata, not only markdown output."
        ),
        why_now=(
            "Migration 004 created `model_runs` and `backtest_runs`, but the "
            "runtime code does not yet write those tables."
        ),
        done_when=(
            "Falsifier runs create durable model/backtest metadata rows.",
            "Report reproducibility fields agree with the persisted run rows.",
            "Insufficient-data outcomes are recorded without pretending success.",
            "Tests prove duplicate/re-run behavior is deterministic.",
        ),
        out_of_scope=(
            "No new prediction model.",
            "No schema migration unless the existing registry is proven insufficient.",
            "No live capital, portfolio execution, or text-feature extraction.",
        ),
        guardrails=(
            "No backtest result without costs, baseline, and reproducibility metadata.",
            "Do not write predictions without frozen feature/model/prompt versions.",
            "Keep point-in-time label availability checks intact.",
        ),
        expected_tickets=(
            TicketProposal(
                title="Add repository helpers for model and backtest run metadata",
                purpose="Create a typed write path for `model_runs` and `backtest_runs`.",
                objective_impact=(
                    "This gives the metadata Objective a safe persistence layer "
                    "so falsifier reports can point at durable run records."
                ),
                technical_summary=(
                    "Add repository functions/dataclasses for inserting and "
                    "finishing model/backtest run rows with stable hashes, costs, "
                    "status, and metrics JSON."
                ),
                owns=(
                    "src/silver/analytics/",
                    "tests/test_*metadata*.py",
                ),
                do_not_touch=("db/migrations/", "src/silver/features/"),
                dependencies=(),
                conflict_zones=("src/silver/analytics/", "tests/"),
                validation=("python -m pytest tests/test_migrations.py",),
            ),
            TicketProposal(
                title="Wire persisted run metadata into the falsifier CLI",
                purpose="Connect the report path to durable run metadata writes.",
                objective_impact=(
                    "This makes each generated falsifier report traceable to "
                    "stored run metadata instead of only a command string."
                ),
                technical_summary=(
                    "Update `scripts/run_falsifier.py` to create and finish "
                    "metadata rows around the existing in-memory falsifier result."
                ),
                owns=("scripts/run_falsifier.py", "tests/test_run_falsifier_cli.py"),
                do_not_touch=("db/migrations/", "src/silver/features/"),
                dependencies=("metadata repository helper ticket",),
                conflict_zones=("scripts/run_falsifier.py",),
                validation=(
                    "python scripts/run_falsifier.py --check",
                    "python -m pytest tests/test_run_falsifier_cli.py",
                ),
            ),
            TicketProposal(
                title="Report persisted metadata in proof and docs",
                purpose="Document how persisted run rows support report claims.",
                objective_impact=(
                    "This lets Michael verify how a falsifier report maps back "
                    "to the durable run registry during review."
                ),
                technical_summary=(
                    "Update the falsifier report/runbook to show model/backtest "
                    "run keys or row IDs and the validation commands used."
                ),
                owns=("src/silver/reports/falsifier.py", "docs/PHASE1_RUNBOOK.md"),
                do_not_touch=("db/migrations/",),
                dependencies=("falsifier CLI metadata wiring ticket",),
                conflict_zones=("src/silver/reports/", "docs/"),
                validation=(
                    "python -m pytest tests/test_falsifier_report.py",
                    "python scripts/run_falsifier.py --check",
                ),
            ),
        ),
        validation=(
            "git diff --check",
            "python scripts/apply_migrations.py --check",
            "python scripts/run_falsifier.py --check",
            "python -m pytest",
            "ruff check .",
        ),
        conflict_zones=(
            "scripts/run_falsifier.py",
            "src/silver/analytics/",
            "src/silver/reports/",
        ),
        migration_lane="No migration expected; use 004_backtest_metadata.sql.",
        evidence=(
            "db/migrations/004_backtest_metadata.sql exists.",
            "No `silver.model_runs` or `silver.backtest_runs` writes found in src/scripts.",
        ),
    )


def _objective_store_objective(context: PlanningContext) -> ObjectiveProposal:
    return ObjectiveProposal(
        objective_id="create-objective-store",
        objective="Create the Objective store and seed the first active Objectives.",
        user_value=(
            "Michael can review coherent build chunks from files before tickets "
            "are generated or promoted."
        ),
        why_now=(
            "The operation policy references `docs/objectives/active`, but the "
            "repository does not yet have that store."
        ),
        done_when=(
            "`docs/objectives/active/` and `docs/objectives/completed/` exist.",
            "At least one active Objective file uses the approved template.",
            "Planning steward output links proposals to Objective file paths.",
        ),
        out_of_scope=(
            "No Linear writes.",
            "No automatic Todo promotion.",
            "No new migration.",
        ),
        guardrails=(
            "Objectives must include User Value, Done When, Out Of Scope, Validation, and Conflict Zones.",
            "Tickets generated from Objectives must include Objective Impact.",
        ),
        expected_tickets=(
            TicketProposal(
                title="Add Objective store folders and template",
                purpose="Create the file-based Objective control surface.",
                objective_impact=(
                    "This gives Michael a stable place to approve larger "
                    "chunks of work before agents receive small tickets."
                ),
                technical_summary=(
                    "Add objective directories plus a markdown template that "
                    "matches the operations policy."
                ),
                owns=("docs/objectives/",),
                do_not_touch=("db/migrations/", "src/"),
                dependencies=(),
                conflict_zones=("docs/objectives/",),
                validation=("git diff --check",),
            ),
            TicketProposal(
                title="Teach planning steward to read Objective files",
                purpose="Make proposals aware of approved active Objectives.",
                objective_impact=(
                    "This lets the steward generate tickets from Michael-approved "
                    "Objectives instead of only repo heuristics."
                ),
                technical_summary=(
                    "Parse objective markdown files and include their paths, "
                    "status, and conflict zones in proposal output."
                ),
                owns=("scripts/planning_steward.py", "tests/test_planning_steward.py"),
                do_not_touch=("db/migrations/",),
                dependencies=("Objective store folders and template",),
                conflict_zones=("scripts/planning_steward.py", "docs/objectives/"),
                validation=("python -m pytest tests/test_planning_steward.py",),
            ),
        ),
        validation=(
            "git diff --check",
            "python scripts/planning_steward.py --check",
            "python -m pytest tests/test_planning_steward.py",
        ),
        conflict_zones=("docs/objectives/", "scripts/planning_steward.py"),
        migration_lane="No migration.",
        evidence=("docs/objectives/active and docs/objectives/completed are missing.",),
    )


def _phase1_plan_reconciliation_objective(
    context: PlanningContext,
) -> ObjectiveProposal:
    sample_items = context.unchecked_plan_items[:5]
    evidence = tuple(f"Unchecked plan item: {item}" for item in sample_items)
    return ObjectiveProposal(
        objective_id="reconcile-phase-1-plan-state",
        objective="Reconcile Phase 1 plan status with the current repository.",
        user_value=(
            "Michael can see what Phase 1 truly still needs instead of reading "
            "stale unchecked boxes."
        ),
        why_now=(
            "The active Phase 1 plan has unchecked acceptance items while tests "
            "and scripts show some of that work may already be implemented."
        ),
        done_when=(
            "Each unchecked acceptance item is marked done, clarified, or moved to a new Objective.",
            "Docs identify live prerequisites separately from code gaps.",
            "Validation commands match canonical horizons and current scripts.",
        ),
        out_of_scope=(
            "No feature behavior changes.",
            "No schema migration.",
            "No live FMP or database writes.",
        ),
        guardrails=(
            "Do not mark work complete without code or validation evidence.",
            "Keep Phase 1 narrow and falsifier-focused.",
        ),
        expected_tickets=(
            TicketProposal(
                title="Audit active Phase 1 acceptance criteria",
                purpose="Separate stale checkboxes from real remaining work.",
                objective_impact=(
                    "This gives Michael a truthful Phase 1 map so the planning "
                    "steward generates useful Objectives instead of stale tickets."
                ),
                technical_summary=(
                    "Compare `docs/exec-plans/active/phase-1-foundation.md` "
                    "against scripts, tests, migrations, and docs, then update "
                    "only the plan state and handoff notes."
                ),
                owns=("docs/exec-plans/active/phase-1-foundation.md",),
                do_not_touch=("db/migrations/", "src/"),
                dependencies=(),
                conflict_zones=("docs/exec-plans/active/",),
                validation=(
                    "git diff --check",
                    "python scripts/run_phase1_pipeline.py --check",
                ),
            ),
            TicketProposal(
                title="Promote true remaining Phase 1 gaps into Objectives",
                purpose="Turn real remaining gaps into reviewable Objective files.",
                objective_impact=(
                    "This converts vague unfinished Phase 1 work into clear "
                    "Objective chunks that can safely create tickets."
                ),
                technical_summary=(
                    "Write Objective packets for remaining live-data, metadata, "
                    "or report gaps after the audit identifies them."
                ),
                owns=("docs/objectives/active/",),
                do_not_touch=("db/migrations/",),
                dependencies=("Phase 1 acceptance audit",),
                conflict_zones=("docs/objectives/",),
                validation=("git diff --check",),
            ),
        ),
        validation=(
            "git diff --check",
            "python scripts/run_phase1_pipeline.py --check",
        ),
        conflict_zones=("docs/exec-plans/active/", "docs/objectives/"),
        migration_lane="No migration.",
        evidence=evidence,
    )


def _linear_ticket_factory_objective(context: PlanningContext) -> ObjectiveProposal:
    return ObjectiveProposal(
        objective_id="add-linear-backlog-ticket-factory",
        objective="Create guarded Backlog ticket creation from approved Objectives.",
        user_value=(
            "Michael can approve an Objective once and let the system create "
            "reviewable ticket drafts without hand-writing each slice."
        ),
        why_now=(
            "The propose-only planning steward is the first automation rung; "
            "the next rung is creating Backlog tickets, not Todo work."
        ),
        done_when=(
            "`--create-backlog` creates Linear tickets only from approved Objective files.",
            "Generated tickets include Objective Impact, ownership, dependencies, validation, and conflict zones.",
            "The command has dry-run behavior and clear no-secret logging.",
        ),
        out_of_scope=(
            "No automatic Todo promotion.",
            "No overnight mode.",
            "No semantic conflict repair.",
        ),
        guardrails=(
            "Write to Backlog only.",
            "Never create tickets from an Objective missing conflict zones or validation.",
            "Never print Linear or GitHub tokens.",
        ),
        expected_tickets=(
            TicketProposal(
                title="Add Linear Backlog writer for approved Objectives",
                purpose="Create guarded tickets from Objective files.",
                objective_impact=(
                    "This removes manual ticket drafting while keeping Michael "
                    "in control of which Objectives become work."
                ),
                technical_summary=(
                    "Add a Linear client path for `--create-backlog` with dry-run, "
                    "input validation, and redacted errors."
                ),
                owns=("scripts/planning_steward.py", "tests/test_planning_steward.py"),
                do_not_touch=("scripts/merge_steward.py", "db/migrations/"),
                dependencies=("Objective store ticket",),
                conflict_zones=("scripts/planning_steward.py", "WORKFLOW.md"),
                validation=("python -m pytest tests/test_planning_steward.py",),
            ),
        ),
        validation=(
            "git diff --check",
            "python scripts/planning_steward.py --check",
            "python -m pytest tests/test_planning_steward.py",
        ),
        conflict_zones=("scripts/planning_steward.py", "WORKFLOW.md"),
        migration_lane="No migration.",
        evidence=("scripts/planning_steward.py exists in propose-only mode.",),
    )


def _dedupe_objectives(
    proposals: Sequence[ObjectiveProposal],
) -> tuple[ObjectiveProposal, ...]:
    seen: set[str] = set()
    deduped: list[ObjectiveProposal] = []
    for proposal in proposals:
        if proposal.objective_id in seen:
            continue
        seen.add(proposal.objective_id)
        deduped.append(proposal)
    return tuple(deduped)


def _render_markdown(proposal: PlanningProposal) -> str:
    lines = [
        "# Planning Steward Proposal",
        "",
        f"Status: {proposal.status}",
        "",
        "No Linear, GitHub, database, or vendor writes were performed.",
        "",
        "## Signals",
        "",
        *_table(("Signal", "Value"), ((item.name, item.value) for item in proposal.signals)),
        "",
        "## Recommended Objectives",
        "",
    ]
    if not proposal.objectives:
        lines.extend(
            [
                "No Objective proposals were produced from the current local signals.",
                "",
            ]
        )
        return "\n".join(lines)

    for index, objective in enumerate(proposal.objectives, start=1):
        lines.extend(_objective_markdown(index, objective))
    return "\n".join(lines).rstrip() + "\n"


def _objective_markdown(index: int, objective: ObjectiveProposal) -> list[str]:
    lines = [
        f"### {index}. {objective.objective}",
        "",
        f"Objective ID: `{objective.objective_id}`",
        "",
        "**User Value**",
        "",
        objective.user_value,
        "",
        "**Why Now**",
        "",
        objective.why_now,
        "",
        "**Done When**",
        *_bullet_lines(objective.done_when),
        "",
        "**Out Of Scope**",
        *_bullet_lines(objective.out_of_scope),
        "",
        "**Guardrails**",
        *_bullet_lines(objective.guardrails),
        "",
        "**Migration Lane**",
        "",
        objective.migration_lane,
        "",
        "**Conflict Zones**",
        *_bullet_lines(objective.conflict_zones),
        "",
        "**Validation**",
        *_bullet_lines(tuple(f"`{item}`" for item in objective.validation)),
        "",
        "**Evidence**",
        *_bullet_lines(objective.evidence),
        "",
        "**Expected Tickets**",
        "",
    ]
    for ticket_index, ticket in enumerate(objective.expected_tickets, start=1):
        lines.extend(_ticket_markdown(ticket_index, ticket))
    return lines


def _ticket_markdown(index: int, ticket: TicketProposal) -> list[str]:
    return [
        f"{index}. {ticket.title}",
        "",
        f"   Purpose: {ticket.purpose}",
        "",
        f"   Objective Impact: {ticket.objective_impact}",
        "",
        f"   Technical Summary: {ticket.technical_summary}",
        "",
        f"   Owns: {_comma_code(ticket.owns)}",
        "",
        f"   Do Not Touch: {_comma_code(ticket.do_not_touch)}",
        "",
        f"   Dependencies: {_comma_or_none(ticket.dependencies)}",
        "",
        f"   Conflict Zones: {_comma_code(ticket.conflict_zones)}",
        "",
        f"   Validation: {_comma_code(ticket.validation)}",
        "",
    ]


def _proposal_dict(proposal: PlanningProposal) -> dict[str, object]:
    return {
        "status": proposal.status,
        "signals": [
            {"name": signal.name, "value": signal.value}
            for signal in proposal.signals
        ],
        "objectives": [_objective_dict(item) for item in proposal.objectives],
    }


def _objective_dict(objective: ObjectiveProposal) -> dict[str, object]:
    return {
        "objective_id": objective.objective_id,
        "objective": objective.objective,
        "user_value": objective.user_value,
        "why_now": objective.why_now,
        "done_when": list(objective.done_when),
        "out_of_scope": list(objective.out_of_scope),
        "guardrails": list(objective.guardrails),
        "expected_tickets": [_ticket_dict(item) for item in objective.expected_tickets],
        "validation": list(objective.validation),
        "conflict_zones": list(objective.conflict_zones),
        "migration_lane": objective.migration_lane,
        "evidence": list(objective.evidence),
    }


def _ticket_dict(ticket: TicketProposal) -> dict[str, object]:
    return {
        "title": ticket.title,
        "purpose": ticket.purpose,
        "objective_impact": ticket.objective_impact,
        "technical_summary": ticket.technical_summary,
        "owns": list(ticket.owns),
        "do_not_touch": list(ticket.do_not_touch),
        "dependencies": list(ticket.dependencies),
        "conflict_zones": list(ticket.conflict_zones),
        "validation": list(ticket.validation),
    }


def _unchecked_items(path: Path) -> tuple[str, ...]:
    if not path.exists():
        return ()
    items: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.match(r"^\s*-\s+\[ \]\s+(.+?)\s*$", line)
        if match is not None:
            items.append(match.group(1))
    return tuple(items)


def _migration_number(path: Path) -> int | None:
    match = re.match(r"^(\d+)_", path.name)
    if match is None:
        return None
    return int(match.group(1))


def _has_code_token(
    root: Path,
    tokens: Sequence[str],
    *,
    search_dirs: Sequence[str],
) -> bool:
    for directory in search_dirs:
        base = root / directory
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if not path.is_file() or path.suffix not in {".py", ".sql", ".md"}:
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            if any(token in text for token in tokens):
                return True
    return False


def _git_branch(root: Path) -> str:
    result = _git(root, "branch", "--show-current")
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def _git_dirty(root: Path) -> bool:
    result = _git(root, "status", "--short")
    if result.returncode != 0:
        return False
    return bool(result.stdout.strip())


def _git(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=root,
        text=True,
        capture_output=True,
        check=False,
    )


def _read_optional(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _table(
    headers: tuple[str, ...],
    rows: Sequence[Sequence[str]],
) -> list[str]:
    normalized_rows = tuple(tuple(row) for row in rows)
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in normalized_rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def _bullet_lines(values: Sequence[str]) -> list[str]:
    return [f"- {value}" for value in values]


def _comma_code(values: Sequence[str]) -> str:
    if not values:
        return "none"
    return ", ".join(f"`{value}`" for value in values)


def _comma_or_none(values: Sequence[str]) -> str:
    if not values:
        return "none"
    return ", ".join(values)


if __name__ == "__main__":
    raise SystemExit(main())

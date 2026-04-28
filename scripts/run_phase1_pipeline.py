#!/usr/bin/env python
"""Run the Phase 1 falsifier pipeline in deterministic order."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = ROOT / "scripts"
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from silver.reference.seed_data import FALSIFIER_UNIVERSE_NAME  # noqa: E402
from silver.time.trading_calendar import CANONICAL_HORIZONS  # noqa: E402


DEFAULT_REPORT_PATH = ROOT / "reports" / "falsifier" / "week_1_momentum.md"


class Phase1PipelineError(RuntimeError):
    """Raised when the Phase 1 pipeline cannot continue safely."""


@dataclass(frozen=True, slots=True)
class PipelineStep:
    name: str
    script: Path
    check_label: str
    apply_label: str

    @property
    def display_path(self) -> str:
        return str(self.script.relative_to(ROOT))


@dataclass(frozen=True, slots=True)
class StepAction:
    step: PipelineStep
    command: list[str] | None
    skip_reason: str | None = None


@dataclass(frozen=True, slots=True)
class StepResult:
    name: str
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class PipelineResult:
    check: bool
    completed: tuple[StepResult, ...]
    skipped: tuple[StepResult, ...]
    report_path: Path


Runner = Callable[[list[str]], subprocess.CompletedProcess[str]]
PsqlFinder = Callable[[str], str | None]


DATABASE_BOOTSTRAP = PipelineStep(
    name="database bootstrap",
    script=SCRIPT_DIR / "bootstrap_database.py",
    check_label="Check database bootstrap",
    apply_label="Run database bootstrap",
)
FMP_PRICE_INGEST = PipelineStep(
    name="FMP price ingest",
    script=SCRIPT_DIR / "ingest_fmp_prices.py",
    check_label="Check FMP price ingest",
    apply_label="Run FMP price ingest",
)
FORWARD_LABELS = PipelineStep(
    name="forward-label materialization",
    script=SCRIPT_DIR / "materialize_forward_labels.py",
    check_label="Check forward-label materialization",
    apply_label="Run forward-label materialization",
)
MOMENTUM_FEATURES = PipelineStep(
    name="momentum feature materialization",
    script=SCRIPT_DIR / "materialize_momentum_12_1.py",
    check_label="Check momentum feature materialization",
    apply_label="Run momentum feature materialization",
)
FALSIFIER_REPORT = PipelineStep(
    name="falsifier report generation",
    script=SCRIPT_DIR / "run_falsifier.py",
    check_label="Check falsifier report generation",
    apply_label="Run falsifier report generation",
)

PIPELINE_STEPS: tuple[PipelineStep, ...] = (
    DATABASE_BOOTSTRAP,
    FMP_PRICE_INGEST,
    FORWARD_LABELS,
    MOMENTUM_FEATURES,
    FALSIFIER_REPORT,
)


def subprocess_run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--universe",
        default=FALSIFIER_UNIVERSE_NAME,
        help=f"point-in-time universe name; defaults to {FALSIFIER_UNIVERSE_NAME}",
    )
    parser.add_argument(
        "--start-date",
        type=_date_arg,
        help="inclusive lower bound for price/label/feature materialization",
    )
    parser.add_argument(
        "--end-date",
        type=_date_arg,
        help="inclusive upper bound for price/label/feature materialization",
    )
    parser.add_argument(
        "--horizon",
        type=int,
        default=63,
        help="forward-return horizon for the falsifier report",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection URL; defaults to DATABASE_URL",
    )
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help="skip live FMP price ingest when normalized prices already exist",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate command wiring without live FMP/database writes",
    )
    return parser.parse_args(argv)


def run_pipeline(
    args: argparse.Namespace,
    *,
    runner: Runner = subprocess_run,
    psql_finder: PsqlFinder = shutil.which,
) -> PipelineResult:
    _validate_args(args)
    if not args.check:
        _validate_live_prerequisites(args=args, psql_finder=psql_finder)

    completed: list[StepResult] = []
    skipped: list[StepResult] = []
    actions = _build_actions(args)
    total = len(actions)
    for index, action in enumerate(actions, start=1):
        label = action.step.check_label if args.check else action.step.apply_label
        if action.command is None:
            reason = action.skip_reason or "skipped"
            print(f"[{index}/{total}] Skip {action.step.name}: {reason}", flush=True)
            skipped.append(StepResult(action.step.name, reason))
            continue

        print(f"[{index}/{total}] {label}: {action.step.display_path}", flush=True)
        result = runner(action.command)
        if result.returncode != 0:
            raise Phase1PipelineError(
                _format_step_failure(
                    action=action,
                    result=result,
                    database_url=args.database_url,
                )
            )
        print(f"[{index}/{total}] OK: {action.step.name}", flush=True)
        completed.append(
            StepResult(action.step.name, "check" if args.check else None)
        )

    return PipelineResult(
        check=args.check,
        completed=tuple(completed),
        skipped=tuple(skipped),
        report_path=DEFAULT_REPORT_PATH,
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    runner: Runner = subprocess_run,
    psql_finder: PsqlFinder = shutil.which,
) -> int:
    args = parse_args(argv)
    try:
        result = run_pipeline(args, runner=runner, psql_finder=psql_finder)
    except Phase1PipelineError as exc:
        print(f"error: {_redact(str(exc), args.database_url)}", file=sys.stderr)
        return 1

    print(_format_summary(result))
    return 0


def _build_actions(args: argparse.Namespace) -> tuple[StepAction, ...]:
    actions: list[StepAction] = []
    for step in PIPELINE_STEPS:
        if step is FMP_PRICE_INGEST and args.skip_ingest:
            actions.append(
                StepAction(
                    step=step,
                    command=None,
                    skip_reason=(
                        "--skip-ingest requested; assuming normalized daily "
                        "prices already exist"
                    ),
                )
            )
            continue
        actions.append(StepAction(step=step, command=_build_command(step, args)))
    return tuple(actions)


def _build_command(step: PipelineStep, args: argparse.Namespace) -> list[str]:
    command = [sys.executable, str(step.script)]
    if step is DATABASE_BOOTSTRAP:
        if args.check:
            command.append("--check")
        else:
            command.extend(["--database-url", args.database_url])
        return command

    if step is FMP_PRICE_INGEST:
        command.extend(["--universe", args.universe])
        _append_date_bounds(command, args)
        if args.check:
            command.append("--check")
        else:
            command.extend(["--database-url", args.database_url])
        return command

    if step is FORWARD_LABELS:
        command.extend(["--universe", args.universe])
        _append_date_bounds(command, args)
        if args.check:
            command.append("--check")
        else:
            command.extend(["--database-url", args.database_url])
        return command

    if step is MOMENTUM_FEATURES:
        command.extend(["--universe", args.universe])
        _append_date_bounds(command, args)
        if args.check:
            command.append("--check")
        else:
            command.extend(["--database-url", args.database_url])
        return command

    if step is FALSIFIER_REPORT:
        command.extend(
            [
                "--horizon",
                str(args.horizon),
                "--universe",
                args.universe,
                "--output-path",
                _display_path(DEFAULT_REPORT_PATH),
            ]
        )
        if args.check:
            command.append("--check")
        else:
            command.extend(["--database-url", args.database_url])
        return command

    raise Phase1PipelineError(f"unknown pipeline step: {step.name}")


def _append_date_bounds(command: list[str], args: argparse.Namespace) -> None:
    if args.start_date is not None:
        command.extend(["--start-date", args.start_date.isoformat()])
    if args.end_date is not None:
        command.extend(["--end-date", args.end_date.isoformat()])


def _validate_args(args: argparse.Namespace) -> None:
    if not isinstance(args.universe, str) or not args.universe.strip():
        raise Phase1PipelineError("universe must be a non-empty string")
    if args.horizon not in CANONICAL_HORIZONS:
        allowed = ", ".join(str(horizon) for horizon in CANONICAL_HORIZONS)
        raise Phase1PipelineError(f"horizon must be one of {allowed}; got {args.horizon}")
    if (
        args.start_date is not None
        and args.end_date is not None
        and args.start_date > args.end_date
    ):
        raise Phase1PipelineError("--start-date must be on or before --end-date")


def _validate_live_prerequisites(
    *,
    args: argparse.Namespace,
    psql_finder: PsqlFinder,
) -> None:
    if not args.database_url:
        raise Phase1PipelineError(
            "DATABASE_URL is required unless --check is used; pass "
            "--database-url or set DATABASE_URL"
        )
    if psql_finder("psql") is None:
        raise Phase1PipelineError(
            "psql is required for database bootstrap and falsifier input reads; "
            "install PostgreSQL client tools or put psql on PATH"
        )
    if not args.skip_ingest and not os.environ.get("FMP_API_KEY"):
        raise Phase1PipelineError(
            "FMP_API_KEY is required unless --check or --skip-ingest is used; "
            "set FMP_API_KEY or rerun with --skip-ingest when prices already exist"
        )


def _format_step_failure(
    *,
    action: StepAction,
    result: subprocess.CompletedProcess[str],
    database_url: str | None,
) -> str:
    message = f"{action.step.name} failed with exit code {result.returncode}"
    detail = _failure_detail(result=result, database_url=database_url)
    if detail:
        return f"{message}:\n{detail}"
    return message


def _failure_detail(
    *,
    result: subprocess.CompletedProcess[str],
    database_url: str | None,
) -> str:
    parts = [
        part.strip()
        for part in (result.stderr, result.stdout)
        if isinstance(part, str) and part.strip()
    ]
    if not parts:
        return ""
    redacted = _redact("\n".join(parts), database_url)
    lines = redacted.splitlines()
    return "\n".join(lines[-12:])


def _format_summary(result: PipelineResult) -> str:
    mode = "check" if result.check else "run"
    lines = [f"Phase 1 pipeline {mode} completed.", "Completed steps:"]
    if result.completed:
        for step in result.completed:
            suffix = f" ({step.detail})" if step.detail else ""
            lines.append(f"- {step.name}{suffix}")
    else:
        lines.append("- none")

    lines.append("Skipped steps:")
    if result.skipped:
        for step in result.skipped:
            detail = f": {step.detail}" if step.detail else ""
            lines.append(f"- {step.name}{detail}")
    else:
        lines.append("- none")

    lines.extend(
        [
            f"Output report path: {_display_path(result.report_path)}",
            f"Next action: {_next_action(result)}",
        ]
    )
    return "\n".join(lines)


def _next_action(result: PipelineResult) -> str:
    if result.check:
        return (
            "set DATABASE_URL and FMP_API_KEY, ensure psql is on PATH, then "
            "rerun without --check"
        )
    return f"review {_display_path(result.report_path)}"


def _display_path(path: Path) -> str:
    resolved = path if path.is_absolute() else (ROOT / path)
    try:
        return str(resolved.resolve().relative_to(ROOT))
    except ValueError:
        return str(resolved.resolve())


def _redact(message: str, database_url: str | None) -> str:
    redacted = message
    for secret in (
        database_url,
        os.environ.get("DATABASE_URL"),
        os.environ.get("FMP_API_KEY"),
    ):
        if secret:
            redacted = redacted.replace(secret, "[REDACTED]")
    return redacted


def _date_arg(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD format") from exc


if __name__ == "__main__":
    raise SystemExit(main())

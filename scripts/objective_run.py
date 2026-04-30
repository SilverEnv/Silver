#!/usr/bin/env python
"""Run the objective-aware controller above Symphony-style ticket runners."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sqlite3
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol

import yaml


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import linear_mirror  # noqa: E402
import merge_steward  # noqa: E402
import work_ledger  # noqa: E402


DEFAULT_CONFIG_PATH = ROOT / "config" / "agentic_build.yaml"
DEFAULT_MAX_ACTIVE = 5
DEFAULT_READY_BUFFER = 5
DEFAULT_POLL_INTERVAL_SECONDS = 60
DEFAULT_MAX_CYCLES = 1
DEFAULT_LIMIT = 100
ACTOR = "objective_run_controller"

RepairMode = Literal["off", "plan", "apply"]
OutputFormat = Literal["text", "json"]
CommandKind = Literal["import", "admit", "mirror", "vcs", "repair", "merge", "status"]
RunnerActionName = Literal["noop", "transition", "skip"]

RUNNER_TO_LEDGER_STATE = {
    "Todo": "Ready",
    "In Progress": "In Progress",
    "Rework": "Rework",
    "Merging": "Merging",
    "Safety Review": "Safety Review",
    "Done": "Done",
    "Canceled": "Canceled",
    "Duplicate": "Duplicate",
}

TERMINAL_LEDGER_STATES = frozenset(("Done", "Canceled", "Duplicate"))
SAFETY_LEDGER_STATES = frozenset(("Safety Review", "Blocked"))


class ObjectiveRunError(RuntimeError):
    """Raised when the objective run controller cannot continue safely."""


@dataclass(frozen=True, slots=True)
class ControllerConfig:
    root: Path
    ledger: Path
    config_path: Path
    project: str
    team_id: str | None
    repo: str
    limit: int
    max_active: int
    ready_buffer: int
    apply: bool
    repair_mode: RepairMode
    push_repairs: bool
    run_repair_validation: bool
    repair_agent_command: str | None
    poll_interval: int
    max_cycles: int
    stop_on_safety: bool
    observe_runner: bool
    output_format: OutputFormat


@dataclass(frozen=True, slots=True)
class CommandSpec:
    kind: CommandKind
    argv: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CommandResult:
    kind: CommandKind
    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str


@dataclass(frozen=True, slots=True)
class RunnerStateAction:
    ticket_id: str
    linear_identifier: str | None
    from_status: str
    linear_state: str
    action: RunnerActionName
    target_status: str | None
    reason: str


@dataclass(frozen=True, slots=True)
class CycleResult:
    cycle: int
    command_results: tuple[CommandResult, ...]
    runner_actions: tuple[RunnerStateAction, ...]
    blockers: tuple[str, ...]
    stopped: bool
    stop_reason: str | None


class CommandRunner(Protocol):
    def run(self, command: CommandSpec, *, cwd: Path) -> CommandResult:
        """Run one controller command."""


class RunnerObserver(Protocol):
    def observe(self, config: ControllerConfig) -> tuple[RunnerStateAction, ...]:
        """Observe runner state and optionally sync it into the ledger."""


class SubprocessCommandRunner:
    def run(self, command: CommandSpec, *, cwd: Path) -> CommandResult:
        result = subprocess.run(
            list(command.argv),
            cwd=cwd,
            text=True,
            capture_output=True,
            check=False,
        )
        return CommandResult(
            kind=command.kind,
            argv=command.argv,
            returncode=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
        )


class LinearSymphonyObserver:
    """Mirror Symphony-visible Linear state back into the local ledger."""

    def observe(self, config: ControllerConfig) -> tuple[RunnerStateAction, ...]:
        if not config.observe_runner:
            return ()
        api_key = os.environ.get("LINEAR_API_KEY")
        if not api_key:
            raise ObjectiveRunError("LINEAR_API_KEY is required to observe Symphony")
        if not config.project:
            raise ObjectiveRunError("Linear project is required to observe Symphony")

        client = linear_mirror.LinearClient(api_key)
        snapshot = client.project_snapshot(config.project, limit=config.limit)
        issue_by_ticket_id = linear_mirror.issues_by_ledger_ticket_id(snapshot.issues)
        issue_by_identifier = {
            issue.identifier: issue
            for issue in snapshot.issues
        }

        with work_ledger.connect_existing(config.ledger) as connection:
            tickets = linear_mirror.mirror_tickets(connection)
            actions = tuple(
                decide_runner_state_action(
                    ticket_id=ticket.id,
                    linear_identifier=ticket.linear_identifier,
                    ledger_status=ticket.status,
                    linear_state=issue.state if issue is not None else None,
                )
                for ticket in tickets
                for issue in [
                    linear_mirror.matching_issue(
                        ticket,
                        issue_by_ticket_id=issue_by_ticket_id,
                        issue_by_identifier=issue_by_identifier,
                    )
                ]
                if issue is not None
            )
            if config.apply:
                apply_runner_state_actions(connection, actions)
        return actions


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--ledger", type=Path, default=None)
    parser.add_argument("--project", default=None)
    parser.add_argument("--team-id", default=None)
    parser.add_argument("--repo", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--max-active", type=int, default=None)
    parser.add_argument("--ready-buffer", type=int, default=None)
    parser.add_argument("--repair-mode", choices=("off", "plan", "apply"), default=None)
    parser.add_argument("--push-repairs", action="store_true")
    parser.add_argument("--run-repair-validation", action="store_true")
    parser.add_argument("--repair-agent-command", default=None)
    parser.add_argument("--poll-interval", type=int, default=None)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument(
        "--no-observe-runner",
        action="store_true",
        help="skip runner-state observation; intended only for diagnostics",
    )
    parser.add_argument(
        "--no-stop-on-safety",
        action="store_true",
        help="continue cycles even when Safety Review or Blocked tickets exist",
    )
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate local controller configuration without network calls or writes",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        config = controller_config_from_args(args)
        if args.check:
            print(check_configuration(config))
            return 0

        max_cycles = config.max_cycles
        if args.watch and max_cycles < 1:
            max_cycles = 1
        runner = SubprocessCommandRunner()
        observer = LinearSymphonyObserver()
        results: list[CycleResult] = []
        cycle = 1
        while True:
            result = run_cycle(config, cycle=cycle, runner=runner, observer=observer)
            results.append(result)
            print(render_results((result,), config.output_format))
            if result.stopped:
                break
            if not args.watch:
                break
            if cycle >= max_cycles:
                break
            cycle += 1
            time.sleep(config.poll_interval)
    except ObjectiveRunError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


def controller_config_from_args(args: argparse.Namespace) -> ControllerConfig:
    raw_config = load_project_config(args.config)
    controller = mapping(raw_config.get("controller"))
    runner = mapping(raw_config.get("runner"))
    tracker = mapping(raw_config.get("tracker"))
    vcs = mapping(raw_config.get("vcs"))

    root = args.root.resolve()
    ledger = args.ledger or Path(
        str(controller.get("ledger_path") or work_ledger.default_ledger_path())
    )
    project = first_text(
        args.project,
        tracker.get("project"),
        tracker.get("project_slug"),
        linear_mirror.read_workflow_project_slug(root / "WORKFLOW.md"),
    )
    repo = first_text(
        args.repo,
        vcs.get("repo"),
        merge_steward.detect_github_repo(),
    )
    repair_mode = first_text(args.repair_mode, controller.get("repair_mode"), "plan")
    if repair_mode not in {"off", "plan", "apply"}:
        raise ObjectiveRunError("repair_mode must be one of: off, plan, apply")

    return ControllerConfig(
        root=root,
        ledger=Path(ledger).expanduser(),
        config_path=args.config,
        project=project,
        team_id=first_optional_text(args.team_id, tracker.get("team_id")),
        repo=repo,
        limit=first_int(args.limit, controller.get("limit"), DEFAULT_LIMIT),
        max_active=first_int(
            args.max_active,
            controller.get("max_active"),
            DEFAULT_MAX_ACTIVE,
        ),
        ready_buffer=first_int(
            args.ready_buffer,
            controller.get("ready_buffer"),
            DEFAULT_READY_BUFFER,
        ),
        apply=args.apply,
        repair_mode=repair_mode,  # type: ignore[arg-type]
        push_repairs=bool(args.push_repairs or controller.get("push_repairs", False)),
        run_repair_validation=bool(
            args.run_repair_validation
            or controller.get("run_repair_validation", False)
        ),
        repair_agent_command=first_optional_text(
            args.repair_agent_command,
            controller.get("repair_agent_command"),
        ),
        poll_interval=first_int(
            args.poll_interval,
            controller.get("poll_interval_seconds"),
            DEFAULT_POLL_INTERVAL_SECONDS,
        ),
        max_cycles=first_int(
            args.max_cycles,
            controller.get("max_cycles"),
            DEFAULT_MAX_CYCLES,
        ),
        stop_on_safety=not bool(
            args.no_stop_on_safety
            or controller.get("stop_on_safety", True) is False
        ),
        observe_runner=not bool(
            args.no_observe_runner
            or runner.get("observe_state", True) is False
        ),
        output_format=args.format,
    )


def run_cycle(
    config: ControllerConfig,
    *,
    cycle: int,
    runner: CommandRunner,
    observer: RunnerObserver,
) -> CycleResult:
    command_results: list[CommandResult] = []
    runner_actions: list[RunnerStateAction] = []

    runner_actions.extend(observer.observe(config))
    blockers = safety_blockers(config.ledger)
    if blockers and config.stop_on_safety:
        return CycleResult(
            cycle=cycle,
            command_results=tuple(command_results),
            runner_actions=tuple(runner_actions),
            blockers=blockers,
            stopped=True,
            stop_reason="safety blockers present before dispatch",
        )

    for command in command_plan(config):
        result = runner.run(command, cwd=config.root)
        command_results.append(result)
        if result.returncode != 0:
            return CycleResult(
                cycle=cycle,
                command_results=tuple(command_results),
                runner_actions=tuple(runner_actions),
                blockers=safety_blockers(config.ledger),
                stopped=True,
                stop_reason=f"{command.kind} command failed",
            )
        if command.kind in {"mirror", "merge"}:
            runner_actions.extend(observer.observe(config))
            blockers = safety_blockers(config.ledger)
            if blockers and config.stop_on_safety:
                return CycleResult(
                    cycle=cycle,
                    command_results=tuple(command_results),
                    runner_actions=tuple(runner_actions),
                    blockers=blockers,
                    stopped=True,
                    stop_reason="safety blockers present after runner observation",
                )

    blockers = safety_blockers(config.ledger)
    stopped = bool(blockers and config.stop_on_safety)
    return CycleResult(
        cycle=cycle,
        command_results=tuple(command_results),
        runner_actions=tuple(runner_actions),
        blockers=blockers,
        stopped=stopped,
        stop_reason="safety blockers present" if stopped else None,
    )


def command_plan(config: ControllerConfig) -> tuple[CommandSpec, ...]:
    commands: list[CommandSpec] = [
        CommandSpec(
            "import",
            command_args(
                "work_ledger.py",
                "--ledger",
                str(config.ledger),
                "--root",
                str(config.root),
                "import-objectives",
                *(("--dry-run",) if not config.apply else ()),
            ),
        ),
        CommandSpec(
            "admit",
            command_args(
                "work_ledger.py",
                "--ledger",
                str(config.ledger),
                "admit",
                "--max-active",
                str(config.max_active),
                "--ready-buffer",
                str(config.ready_buffer),
                *(("--dry-run",) if not config.apply else ()),
            ),
        ),
        CommandSpec(
            "mirror",
            command_args(
                "linear_mirror.py",
                "--ledger",
                str(config.ledger),
                "--project",
                config.project,
                "--limit",
                str(config.limit),
                *((("--team-id", config.team_id) if config.team_id else ())),
                *(("--apply",) if config.apply else ()),
            ),
        ),
        CommandSpec(
            "vcs",
            command_args(
                "vcs_reconciler.py",
                "--ledger",
                str(config.ledger),
                "--repo",
                config.repo,
                "--limit",
                str(config.limit),
                *(("--apply",) if config.apply else ()),
            ),
        ),
        CommandSpec(
            "repair",
            command_args(
                "integration_steward.py",
                "--ledger",
                str(config.ledger),
                *(("--apply",) if config.apply else ()),
            ),
        ),
    ]
    if config.repair_mode != "off":
        commands.append(repair_runner_command(config))
    commands.extend(
        [
            CommandSpec(
                "merge",
                command_args(
                    "merge_steward.py",
                    "--project",
                    config.project,
                    "--repo",
                    config.repo,
                    "--limit",
                    str(config.limit),
                    *(("--dry-run",) if not config.apply else ()),
                ),
            ),
            CommandSpec(
                "vcs",
                command_args(
                    "vcs_reconciler.py",
                    "--ledger",
                    str(config.ledger),
                    "--repo",
                    config.repo,
                    "--limit",
                    str(config.limit),
                    *(("--apply",) if config.apply else ()),
                ),
            ),
            CommandSpec(
                "mirror",
                command_args(
                    "linear_mirror.py",
                    "--ledger",
                    str(config.ledger),
                    "--project",
                    config.project,
                    "--limit",
                    str(config.limit),
                    *((("--team-id", config.team_id) if config.team_id else ())),
                    *(("--apply",) if config.apply else ()),
                ),
            ),
            CommandSpec(
                "status",
                command_args("work_ledger.py", "--ledger", str(config.ledger), "status"),
            ),
        ]
    )
    return tuple(commands)


def repair_runner_command(config: ControllerConfig) -> CommandSpec:
    should_apply = config.apply and config.repair_mode == "apply"
    args: list[str] = [
        "integration_repair_runner.py",
        "--ledger",
        str(config.ledger),
    ]
    if should_apply:
        args.append("--apply")
        if config.push_repairs:
            args.append("--push")
        if config.run_repair_validation:
            args.append("--run-validation")
        if config.repair_agent_command:
            args.extend(("--agent-command", config.repair_agent_command))
    return CommandSpec("repair", command_args(*args))


def command_args(script_name: str, *args: str) -> tuple[str, ...]:
    return (sys.executable, str(SCRIPT_DIR / script_name), *args)


def decide_runner_state_action(
    *,
    ticket_id: str,
    linear_identifier: str | None,
    ledger_status: str,
    linear_state: str | None,
) -> RunnerStateAction:
    if linear_state is None:
        return RunnerStateAction(
            ticket_id=ticket_id,
            linear_identifier=linear_identifier,
            from_status=ledger_status,
            linear_state="missing",
            action="skip",
            target_status=None,
            reason="no mirrored runner issue found",
        )
    target_status = RUNNER_TO_LEDGER_STATE.get(linear_state)
    if target_status is None:
        return RunnerStateAction(
            ticket_id=ticket_id,
            linear_identifier=linear_identifier,
            from_status=ledger_status,
            linear_state=linear_state,
            action="skip",
            target_status=None,
            reason=f"runner state is not mapped: {linear_state}",
        )
    if ledger_status == target_status:
        return RunnerStateAction(
            ticket_id=ticket_id,
            linear_identifier=linear_identifier,
            from_status=ledger_status,
            linear_state=linear_state,
            action="noop",
            target_status=target_status,
            reason="ledger already matches runner state",
        )
    if ledger_status in TERMINAL_LEDGER_STATES:
        return RunnerStateAction(
            ticket_id=ticket_id,
            linear_identifier=linear_identifier,
            from_status=ledger_status,
            linear_state=linear_state,
            action="skip",
            target_status=target_status,
            reason="terminal ledger state is authoritative",
        )
    if ledger_status == "Backlog":
        return RunnerStateAction(
            ticket_id=ticket_id,
            linear_identifier=linear_identifier,
            from_status=ledger_status,
            linear_state=linear_state,
            action="skip",
            target_status=target_status,
            reason="ledger admission is authoritative for Backlog tickets",
        )
    if target_status == "Ready":
        return RunnerStateAction(
            ticket_id=ticket_id,
            linear_identifier=linear_identifier,
            from_status=ledger_status,
            linear_state=linear_state,
            action="skip",
            target_status=target_status,
            reason="runner Todo cannot pull ledger state backward",
        )
    if target_status == "Done" and ledger_status != "Merging":
        return RunnerStateAction(
            ticket_id=ticket_id,
            linear_identifier=linear_identifier,
            from_status=ledger_status,
            linear_state=linear_state,
            action="skip",
            target_status=target_status,
            reason="Done requires a Merging ledger handoff or VCS evidence",
        )
    return RunnerStateAction(
        ticket_id=ticket_id,
        linear_identifier=linear_identifier,
        from_status=ledger_status,
        linear_state=linear_state,
        action="transition",
        target_status=target_status,
        reason="runner advanced mirrored ticket state",
    )


def apply_runner_state_actions(
    connection: sqlite3.Connection,
    actions: Sequence[RunnerStateAction],
) -> None:
    for action in actions:
        if action.action != "transition" or action.target_status is None:
            continue
        work_ledger.transition_ticket(
            connection,
            ticket_id=action.ticket_id,
            status=action.target_status,
            actor=ACTOR,
            message=(
                f"Observed {action.linear_identifier or action.ticket_id} "
                f"in runner state {action.linear_state}."
            ),
        )


def safety_blockers(ledger: Path) -> tuple[str, ...]:
    with work_ledger.connect_existing(ledger) as connection:
        rows = connection.execute(
            """
            SELECT id, status, title
            FROM tickets
            WHERE status IN (?, ?)
            ORDER BY objective_id, sequence
            """,
            tuple(SAFETY_LEDGER_STATES),
        ).fetchall()
    return tuple(f"{row['id']} | {row['status']} | {row['title']}" for row in rows)


def check_configuration(config: ControllerConfig) -> str:
    if not config.ledger.exists():
        raise ObjectiveRunError(
            f"ledger does not exist: {config.ledger}; run work_ledger.py init"
        )
    if not config.project:
        raise ObjectiveRunError("runner project is not configured")
    if not config.repo:
        raise ObjectiveRunError("VCS repo is not configured")
    if config.max_active < 1:
        raise ObjectiveRunError("max_active must be positive")
    if config.ready_buffer < 1:
        raise ObjectiveRunError("ready_buffer must be positive")
    if config.poll_interval < 1:
        raise ObjectiveRunError("poll_interval must be positive")
    if config.max_cycles < 1:
        raise ObjectiveRunError("max_cycles must be positive")
    with work_ledger.connect_existing(config.ledger):
        pass
    return "\n".join(
        (
            "Objective run controller configuration check",
            "",
            "Symphony boundary: retained as runner/execution engine",
            f"Config: {config.config_path}",
            f"Ledger: {config.ledger}",
            f"Runner adapter: Linear/Symphony project {config.project}",
            f"VCS adapter: GitHub repo {config.repo}",
            f"Max active / ready buffer: {config.max_active} / {config.ready_buffer}",
            f"Repair mode: {config.repair_mode}",
            "Result: local objective controller configuration is valid",
        )
    )


def load_project_config(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, Mapping):
        raise ObjectiveRunError(f"controller config must be a mapping: {path}")
    return payload


def mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def first_text(*values: object) -> str:
    for value in values:
        text = first_optional_text(value)
        if text:
            return text
    return ""


def first_optional_text(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def first_int(*values: object) -> int:
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    raise ObjectiveRunError("missing integer default")


def render_results(
    results: Sequence[CycleResult],
    output_format: OutputFormat,
) -> str:
    if output_format == "json":
        return json.dumps(
            [cycle_payload(result) for result in results],
            indent=2,
            sort_keys=True,
        )
    lines: list[str] = []
    for result in results:
        if lines:
            lines.append("")
        lines.append(f"Objective run controller cycle {result.cycle}")
        interesting_actions = tuple(
            action for action in result.runner_actions if action.action != "noop"
        )
        noop_count = len(result.runner_actions) - len(interesting_actions)
        if interesting_actions:
            lines.append("Runner observation:")
            lines.extend(
                f"- {format_runner_action(action)}" for action in interesting_actions
            )
            if noop_count:
                lines.append(f"- {noop_count} mirrored runner states already current")
        elif noop_count:
            lines.append(
                f"Runner observation: {noop_count} mirrored runner states already current"
            )
        else:
            lines.append("Runner observation: no mirrored state changes")
        lines.append("Commands:")
        for command_result in result.command_results:
            status = "ok" if command_result.returncode == 0 else "failed"
            lines.append(
                f"- {command_result.kind} | {status} | "
                f"{shlex.join(command_result.argv)}"
            )
        if result.blockers:
            lines.append("Safety blockers:")
            lines.extend(f"- {blocker}" for blocker in result.blockers)
        if result.stopped:
            lines.append(f"Stopped: {result.stop_reason}")
        else:
            lines.append("Stopped: no")
    return "\n".join(lines)


def format_runner_action(action: RunnerStateAction) -> str:
    target = action.target_status or "-"
    return (
        f"{action.ticket_id} | {action.action} | {action.from_status} -> "
        f"{target} | Linear {action.linear_state} | {action.reason}"
    )


def cycle_payload(result: CycleResult) -> dict[str, object]:
    return {
        "cycle": result.cycle,
        "commands": [
            {
                "kind": item.kind,
                "argv": list(item.argv),
                "returncode": item.returncode,
                "stdout": item.stdout,
                "stderr": item.stderr,
            }
            for item in result.command_results
        ],
        "runner_actions": [
            {
                "ticket_id": item.ticket_id,
                "linear_identifier": item.linear_identifier,
                "from_status": item.from_status,
                "linear_state": item.linear_state,
                "action": item.action,
                "target_status": item.target_status,
                "reason": item.reason,
            }
            for item in result.runner_actions
        ],
        "blockers": list(result.blockers),
        "stopped": result.stopped,
        "stop_reason": result.stop_reason,
    }


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python
"""Check and run the local Silver database bootstrap sequence."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = ROOT / "scripts"


class BootstrapError(RuntimeError):
    """Raised when the bootstrap sequence cannot continue."""


@dataclass(frozen=True)
class BootstrapStep:
    name: str
    script: Path
    check_label: str
    apply_label: str

    @property
    def display_path(self) -> str:
        return str(self.script.relative_to(ROOT))


STEPS: tuple[BootstrapStep, ...] = (
    BootstrapStep(
        name="migrations",
        script=SCRIPT_DIR / "apply_migrations.py",
        check_label="Check migrations",
        apply_label="Apply migrations",
    ),
    BootstrapStep(
        name="available_at policies",
        script=SCRIPT_DIR / "seed_available_at_policies.py",
        check_label="Check available_at policies",
        apply_label="Seed available_at policies",
    ),
    BootstrapStep(
        name="reference data",
        script=SCRIPT_DIR / "seed_reference_data.py",
        check_label="Check reference data",
        apply_label="Seed reference data",
    ),
    BootstrapStep(
        name="trading calendar",
        script=SCRIPT_DIR / "seed_trading_calendar.py",
        check_label="Check trading calendar",
        apply_label="Seed trading calendar",
    ),
)


Runner = Callable[[list[str]], subprocess.CompletedProcess]


def subprocess_run(command: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(command, check=False)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate the full bootstrap chain without connecting to Postgres",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection URL; defaults to DATABASE_URL",
    )
    return parser.parse_args(argv)


def build_step_command(
    step: BootstrapStep,
    *,
    check: bool,
    database_url: str | None,
) -> list[str]:
    command = [sys.executable, str(step.script)]
    if check:
        command.append("--check")
        return command

    if not database_url:
        raise BootstrapError("DATABASE_URL is required unless --check is used")
    command.extend(["--database-url", database_url])
    return command


def run_bootstrap(
    *,
    check: bool,
    database_url: str | None,
    runner: Runner = subprocess_run,
) -> None:
    if not check and not database_url:
        raise BootstrapError("DATABASE_URL is required unless --check is used")

    total = len(STEPS)
    for index, step in enumerate(STEPS, start=1):
        action = step.check_label if check else step.apply_label
        print(f"[{index}/{total}] {action}: {step.display_path}", flush=True)
        result = runner(
            build_step_command(step, check=check, database_url=database_url)
        )
        if result.returncode != 0:
            raise BootstrapError(
                f"{step.display_path} failed with exit code {result.returncode}"
            )
        print(f"[{index}/{total}] OK: {step.name}", flush=True)


def main(
    argv: Sequence[str] | None = None,
    *,
    runner: Runner = subprocess_run,
) -> int:
    args = parse_args(argv)
    try:
        run_bootstrap(check=args.check, database_url=args.database_url, runner=runner)
    except BootstrapError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    mode = "check" if args.check else "apply"
    print(f"OK: local database bootstrap {mode} completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

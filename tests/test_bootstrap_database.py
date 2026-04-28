from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP_SCRIPT = ROOT / "scripts" / "bootstrap_database.py"
STEP_SCRIPT_NAMES = [
    "apply_migrations.py",
    "seed_available_at_policies.py",
    "seed_reference_data.py",
    "seed_trading_calendar.py",
]


def load_bootstrap_module():
    spec = importlib.util.spec_from_file_location(
        "bootstrap_database",
        BOOTSTRAP_SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


bootstrap_database = load_bootstrap_module()


def test_check_mode_invokes_all_steps_in_order_without_database_url(
    capsys: pytest.CaptureFixture[str],
) -> None:
    commands, runner = _recording_runner()

    bootstrap_database.run_bootstrap(
        check=True,
        database_url=None,
        runner=runner,
    )

    assert _script_names(commands) == STEP_SCRIPT_NAMES
    assert all(command[2:] == ["--check"] for command in commands)

    stdout = capsys.readouterr().out
    assert "[1/4] Check migrations: scripts/apply_migrations.py" in stdout
    assert "[4/4] OK: trading calendar" in stdout


def test_apply_mode_passes_database_url_to_all_steps_in_order() -> None:
    commands, runner = _recording_runner()
    database_url = "postgresql://localhost:5432/silver"

    bootstrap_database.run_bootstrap(
        check=False,
        database_url=database_url,
        runner=runner,
    )

    assert _script_names(commands) == STEP_SCRIPT_NAMES
    assert all(
        command[2:] == ["--database-url", database_url] for command in commands
    )


def test_apply_mode_requires_database_url_before_running_steps() -> None:
    commands, runner = _recording_runner()

    with pytest.raises(bootstrap_database.BootstrapError, match="DATABASE_URL"):
        bootstrap_database.run_bootstrap(
            check=False,
            database_url=None,
            runner=runner,
        )

    assert commands == []


def test_bootstrap_fails_fast_when_a_step_fails() -> None:
    commands, runner = _recording_runner([0, 9, 0, 0])

    with pytest.raises(bootstrap_database.BootstrapError, match="exit code 9"):
        bootstrap_database.run_bootstrap(
            check=True,
            database_url=None,
            runner=runner,
        )

    assert _script_names(commands) == STEP_SCRIPT_NAMES[:2]


def test_main_apply_uses_database_url_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands, runner = _recording_runner()
    database_url = "postgresql://localhost:5432/silver"
    monkeypatch.setenv("DATABASE_URL", database_url)

    assert bootstrap_database.main([], runner=runner) == 0

    assert all(command[2:] == ["--database-url", database_url] for command in commands)


def test_main_apply_reports_missing_database_url(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    assert bootstrap_database.main([]) == 1

    stderr = capsys.readouterr().err
    assert "DATABASE_URL is required unless --check is used" in stderr


def _recording_runner(
    returncodes: list[int] | None = None,
) -> tuple[list[list[str]], bootstrap_database.Runner]:
    commands: list[list[str]] = []
    remaining_returncodes = list(returncodes or [])

    def run(command: list[str]) -> subprocess.CompletedProcess:
        commands.append(command)
        returncode = remaining_returncodes.pop(0) if remaining_returncodes else 0
        return subprocess.CompletedProcess(command, returncode)

    return commands, run


def _script_names(commands: list[list[str]]) -> list[str]:
    return [Path(command[1]).name for command in commands]

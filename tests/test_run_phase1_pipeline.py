from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
PIPELINE_SCRIPT = ROOT / "scripts" / "run_phase1_pipeline.py"
STEP_SCRIPT_NAMES = [
    "bootstrap_database.py",
    "ingest_fmp_prices.py",
    "materialize_forward_labels.py",
    "materialize_momentum_12_1.py",
    "run_falsifier.py",
]


def load_pipeline_module():
    spec = importlib.util.spec_from_file_location(
        "run_phase1_pipeline",
        PIPELINE_SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


run_phase1_pipeline = load_pipeline_module()


def test_check_mode_invokes_all_step_checks_without_live_prerequisites(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("FMP_API_KEY", raising=False)
    commands, runner = _recording_runner()

    exit_code = run_phase1_pipeline.main(
        [
            "--check",
            "--start-date",
            "2024-01-02",
            "--end-date",
            "2024-02-02",
        ],
        runner=runner,
        psql_finder=lambda _: None,
    )

    assert exit_code == 0
    assert _script_names(commands) == STEP_SCRIPT_NAMES
    assert all("--check" in command for command in commands)
    assert all("--database-url" not in command for command in commands)
    assert commands[1][2:] == [
        "--universe",
        "falsifier_seed",
        "--start-date",
        "2024-01-02",
        "--end-date",
        "2024-02-02",
        "--check",
    ]
    assert commands[-1][2:] == [
        "--horizon",
        "63",
        "--universe",
        "falsifier_seed",
        "--output-path",
        "reports/falsifier/week_1_momentum.md",
        "--check",
    ]

    stdout = capsys.readouterr().out
    assert "Phase 1 pipeline check completed." in stdout
    assert "Output report path: reports/falsifier/week_1_momentum.md" in stdout
    assert "Next action: set DATABASE_URL and FMP_API_KEY" in stdout


def test_live_run_requires_database_url_before_running_steps(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("FMP_API_KEY", "fmp-secret")
    commands, runner = _recording_runner()

    exit_code = run_phase1_pipeline.main(
        [],
        runner=runner,
        psql_finder=lambda _: "/usr/bin/psql",
    )

    assert exit_code == 1
    assert commands == []
    assert "DATABASE_URL is required unless --check is used" in capsys.readouterr().err


def test_live_run_requires_psql_before_running_steps(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("FMP_API_KEY", "fmp-secret")
    commands, runner = _recording_runner()

    exit_code = run_phase1_pipeline.main(
        ["--database-url", "postgresql://localhost/silver"],
        runner=runner,
        psql_finder=lambda _: None,
    )

    assert exit_code == 1
    assert commands == []
    assert "psql is required" in capsys.readouterr().err


def test_live_run_requires_fmp_api_key_unless_ingest_is_skipped(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("FMP_API_KEY", raising=False)
    commands, runner = _recording_runner()

    exit_code = run_phase1_pipeline.main(
        ["--database-url", "postgresql://localhost/silver"],
        runner=runner,
        psql_finder=lambda _: "/usr/bin/psql",
    )

    assert exit_code == 1
    assert commands == []
    assert "FMP_API_KEY is required unless --check or --skip-ingest is used" in (
        capsys.readouterr().err
    )


def test_skip_ingest_runs_remaining_live_steps_without_fmp_key(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("FMP_API_KEY", raising=False)
    database_url = "postgresql://localhost/silver"
    commands, runner = _recording_runner()

    exit_code = run_phase1_pipeline.main(
        ["--database-url", database_url, "--skip-ingest"],
        runner=runner,
        psql_finder=lambda _: "/usr/bin/psql",
    )

    assert exit_code == 0
    assert _script_names(commands) == [
        "bootstrap_database.py",
        "materialize_forward_labels.py",
        "materialize_momentum_12_1.py",
        "run_falsifier.py",
    ]
    assert all("--database-url" in command for command in commands)
    assert all(database_url in command for command in commands)

    stdout = capsys.readouterr().out
    assert "[2/5] Skip FMP price ingest" in stdout
    assert "Skipped steps:\n- FMP price ingest: --skip-ingest requested" in stdout


def test_step_failure_redacts_database_url_and_fmp_key(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = "postgresql://user:db-secret@localhost/silver"
    api_key = "fmp-secret"
    monkeypatch.setenv("FMP_API_KEY", api_key)
    commands, runner = _recording_runner(
        returncodes=[0, 1, 0, 0, 0],
        stderrs=[
            "",
            f"error: could not load {database_url} with key {api_key}",
            "",
            "",
            "",
        ],
    )

    exit_code = run_phase1_pipeline.main(
        ["--database-url", database_url],
        runner=runner,
        psql_finder=lambda _: "/usr/bin/psql",
    )

    assert exit_code == 1
    assert _script_names(commands) == STEP_SCRIPT_NAMES[:2]
    stderr = capsys.readouterr().err
    assert "FMP price ingest failed with exit code 1" in stderr
    assert "[REDACTED]" in stderr
    assert database_url not in stderr
    assert api_key not in stderr


def test_invalid_horizon_fails_before_running_steps(
    capsys: pytest.CaptureFixture[str],
) -> None:
    commands, runner = _recording_runner()

    exit_code = run_phase1_pipeline.main(
        ["--check", "--horizon", "999"],
        runner=runner,
        psql_finder=lambda _: None,
    )

    assert exit_code == 1
    assert commands == []
    assert "horizon must be one of" in capsys.readouterr().err


def _recording_runner(
    *,
    returncodes: list[int] | None = None,
    stdouts: list[str] | None = None,
    stderrs: list[str] | None = None,
) -> tuple[list[list[str]], run_phase1_pipeline.Runner]:
    commands: list[list[str]] = []
    remaining_returncodes = list(returncodes or [])
    remaining_stdouts = list(stdouts or [])
    remaining_stderrs = list(stderrs or [])

    def run(command: list[str]) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        returncode = remaining_returncodes.pop(0) if remaining_returncodes else 0
        stdout = remaining_stdouts.pop(0) if remaining_stdouts else ""
        stderr = remaining_stderrs.pop(0) if remaining_stderrs else ""
        return subprocess.CompletedProcess(command, returncode, stdout, stderr)

    return commands, run


def _script_names(commands: list[list[str]]) -> list[str]:
    return [Path(command[1]).name for command in commands]

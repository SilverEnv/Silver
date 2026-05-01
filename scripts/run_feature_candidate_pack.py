#!/usr/bin/env python
"""Materialize and evaluate Silver's first numeric feature-candidate pack."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from silver.features import (  # noqa: E402
    DEFAULT_CANDIDATE_CONFIG_PATH,
    FeatureCandidate,
    FeatureStoreError,
    feature_candidates_for_keys,
)
from silver.hypotheses import (  # noqa: E402
    HypothesisCreate,
    HypothesisRegistryError,
    HypothesisRepository,
)
from silver.reference.seed_data import FALSIFIER_UNIVERSE_NAME  # noqa: E402


DEFAULT_OUTPUT_DIR = ROOT / "reports" / "falsifier" / "candidate_pack"
BACKTEST_RUN_ID_RE = re.compile(r"\bbacktest_run_id=(?P<id>\d+)\b")


@dataclass(frozen=True, slots=True)
class CandidatePackResult:
    candidate_key: str
    feature_name: str
    selection_direction: str
    backtest_run_id: int
    evaluation_status: str
    failure_reason: str | None
    report_path: Path


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate candidate-pack configuration without connecting to Postgres",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection URL; defaults to DATABASE_URL",
    )
    parser.add_argument(
        "--universe",
        default=FALSIFIER_UNIVERSE_NAME,
        help=f"universe name to evaluate; defaults to {FALSIFIER_UNIVERSE_NAME}",
    )
    parser.add_argument(
        "--horizon",
        type=int,
        default=63,
        help="forward-return horizon in trading sessions; defaults to 63",
    )
    parser.add_argument(
        "--candidate",
        action="append",
        help="candidate key to run; repeat to choose several",
    )
    parser.add_argument(
        "--candidate-config",
        type=Path,
        default=DEFAULT_CANDIDATE_CONFIG_PATH,
        help="YAML feature-candidate definition file",
    )
    parser.add_argument(
        "--skip-materialize",
        action="store_true",
        help="evaluate existing feature_values without refreshing candidates first",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="directory for per-candidate falsifier reports",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        candidate_config_path = _resolve_candidate_config_path(args.candidate_config)
        candidates = feature_candidates_for_keys(
            args.candidate,
            config_path=candidate_config_path,
        )
        if args.check:
            print(
                "OK: feature candidate pack check passed for "
                + ", ".join(candidate.hypothesis_key for candidate in candidates)
            )
            return 0
        if not args.database_url:
            raise FeatureStoreError("DATABASE_URL is required unless --check is used")

        psycopg = _load_psycopg()
        with psycopg.connect(args.database_url) as connection:
            repository = HypothesisRepository(connection)
            results = []
            for candidate in candidates:
                try:
                    results.append(
                        run_candidate(
                            candidate,
                            repository=repository,
                            database_url=args.database_url,
                            universe=args.universe,
                            horizon=args.horizon,
                            output_dir=args.output_dir,
                            skip_materialize=args.skip_materialize,
                            candidate_config_path=candidate_config_path,
                        )
                    )
                except Exception:
                    connection.rollback()
                    raise
                connection.commit()
    except (FeatureStoreError, HypothesisRegistryError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - CLI must fail without tracebacks.
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    print(render_candidate_pack_results(results))
    return 0


def run_candidate(
    candidate: FeatureCandidate,
    *,
    repository: HypothesisRepository,
    database_url: str,
    universe: str,
    horizon: int,
    output_dir: Path,
    skip_materialize: bool,
    candidate_config_path: Path,
) -> CandidatePackResult:
    if not skip_materialize:
        _run_command(
            _materialize_command(
                candidate,
                universe=universe,
                candidate_config_path=candidate_config_path,
            ),
            database_url=database_url,
        )

    repository.upsert_hypothesis(
        candidate_hypothesis(candidate, universe=universe, horizon=horizon)
    )
    report_path = output_dir / f"{candidate.hypothesis_key}_h{horizon}.md"
    stdout = _run_command(
        _falsifier_command(
            candidate,
            universe=universe,
            horizon=horizon,
            output_path=report_path,
        ),
        database_url=database_url,
    )
    backtest_run_id = parse_backtest_run_id(stdout)
    evaluation = repository.record_backtest_evaluation(
        hypothesis_key=candidate.hypothesis_key,
        backtest_run_id=backtest_run_id,
        notes="feature candidate pack v0 evaluation",
    )
    return CandidatePackResult(
        candidate_key=candidate.hypothesis_key,
        feature_name=candidate.signal_name,
        selection_direction=candidate.selection_direction,
        backtest_run_id=evaluation.backtest_run_id,
        evaluation_status=evaluation.evaluation_status,
        failure_reason=evaluation.failure_reason,
        report_path=report_path,
    )


def candidate_hypothesis(
    candidate: FeatureCandidate,
    *,
    universe: str,
    horizon: int,
) -> HypothesisCreate:
    return HypothesisCreate(
        hypothesis_key=candidate.hypothesis_key,
        name=candidate.name,
        thesis=candidate.thesis,
        signal_name=candidate.signal_name,
        mechanism=candidate.mechanism,
        universe_name=universe,
        horizon_days=horizon,
        target_kind="raw_return",
        status="proposed",
        metadata={
            "candidate_pack": candidate.candidate_pack_key,
            "feature": candidate.signal_name,
            "selection_direction": candidate.selection_direction,
        },
    )


def parse_backtest_run_id(stdout: str) -> int:
    match = BACKTEST_RUN_ID_RE.search(stdout)
    if match is None:
        raise FeatureStoreError("falsifier output did not include backtest_run_id")
    return int(match.group("id"))


def render_candidate_pack_results(results: Sequence[CandidatePackResult]) -> str:
    if not results:
        return "No feature candidates evaluated."
    lines = [
        "candidate | feature | direction | verdict | backtest_run_id | report",
        "--- | --- | --- | --- | --- | ---",
    ]
    for result in results:
        verdict = result.evaluation_status
        if result.failure_reason:
            verdict = f"{verdict} ({result.failure_reason})"
        lines.append(
            " | ".join(
                (
                    result.candidate_key,
                    result.feature_name,
                    result.selection_direction,
                    verdict,
                    str(result.backtest_run_id),
                    _display_path(result.report_path),
                )
            )
        )
    return "\n".join(lines)


def _materialize_command(
    candidate: FeatureCandidate,
    *,
    universe: str,
    candidate_config_path: Path,
) -> list[str]:
    return [
        sys.executable,
        str(ROOT / "scripts" / "materialize_feature_candidates.py"),
        "--universe",
        universe,
        "--candidate-config",
        str(candidate_config_path),
        "--candidate",
        candidate.hypothesis_key,
    ]


def _falsifier_command(
    candidate: FeatureCandidate,
    *,
    universe: str,
    horizon: int,
    output_path: Path,
) -> list[str]:
    command = [
        sys.executable,
        str(ROOT / "scripts" / "run_falsifier.py"),
        "--strategy",
        candidate.signal_name,
        "--horizon",
        str(horizon),
        "--universe",
        universe,
        "--output-path",
        str(output_path),
    ]
    if candidate.selection_direction != "high":
        command.extend(["--selection-direction", candidate.selection_direction])
    return command


def _run_command(command: Sequence[str], *, database_url: str) -> str:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    result = subprocess.run(
        list(command),
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        stdout = result.stdout.strip()
        detail = stderr or stdout or f"exit code {result.returncode}"
        raise FeatureStoreError(f"candidate-pack command failed: {detail}")
    return result.stdout


def _load_psycopg() -> object:
    try:
        import psycopg
    except ImportError as exc:
        raise FeatureStoreError(
            "psycopg is required for candidate-pack evaluation; run `uv sync`"
        ) from exc
    return psycopg


def _resolve_candidate_config_path(path: Path) -> Path:
    candidate_path = path.expanduser()
    if candidate_path.is_absolute():
        return candidate_path
    return (Path.cwd() / candidate_path).resolve()


def _display_path(path: Path) -> str:
    resolved = path if path.is_absolute() else ROOT / path
    try:
        return str(resolved.resolve().relative_to(ROOT))
    except ValueError:
        return str(resolved)


if __name__ == "__main__":
    raise SystemExit(main())

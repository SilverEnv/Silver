#!/usr/bin/env python
"""Check persisted falsifier input coverage before running the falsifier."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Sequence


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from silver.analytics.falsifier_diagnostics import (  # noqa: E402
    FalsifierDiagnosticsError,
    load_falsifier_input_diagnostics,
    render_falsifier_input_diagnostics,
)
from silver.features.momentum_12_1 import FEATURE_NAME  # noqa: E402
from silver.reference.seed_data import (  # noqa: E402
    DEFAULT_CONFIG_PATH as DEFAULT_REFERENCE_CONFIG_PATH,
)
from silver.reference.seed_data import FALSIFIER_UNIVERSE_NAME, load_seed_file  # noqa: E402
from silver.time.trading_calendar import (  # noqa: E402
    CANONICAL_HORIZONS,
    DEFAULT_SEED_PATH as DEFAULT_TRADING_CALENDAR_SEED_PATH,
)
from silver.time.trading_calendar import TradingCalendar, load_seed_csv  # noqa: E402


class CheckFalsifierInputsError(RuntimeError):
    """Raised when the diagnostics CLI cannot complete."""


class PsqlJsonClient:
    """Tiny psql-backed JSON reader for persisted Silver diagnostics."""

    def __init__(self, *, database_url: str, psql_path: str | None = None) -> None:
        self._database_url = database_url
        self._psql_path = psql_path or shutil.which("psql")
        if self._psql_path is None:
            raise CheckFalsifierInputsError(
                "psql is required to read persisted falsifier inputs"
            )

    def fetch_json(self, sql: str) -> Any:
        result = subprocess.run(
            [
                self._psql_path,
                "-X",
                "-v",
                "ON_ERROR_STOP=1",
                "-q",
                "-t",
                "-A",
                "-d",
                self._database_url,
            ],
            input=sql,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            stderr = result.stderr.replace(self._database_url, "[DATABASE_URL]").strip()
            detail = f": {stderr}" if stderr else ""
            raise CheckFalsifierInputsError(
                "psql failed while reading falsifier input diagnostics"
                f"{detail}. If the schema is missing, run "
                "`python scripts/bootstrap_database.py` first."
            )
        output = result.stdout.strip()
        if not output:
            raise CheckFalsifierInputsError("psql returned no JSON output")
        try:
            return json.loads(output)
        except json.JSONDecodeError as exc:
            raise CheckFalsifierInputsError("psql returned invalid JSON") from exc


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--universe",
        default=FALSIFIER_UNIVERSE_NAME,
        help=f"point-in-time universe name; defaults to {FALSIFIER_UNIVERSE_NAME}",
    )
    parser.add_argument(
        "--horizon",
        type=int,
        default=63,
        help="forward-return horizon in trading sessions",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection URL; defaults to DATABASE_URL",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate CLI/config/query shape without connecting to Postgres",
    )
    parser.add_argument(
        "--psql-path",
        help="path to psql; defaults to the first psql on PATH",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        _validate_args(args)
        if args.check:
            run_check(args)
            return 0
        return run_diagnostics(args)
    except (CheckFalsifierInputsError, FalsifierDiagnosticsError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def run_check(args: argparse.Namespace) -> None:
    """Validate offline diagnostics prerequisites."""

    seed_config = load_seed_file(DEFAULT_REFERENCE_CONFIG_PATH)
    if args.universe not in {
        membership.universe_name for membership in seed_config.universe_memberships
    }:
        raise CheckFalsifierInputsError(
            f"universe `{args.universe}` is not present in "
            f"{DEFAULT_REFERENCE_CONFIG_PATH.relative_to(ROOT)}"
        )

    calendar_rows = load_seed_csv(DEFAULT_TRADING_CALENDAR_SEED_PATH)
    TradingCalendar(calendar_rows)
    print(
        "OK: falsifier input diagnostics check passed for "
        f"--universe {args.universe} --horizon {args.horizon}"
    )


def run_diagnostics(args: argparse.Namespace) -> int:
    if not args.database_url:
        raise CheckFalsifierInputsError(
            "DATABASE_URL is required unless --check is used. Run "
            "`python scripts/bootstrap_database.py` after setting DATABASE_URL, "
            "then rerun the diagnostics command."
        )

    client = PsqlJsonClient(database_url=args.database_url, psql_path=args.psql_path)
    diagnostics = load_falsifier_input_diagnostics(
        client,
        universe=args.universe,
        horizon=args.horizon,
        feature_name=FEATURE_NAME,
    )
    print(render_falsifier_input_diagnostics(diagnostics), end="")
    return 0 if diagnostics.is_sufficient else 1


def _validate_args(args: argparse.Namespace) -> None:
    if args.horizon not in CANONICAL_HORIZONS:
        allowed = ", ".join(str(horizon) for horizon in CANONICAL_HORIZONS)
        raise CheckFalsifierInputsError(
            f"horizon must be one of {allowed}; got {args.horizon}"
        )
    if not isinstance(args.universe, str) or not args.universe.strip():
        raise CheckFalsifierInputsError("universe must be a non-empty string")


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python
"""Ingest selected FMP normalized fundamentals for a persisted Silver universe."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from silver.ingest import (  # noqa: E402
    FmpFundamentalsIngestError,
    RawVault,
    ingest_fmp_fundamentals,
)
from silver.ingest.fmp_fundamentals import (  # noqa: E402
    ALL_PERIOD_TYPES,
    ALL_STATEMENT_TYPES,
    DEFAULT_LOOKBACK_START_YEAR,
    DEFAULT_STATEMENT_LIMIT,
)
from silver.reference.seed_data import (  # noqa: E402
    DEFAULT_CONFIG_PATH,
    FALSIFIER_UNIVERSE_NAME,
    SeedValidationError,
    load_seed_file,
)
from silver.sources.fmp import FMPClient, FMPClientError  # noqa: E402


class CommandError(RuntimeError):
    """Raised for CLI-level configuration and connection failures."""


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--universe",
        default=FALSIFIER_UNIVERSE_NAME,
        help=f"universe name to ingest; defaults to {FALSIFIER_UNIVERSE_NAME}",
    )
    parser.add_argument(
        "--ticker",
        action="append",
        dest="tickers",
        help="ticker to ingest; may be repeated; defaults to all universe tickers",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int_arg,
        help="maximum number of selected tickers to ingest",
    )
    parser.add_argument(
        "--statement",
        action="append",
        dest="statements",
        help="statement to ingest: income, cash-flow, or both; may be repeated",
    )
    parser.add_argument(
        "--period",
        action="append",
        dest="periods",
        help="period to ingest: annual, quarterly, or both; may be repeated",
    )
    parser.add_argument(
        "--lookback-start-year",
        type=_year_arg,
        default=DEFAULT_LOOKBACK_START_YEAR,
        help=f"first fiscal year to normalize; default {DEFAULT_LOOKBACK_START_YEAR}",
    )
    parser.add_argument(
        "--statement-limit",
        type=_positive_int_arg,
        default=DEFAULT_STATEMENT_LIMIT,
        help=f"FMP statement row limit per request; default {DEFAULT_STATEMENT_LIMIT}",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection URL; defaults to DATABASE_URL",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.2,
        help="pause between FMP requests; default is 0.2 seconds",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="read persisted universe membership and print the planned ingest",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate local seed config and CLI selection without DB or FMP access",
    )
    parser.add_argument(
        "--seed-config-path",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="path to reference seed config for --check",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        statements = expand_statements(args.statements)
        periods = expand_periods(args.periods)
        if args.check:
            result = check_config(
                universe=args.universe,
                tickers=args.tickers,
                limit=args.limit,
                statements=statements,
                periods=periods,
                seed_config_path=args.seed_config_path,
            )
            print(result)
            return 0

        if not args.database_url:
            raise CommandError(
                "DATABASE_URL is required; pass --database-url or set DATABASE_URL"
            )
        if not args.dry_run and not os.environ.get("FMP_API_KEY"):
            raise CommandError(
                "FMP_API_KEY is required unless --check or --dry-run is used"
            )

        connection = connect_database(args.database_url)
        try:
            client = None
            if not args.dry_run:
                client = FMPClient(raw_vault=RawVault(connection))
            result = ingest_fmp_fundamentals(
                connection=connection,
                client=client,
                universe=args.universe,
                tickers=args.tickers,
                limit=args.limit,
                statement_types=statements,
                period_types=periods,
                lookback_start_year=args.lookback_start_year,
                statement_limit=args.statement_limit,
                code_git_sha=code_git_sha(),
                dry_run=args.dry_run,
                sleep_seconds=args.sleep_seconds,
            )
        finally:
            close = getattr(connection, "close", None)
            if close is not None:
                close()
    except (
        CommandError,
        FmpFundamentalsIngestError,
        FMPClientError,
        SeedValidationError,
    ) as exc:
        print(f"error: {redact(str(exc), args.database_url)}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - CLI must fail without traces/secrets.
        print(f"error: {redact(str(exc), args.database_url)}", file=sys.stderr)
        return 1

    print(format_result(result))
    return 0


def check_config(
    *,
    universe: str,
    tickers: Sequence[str] | None,
    limit: int | None,
    statements: Sequence[str],
    periods: Sequence[str],
    seed_config_path: Path,
) -> str:
    seed_config = load_seed_file(seed_config_path)
    memberships = tuple(
        membership
        for membership in seed_config.universe_memberships
        if membership.universe_name == universe
    )
    if not memberships:
        raise CommandError(f"seed config has no memberships for universe {universe}")

    selected_tickers = tuple(sorted({membership.ticker for membership in memberships}))
    if tickers:
        requested = tuple(sorted({_ticker(ticker) for ticker in tickers}))
        missing = set(requested) - set(selected_tickers)
        if missing:
            raise CommandError(
                f"selected ticker(s) are not in seed universe: "
                f"{', '.join(sorted(missing))}"
            )
        selected_tickers = requested
    if limit is not None:
        selected_tickers = selected_tickers[:limit]

    planned_requests = len(selected_tickers) * len(statements) * len(periods)
    return (
        "OK: checked FMP fundamentals ingest config for "
        f"{universe} with {len(selected_tickers)} seed ticker(s), "
        f"statements={','.join(statements)}, periods={','.join(periods)}, "
        f"planned_requests={planned_requests}"
    )


def expand_statements(values: Sequence[str] | None) -> tuple[str, ...]:
    if not values:
        return ALL_STATEMENT_TYPES
    expanded: list[str] = []
    for value in values:
        normalized = _label(value, "statement").lower().replace("-", "_")
        if normalized == "both":
            expanded.extend(ALL_STATEMENT_TYPES)
        elif normalized == "income":
            expanded.append("income_statement")
        elif normalized in {"cash_flow", "cashflow"}:
            expanded.append("cash_flow_statement")
        elif normalized in ALL_STATEMENT_TYPES:
            expanded.append(normalized)
        else:
            raise CommandError(
                "statement must be income, cash-flow, income_statement, "
                "cash_flow_statement, or both"
            )
    return tuple(dict.fromkeys(expanded))


def expand_periods(values: Sequence[str] | None) -> tuple[str, ...]:
    if not values:
        return ALL_PERIOD_TYPES
    expanded: list[str] = []
    for value in values:
        normalized = _label(value, "period").lower()
        if normalized == "both":
            expanded.extend(ALL_PERIOD_TYPES)
        elif normalized in {"annual", "fy"}:
            expanded.append("annual")
        elif normalized in {"quarter", "quarterly"}:
            expanded.append("quarterly")
        else:
            raise CommandError("period must be annual, quarterly, or both")
    return tuple(dict.fromkeys(expanded))


def connect_database(database_url: str) -> Any:
    try:
        import psycopg  # type: ignore[import-not-found]
    except ImportError as exc:
        raise CommandError(
            "psycopg is required to connect to Postgres; run uv sync"
        ) from exc

    try:
        return psycopg.connect(database_url)
    except Exception as exc:  # noqa: BLE001 - sanitize DB adapter details.
        raise CommandError(
            f"could not connect to Postgres: {type(exc).__name__}"
        ) from exc


def code_git_sha() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    return "unknown"


def format_result(result: Any) -> str:
    mode = "dry run" if result.dry_run else "ingested"
    summary = (
        f"OK: {mode} FMP fundamentals for {result.universe} "
        f"with {len(result.tickers)} ticker(s): {', '.join(result.tickers)}; "
        f"statements={','.join(result.statement_types)}, "
        f"periods={','.join(result.period_types)}, "
        f"planned_requests={result.planned_requests}"
    )
    if result.dry_run:
        return summary
    return (
        f"{summary}; raw_responses={result.raw_responses_captured}, "
        f"values_written={result.rows_written}, run_id={result.run_id}"
    )


def redact(message: str, database_url: str | None) -> str:
    redacted = message
    for secret in (database_url, os.environ.get("FMP_API_KEY")):
        if secret:
            redacted = redacted.replace(secret, "[REDACTED]")
    return redacted


def _ticker(value: object) -> str:
    return _label(value, "ticker").upper()


def _label(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CommandError(f"{name} must be a non-empty string")
    return value.strip()


def _positive_int_arg(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _year_arg(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a year") from exc
    if parsed < 1900 or parsed > 2100:
        raise argparse.ArgumentTypeError("must be between 1900 and 2100")
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())

"""Orchestrate FMP normalized fundamentals ingest through raw and normalized stores."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal

from silver.analytics import AnalyticsRunRepository
from silver.fundamentals import (
    FmpFundamentalValue,
    FmpStatementParseError,
    FundamentalPolicy,
    FundamentalValueRecord,
    FundamentalValueRepository,
    filing_available_at,
    parse_fmp_cash_flow_statement,
    parse_fmp_income_statement,
)
from silver.reference import UniverseMember, UniverseMembershipRepository
from silver.sources.fmp import FMPClient, FMPHTTPError
from silver.time.trading_calendar import TradingCalendar


FMP_FUNDAMENTALS_RUN_KIND = "fmp_fundamentals_normalization"
FMP_FUNDAMENTALS_NORMALIZATION_VERSION = "fmp_fundamentals_v0"
DEFAULT_LOOKBACK_START_YEAR = 2014
DEFAULT_STATEMENT_LIMIT = 120
StatementType = Literal["income_statement", "cash_flow_statement"]
PeriodType = Literal["annual", "quarterly"]
ALL_STATEMENT_TYPES: tuple[StatementType, ...] = (
    "income_statement",
    "cash_flow_statement",
)
ALL_PERIOD_TYPES: tuple[PeriodType, ...] = ("annual", "quarterly")


class FmpFundamentalsIngestError(RuntimeError):
    """Raised when FMP fundamentals ingest cannot complete safely."""


@dataclass(frozen=True, slots=True)
class FmpFundamentalsMember:
    """One universe security to ingest from FMP normalized statements."""

    security_id: int
    ticker: str
    universe_name: str
    valid_from: date
    valid_to: date | None


@dataclass(frozen=True, slots=True)
class FmpStatementIngestResult:
    """Per-response ingest summary."""

    ticker: str
    statement_type: StatementType
    period_type: PeriodType
    raw_object_id: int
    raw_inserted: bool
    http_status: int
    rows_parsed: int
    values_parsed: int


@dataclass(frozen=True, slots=True)
class FmpFundamentalsIngestResult:
    """Summary of an FMP normalized fundamentals ingest run."""

    universe: str
    tickers: tuple[str, ...]
    statement_types: tuple[StatementType, ...]
    period_types: tuple[PeriodType, ...]
    lookback_start_year: int
    statement_limit: int
    dry_run: bool
    run_id: int | None
    policy_versions: Mapping[str, int]
    statement_results: tuple[FmpStatementIngestResult, ...] = ()
    rows_written: int = 0

    @property
    def planned_requests(self) -> int:
        return len(self.tickers) * len(self.statement_types) * len(self.period_types)

    @property
    def raw_responses_captured(self) -> int:
        return len(self.statement_results)

    @property
    def values_parsed(self) -> int:
        return sum(result.values_parsed for result in self.statement_results)


def ingest_fmp_fundamentals(
    *,
    connection: Any,
    client: FMPClient | None,
    universe: str,
    code_git_sha: str,
    tickers: Sequence[str] | None = None,
    limit: int | None = None,
    statement_types: Sequence[str] = ALL_STATEMENT_TYPES,
    period_types: Sequence[str] = ALL_PERIOD_TYPES,
    lookback_start_year: int = DEFAULT_LOOKBACK_START_YEAR,
    statement_limit: int = DEFAULT_STATEMENT_LIMIT,
    dry_run: bool = False,
    sleep_seconds: float = 0.2,
    sleep: Callable[[float], Any] = time.sleep,
) -> FmpFundamentalsIngestResult:
    """Ingest selected FMP normalized fundamentals for persisted universe members."""
    normalized_universe = _required_label(universe, "universe")
    normalized_tickers = _ticker_filter(tickers)
    normalized_limit = _optional_limit(limit)
    normalized_statement_types = _statement_types(statement_types)
    normalized_period_types = _period_types(period_types)
    normalized_start_year = _start_year(lookback_start_year)
    normalized_statement_limit = _positive_int(statement_limit, "statement_limit")
    normalized_sleep_seconds = _non_negative_number(sleep_seconds, "sleep_seconds")

    universe_repository = UniverseMembershipRepository(connection)
    all_members = universe_repository.list_members(normalized_universe)
    if not all_members:
        raise FmpFundamentalsIngestError(
            f"universe {normalized_universe} has no persisted members"
        )

    selected_members = _filter_members(all_members, normalized_tickers)
    if normalized_limit is not None:
        selected_members = selected_members[:normalized_limit]
    if not selected_members:
        raise FmpFundamentalsIngestError(
            f"universe {normalized_universe} has no members matching selection"
        )
    ingest_members = tuple(_ingest_member(member) for member in selected_members)

    repository = FundamentalValueRepository(connection)
    policies = repository.load_filing_policies()
    policy_versions = {name: policy.version for name, policy in sorted(policies.items())}
    if dry_run:
        return FmpFundamentalsIngestResult(
            universe=normalized_universe,
            tickers=tuple(member.ticker for member in ingest_members),
            statement_types=normalized_statement_types,
            period_types=normalized_period_types,
            lookback_start_year=normalized_start_year,
            statement_limit=normalized_statement_limit,
            dry_run=True,
            run_id=None,
            policy_versions=policy_versions,
        )
    if client is None:
        raise FmpFundamentalsIngestError("client is required unless dry_run is used")

    calendar = repository.load_trading_calendar()
    analytics_repository = AnalyticsRunRepository(connection)
    run_id: int | None = None

    try:
        run = analytics_repository.create_run(
            run_kind=FMP_FUNDAMENTALS_RUN_KIND,
            code_git_sha=code_git_sha,
            available_at_policy_versions=policy_versions,
            parameters={
                "source": "fmp",
                "universe": normalized_universe,
                "tickers": tuple(member.ticker for member in ingest_members),
                "statement_types": normalized_statement_types,
                "period_types": normalized_period_types,
                "lookback_start_year": normalized_start_year,
                "statement_limit": normalized_statement_limit,
                "normalization_version": FMP_FUNDAMENTALS_NORMALIZATION_VERSION,
                "selected_metrics": _selected_metrics(),
            },
            input_fingerprints={
                "universe_membership": [
                    _member_fingerprint(member) for member in selected_members
                ],
                "fmp_statement_requests": [
                    {
                        "ticker": member.ticker,
                        "statement_type": statement_type,
                        "period_type": period_type,
                        "statement_limit": normalized_statement_limit,
                    }
                    for member in ingest_members
                    for statement_type in normalized_statement_types
                    for period_type in normalized_period_types
                ],
            },
        )
        run_id = run.id
        _commit(connection)

        statement_results: list[FmpStatementIngestResult] = []
        records: list[FundamentalValueRecord] = []
        planned_requests = (
            len(ingest_members)
            * len(normalized_statement_types)
            * len(normalized_period_types)
        )
        request_index = 0
        for member in ingest_members:
            for statement_type in normalized_statement_types:
                for period_type in normalized_period_types:
                    request_index += 1
                    try:
                        raw_response = _fetch_statement(
                            client,
                            ticker=member.ticker,
                            statement_type=statement_type,
                            period_type=period_type,
                            statement_limit=normalized_statement_limit,
                        )
                    except FMPHTTPError:
                        _commit(connection)
                        raise
                    _commit(connection)

                    parsed = _parse_statement_values(
                        raw_response.body,
                        ticker=member.ticker,
                        statement_type=statement_type,
                        period_type=period_type,
                        lookback_start_year=normalized_start_year,
                    )
                    statement_results.append(
                        FmpStatementIngestResult(
                            ticker=member.ticker,
                            statement_type=statement_type,
                            period_type=period_type,
                            raw_object_id=(
                                raw_response.raw_vault_result.raw_object_id
                            ),
                            raw_inserted=raw_response.raw_vault_result.inserted,
                            http_status=raw_response.http_status,
                            rows_parsed=_period_rows(parsed),
                            values_parsed=len(parsed),
                        )
                    )
                    records.extend(
                        _records(
                            member=member,
                            values=parsed,
                            raw_object_id=(
                                raw_response.raw_vault_result.raw_object_id
                            ),
                            run_id=run_id,
                            policies=policies,
                            calendar=calendar,
                        )
                    )
                    if normalized_sleep_seconds and request_index < planned_requests:
                        sleep(normalized_sleep_seconds)

        write_result = repository.write_values(records)
        analytics_repository.finish_run(run_id, status="succeeded")
        _commit(connection)
    except Exception:
        _rollback(connection)
        if run_id is not None:
            try:
                analytics_repository.finish_run(run_id, status="failed")
                _commit(connection)
            except Exception:
                _rollback(connection)
        raise

    return FmpFundamentalsIngestResult(
        universe=normalized_universe,
        tickers=tuple(member.ticker for member in ingest_members),
        statement_types=normalized_statement_types,
        period_types=normalized_period_types,
        lookback_start_year=normalized_start_year,
        statement_limit=normalized_statement_limit,
        dry_run=False,
        run_id=run_id,
        policy_versions=policy_versions,
        statement_results=tuple(statement_results),
        rows_written=write_result.rows_written,
    )


def _fetch_statement(
    client: FMPClient,
    *,
    ticker: str,
    statement_type: StatementType,
    period_type: PeriodType,
    statement_limit: int,
) -> Any:
    fmp_period = "annual" if period_type == "annual" else "quarter"
    if statement_type == "income_statement":
        return client.fetch_income_statement(
            ticker,
            period=fmp_period,
            limit=statement_limit,
        )
    return client.fetch_cash_flow_statement(
        ticker,
        period=fmp_period,
        limit=statement_limit,
    )


def _parse_statement_values(
    body: bytes,
    *,
    ticker: str,
    statement_type: StatementType,
    period_type: PeriodType,
    lookback_start_year: int,
) -> tuple[FmpFundamentalValue, ...]:
    payload = _json_payload(body, ticker, statement_type, period_type)
    try:
        if statement_type == "income_statement":
            return parse_fmp_income_statement(
                payload,
                expected_symbol=ticker,
                period_type=period_type,
                lookback_start_year=lookback_start_year,
            )
        return parse_fmp_cash_flow_statement(
            payload,
            expected_symbol=ticker,
            period_type=period_type,
            lookback_start_year=lookback_start_year,
        )
    except FmpStatementParseError as exc:
        raise FmpFundamentalsIngestError(str(exc)) from exc


def _records(
    *,
    member: FmpFundamentalsMember,
    values: Sequence[FmpFundamentalValue],
    raw_object_id: int,
    run_id: int,
    policies: Mapping[str, FundamentalPolicy],
    calendar: TradingCalendar,
) -> tuple[FundamentalValueRecord, ...]:
    records: list[FundamentalValueRecord] = []
    for value in values:
        policy = _policy_for_period(value.period_type, policies)
        records.append(
            FundamentalValueRecord(
                security_id=member.security_id,
                period_end_date=value.period_end_date,
                fiscal_year=value.fiscal_year,
                fiscal_period=value.fiscal_period,
                period_type=value.period_type,
                statement_type=value.statement_type,
                metric_name=value.metric_name,
                metric_value=value.metric_value,
                currency=value.currency,
                source_system=value.source_system,
                source_field=value.source_field,
                raw_object_id=raw_object_id,
                accepted_at=value.accepted_at,
                filing_date=value.filing_date,
                available_at=filing_available_at(
                    value.accepted_at,
                    policy=policy,
                    calendar=calendar,
                ),
                available_at_policy_id=policy.id,
                normalized_by_run_id=run_id,
                metadata={
                    "normalization_version": FMP_FUNDAMENTALS_NORMALIZATION_VERSION,
                    "source_metadata": dict(value.source_metadata),
                },
            )
        )
    return tuple(records)


def _policy_for_period(
    period_type: PeriodType,
    policies: Mapping[str, FundamentalPolicy],
) -> FundamentalPolicy:
    policy_name = "sec_10k_filing" if period_type == "annual" else "sec_10q_filing"
    policy = policies.get(policy_name)
    if policy is None:
        raise FmpFundamentalsIngestError(f"missing policy {policy_name}")
    return policy


def _period_rows(values: Sequence[FmpFundamentalValue]) -> int:
    return len(
        {
            (
                value.symbol,
                value.period_end_date,
                value.period_type,
                value.statement_type,
            )
            for value in values
        }
    )


def _filter_members(
    members: Sequence[UniverseMember],
    tickers: frozenset[str] | None,
) -> tuple[UniverseMember, ...]:
    unique_members_by_security_id = {member.security_id: member for member in members}
    selected = tuple(
        sorted(
            unique_members_by_security_id.values(),
            key=lambda member: (member.ticker, member.security_id),
        )
    )
    if tickers is None:
        return selected

    selected_by_ticker = tuple(member for member in selected if member.ticker in tickers)
    found = {member.ticker for member in selected_by_ticker}
    missing = tickers - found
    if missing:
        raise FmpFundamentalsIngestError(
            "selected ticker(s) are not in universe "
            f"{', '.join(sorted(missing))}"
        )
    return selected_by_ticker


def _ingest_member(member: UniverseMember) -> FmpFundamentalsMember:
    return FmpFundamentalsMember(
        security_id=member.security_id,
        ticker=member.ticker,
        universe_name=member.universe_name,
        valid_from=member.valid_from,
        valid_to=member.valid_to,
    )


def _member_fingerprint(member: UniverseMember) -> dict[str, Any]:
    return {
        "security_id": member.security_id,
        "ticker": member.ticker,
        "universe_name": member.universe_name,
        "valid_from": member.valid_from.isoformat(),
        "valid_to": None if member.valid_to is None else member.valid_to.isoformat(),
    }


def _json_payload(
    body: bytes,
    ticker: str,
    statement_type: str,
    period_type: str,
) -> Any:
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise FmpFundamentalsIngestError(
            f"FMP {statement_type} {period_type} response for {ticker} "
            "was not valid JSON"
        ) from exc


def _selected_metrics() -> dict[str, tuple[str, ...]]:
    return {
        "income_statement": (
            "revenue",
            "gross_profit",
            "operating_income",
            "net_income",
            "diluted_weighted_average_shares",
        ),
        "cash_flow_statement": (
            "operating_cash_flow",
            "capital_expenditure",
            "free_cash_flow",
        ),
    }


def _statement_types(values: Sequence[str]) -> tuple[StatementType, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise FmpFundamentalsIngestError("statement_types must be a sequence")
    normalized = tuple(dict.fromkeys(_statement_type(value) for value in values))
    if not normalized:
        raise FmpFundamentalsIngestError("at least one statement type is required")
    return normalized


def _statement_type(value: object) -> StatementType:
    if not isinstance(value, str):
        raise FmpFundamentalsIngestError(
            "statement type must be income_statement or cash_flow_statement"
        )
    normalized = value.strip().lower().replace("-", "_")
    if normalized == "income":
        normalized = "income_statement"
    if normalized in {"cash_flow", "cashflow"}:
        normalized = "cash_flow_statement"
    if normalized not in ALL_STATEMENT_TYPES:
        raise FmpFundamentalsIngestError(
            "statement type must be income_statement or cash_flow_statement"
        )
    return normalized  # type: ignore[return-value]


def _period_types(values: Sequence[str]) -> tuple[PeriodType, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise FmpFundamentalsIngestError("period_types must be a sequence")
    normalized = tuple(dict.fromkeys(_period_type(value) for value in values))
    if not normalized:
        raise FmpFundamentalsIngestError("at least one period type is required")
    return normalized


def _period_type(value: object) -> PeriodType:
    if not isinstance(value, str):
        raise FmpFundamentalsIngestError("period type must be annual or quarterly")
    normalized = value.strip().lower()
    if normalized in {"annual", "fy"}:
        return "annual"
    if normalized in {"quarter", "quarterly"}:
        return "quarterly"
    raise FmpFundamentalsIngestError("period type must be annual or quarterly")


def _ticker_filter(value: Sequence[str] | None) -> frozenset[str] | None:
    if value is None:
        return None
    tickers = frozenset(_ticker(ticker) for ticker in value)
    if not tickers:
        raise FmpFundamentalsIngestError("tickers must not be empty when provided")
    return tickers


def _ticker(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FmpFundamentalsIngestError("ticker must be a non-empty string")
    return value.strip().upper()


def _required_label(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FmpFundamentalsIngestError(f"{name} must be a non-empty string")
    return value.strip()


def _start_year(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FmpFundamentalsIngestError("lookback_start_year must be an integer")
    if value < 1900 or value > 2100:
        raise FmpFundamentalsIngestError(
            "lookback_start_year must be between 1900 and 2100"
        )
    return value


def _optional_limit(value: int | None) -> int | None:
    if value is None:
        return None
    return _positive_int(value, "limit")


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FmpFundamentalsIngestError(f"{name} must be a positive integer")
    return value


def _non_negative_number(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FmpFundamentalsIngestError(f"{name} must be non-negative")
    normalized = float(value)
    if normalized < 0:
        raise FmpFundamentalsIngestError(f"{name} must be non-negative")
    return normalized


def _commit(connection: Any) -> None:
    commit = getattr(connection, "commit", None)
    if commit is not None:
        commit()


def _rollback(connection: Any) -> None:
    rollback = getattr(connection, "rollback", None)
    if rollback is not None:
        rollback()

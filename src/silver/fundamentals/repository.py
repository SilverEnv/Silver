"""Persistence helpers for normalized fundamental values."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from silver.time.trading_calendar import TradingCalendar, TradingCalendarRow


ANNUAL_FILING_POLICY_NAME = "sec_10k_filing"
QUARTERLY_FILING_POLICY_NAME = "sec_10q_filing"
DEFAULT_FILING_POLICY_VERSION = 1
SOURCE_SYSTEM = "fmp"


class FundamentalValuesError(ValueError):
    """Raised when fundamental values cannot be persisted safely."""


@dataclass(frozen=True, slots=True)
class FundamentalPolicy:
    """Available-at policy metadata for filing-derived fundamentals."""

    id: int
    name: str
    version: int
    rule: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class FundamentalValueRecord:
    """One normalized fundamental metric ready for persistence."""

    security_id: int
    period_end_date: date
    fiscal_year: int
    fiscal_period: str
    period_type: str
    statement_type: str
    metric_name: str
    metric_value: Decimal
    currency: str
    source_system: str
    source_field: str
    raw_object_id: int
    accepted_at: datetime
    filing_date: date
    available_at: datetime
    available_at_policy_id: int
    normalized_by_run_id: int
    metadata: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class FundamentalValueWriteResult:
    """Summary of a fundamental-values persistence call."""

    rows_seen: int
    rows_written: int


class FundamentalValueRepository:
    """Persist normalized metrics into ``silver.fundamental_values``."""

    def __init__(self, connection: Any):
        self._connection = connection

    def load_trading_calendar(self) -> TradingCalendar:
        """Load all seeded trading-calendar rows for availability arithmetic."""
        with _cursor(self._connection) as cursor:
            cursor.execute(_SELECT_TRADING_CALENDAR_SQL, {})
            rows = cursor.fetchall()
        return TradingCalendar(tuple(_calendar_row(row) for row in rows))

    def load_filing_policies(
        self,
        *,
        version: int = DEFAULT_FILING_POLICY_VERSION,
    ) -> dict[str, FundamentalPolicy]:
        """Load the 10-K and 10-Q available-at policies by version."""
        normalized_version = _positive_int(version, "version")
        names = (ANNUAL_FILING_POLICY_NAME, QUARTERLY_FILING_POLICY_NAME)
        with _cursor(self._connection) as cursor:
            cursor.execute(
                _SELECT_POLICIES_BY_NAME_VERSION_SQL,
                {"names": list(names), "version": normalized_version},
            )
            rows = cursor.fetchall()

        policies: dict[str, FundamentalPolicy] = {}
        for row in rows:
            policy = _policy_record(row)
            policies[policy.name] = policy
        missing = set(names) - set(policies)
        if missing:
            raise FundamentalValuesError(
                "available_at policy version "
                f"{normalized_version} missing for {', '.join(sorted(missing))}"
            )
        return policies

    def write_values(
        self,
        records: Sequence[FundamentalValueRecord],
    ) -> FundamentalValueWriteResult:
        """Upsert normalized fundamental values."""
        normalized_records = _validated_records(records)
        rows_written = 0
        for record in normalized_records:
            with _cursor(self._connection) as cursor:
                cursor.execute(_UPSERT_FUNDAMENTAL_VALUE_SQL, _record_params(record))
            rows_written += 1
        return FundamentalValueWriteResult(
            rows_seen=len(normalized_records),
            rows_written=rows_written,
        )


def filing_available_at(
    accepted_at: datetime,
    *,
    policy: FundamentalPolicy,
    calendar: TradingCalendar,
) -> datetime:
    """Compute conservative next-session filing availability from a policy rule."""
    if accepted_at.tzinfo is None or accepted_at.utcoffset() is None:
        raise FundamentalValuesError("accepted_at must be timezone-aware")
    rule = policy.rule
    if rule.get("type") != "next_trading_session_time_after_timestamp":
        raise FundamentalValuesError(
            f"{policy.name} policy type must be next_trading_session_time_after_timestamp"
        )
    if rule.get("base") != "accepted_at":
        raise FundamentalValuesError(f"{policy.name} policy base must be accepted_at")
    offset = _positive_int(rule.get("trading_days_offset"), "trading_days_offset")
    if offset != 1:
        raise FundamentalValuesError(
            f"{policy.name} trading_days_offset must be 1 for fundamentals v0"
        )
    policy_time = _policy_time(rule.get("time"), policy.name)
    timezone_name = _rule_str(rule, "timezone", policy.name)
    try:
        policy_timezone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise FundamentalValuesError(
            f"{policy.name} policy timezone is unknown: {timezone_name}"
        ) from exc

    current = accepted_at.astimezone(policy_timezone).date()
    sessions_seen = 0
    while sessions_seen < offset:
        current += timedelta(days=1)
        row = calendar.row_for(current)
        if row.is_session:
            sessions_seen += 1
    local_available_at = datetime.combine(current, policy_time, tzinfo=policy_timezone)
    return local_available_at.astimezone(timezone.utc)


def _validated_records(
    records: Sequence[FundamentalValueRecord],
) -> tuple[FundamentalValueRecord, ...]:
    if isinstance(records, (str, bytes)) or not isinstance(records, Sequence):
        raise FundamentalValuesError("records must be a sequence")
    return tuple(_validated_record(record) for record in records)


def _validated_record(record: FundamentalValueRecord) -> FundamentalValueRecord:
    if not isinstance(record, FundamentalValueRecord):
        raise FundamentalValuesError("records must contain FundamentalValueRecord items")
    _positive_int(record.security_id, "security_id")
    _validate_date(record.period_end_date, "period_end_date")
    if record.fiscal_year < 1900 or record.fiscal_year > 2100:
        raise FundamentalValuesError("fiscal_year must be between 1900 and 2100")
    if record.fiscal_period not in {"FY", "Q1", "Q2", "Q3", "Q4"}:
        raise FundamentalValuesError("fiscal_period must be FY or Q1-Q4")
    if record.period_type not in {"annual", "quarterly"}:
        raise FundamentalValuesError("period_type must be annual or quarterly")
    if record.statement_type not in {"income_statement", "cash_flow_statement"}:
        raise FundamentalValuesError(
            "statement_type must be income_statement or cash_flow_statement"
        )
    _required_label(record.metric_name, "metric_name")
    if not isinstance(record.metric_value, Decimal):
        raise FundamentalValuesError("metric_value must be Decimal")
    if not record.metric_value.is_finite():
        raise FundamentalValuesError("metric_value must be finite")
    _required_label(record.currency, "currency")
    _required_label(record.source_system, "source_system")
    _required_label(record.source_field, "source_field")
    _positive_int(record.raw_object_id, "raw_object_id")
    _validate_datetime(record.accepted_at, "accepted_at")
    _validate_date(record.filing_date, "filing_date")
    _validate_datetime(record.available_at, "available_at")
    if record.available_at <= record.accepted_at:
        raise FundamentalValuesError("available_at must be after accepted_at")
    _positive_int(record.available_at_policy_id, "available_at_policy_id")
    _positive_int(record.normalized_by_run_id, "normalized_by_run_id")
    if not isinstance(record.metadata, Mapping):
        raise FundamentalValuesError("metadata must be a mapping")
    return record


def _record_params(record: FundamentalValueRecord) -> dict[str, Any]:
    return {
        "security_id": record.security_id,
        "period_end_date": record.period_end_date,
        "fiscal_year": record.fiscal_year,
        "fiscal_period": record.fiscal_period,
        "period_type": record.period_type,
        "statement_type": record.statement_type,
        "metric_name": record.metric_name,
        "metric_value": record.metric_value,
        "currency": record.currency,
        "source_system": record.source_system,
        "source_field": record.source_field,
        "raw_object_id": record.raw_object_id,
        "accepted_at": record.accepted_at,
        "filing_date": record.filing_date,
        "available_at": record.available_at,
        "available_at_policy_id": record.available_at_policy_id,
        "normalized_by_run_id": record.normalized_by_run_id,
        "metadata": _stable_json(record.metadata),
    }


def _policy_record(row: object) -> FundamentalPolicy:
    rule = _row_value(row, "rule", 3)
    if isinstance(rule, str):
        try:
            rule = json.loads(rule)
        except json.JSONDecodeError as exc:
            raise FundamentalValuesError("available_at policy rule must be JSON") from exc
    if not isinstance(rule, Mapping):
        raise FundamentalValuesError("available_at policy rule must be a mapping")
    return FundamentalPolicy(
        id=_row_int(row, "id", 0, "available_at_policies.id"),
        name=_row_str(row, "name", 1, "available_at_policies.name"),
        version=_row_int(row, "version", 2, "available_at_policies.version"),
        rule=rule,
    )


def _calendar_row(row: object) -> TradingCalendarRow:
    return TradingCalendarRow(
        date=_row_date(row, "date", 0, "trading_calendar.date"),
        is_session=_row_bool(row, "is_session", 1),
        session_close=_row_optional_datetime(
            row,
            "session_close",
            2,
            "trading_calendar.session_close",
        ),
        is_early_close=_row_bool(row, "is_early_close", 3),
    )


def _stable_json(value: Mapping[str, Any]) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    except TypeError as exc:
        raise FundamentalValuesError("metadata must be JSON serializable") from exc


def _policy_time(value: object, policy_name: str) -> time:
    if not isinstance(value, str):
        raise FundamentalValuesError(f"{policy_name} policy time must be HH:MM")
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise FundamentalValuesError(f"{policy_name} policy time must be HH:MM") from exc


def _rule_str(rule: Mapping[str, Any], key: str, policy_name: str) -> str:
    value = rule.get(key)
    if not isinstance(value, str) or not value.strip():
        raise FundamentalValuesError(f"{policy_name} policy {key} must be a string")
    return value.strip()


def _required_label(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FundamentalValuesError(f"{name} must be a non-empty string")
    return value.strip()


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FundamentalValuesError(f"{name} must be a positive integer")
    return value


def _validate_date(value: object, name: str) -> None:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise FundamentalValuesError(f"{name} must be a date")


def _validate_datetime(value: object, name: str) -> None:
    if not isinstance(value, datetime):
        raise FundamentalValuesError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise FundamentalValuesError(f"{name} must be timezone-aware")


def _row_value(row: object, key: str, index: int) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return row[index]  # type: ignore[index]


def _row_int(row: object, key: str, index: int, name: str) -> int:
    value = _row_value(row, key, index)
    if isinstance(value, bool) or not isinstance(value, int):
        raise FundamentalValuesError(f"{name} returned by database must be an integer")
    return value


def _row_str(row: object, key: str, index: int, name: str) -> str:
    value = _row_value(row, key, index)
    if not isinstance(value, str) or not value.strip():
        raise FundamentalValuesError(
            f"{name} returned by database must be a non-empty string"
        )
    return value.strip()


def _row_bool(row: object, key: str, index: int) -> bool:
    value = _row_value(row, key, index)
    if not isinstance(value, bool):
        raise FundamentalValuesError(f"{key} returned by database must be a boolean")
    return value


def _row_date(row: object, key: str, index: int, name: str) -> date:
    value = _row_value(row, key, index)
    if isinstance(value, datetime) or not isinstance(value, date):
        raise FundamentalValuesError(f"{name} returned by database must be a date")
    return value


def _row_optional_datetime(
    row: object,
    key: str,
    index: int,
    name: str,
) -> datetime | None:
    value = _row_value(row, key, index)
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise FundamentalValuesError(f"{name} returned by database must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise FundamentalValuesError(f"{name} must be timezone-aware")
    return value


@contextmanager
def _cursor(connection: Any) -> Any:
    cursor = connection.cursor()
    if hasattr(cursor, "__enter__"):
        with cursor as managed_cursor:
            yield managed_cursor
        return
    try:
        yield cursor
    finally:
        close = getattr(cursor, "close", None)
        if close is not None:
            close()


_SELECT_TRADING_CALENDAR_SQL = """
SELECT date, is_session, session_close, is_early_close
FROM silver.trading_calendar
ORDER BY date;
""".strip()

_SELECT_POLICIES_BY_NAME_VERSION_SQL = """
SELECT id, name, version, rule
FROM silver.available_at_policies
WHERE name = ANY(%(names)s)
  AND version = %(version)s
ORDER BY name;
""".strip()

_UPSERT_FUNDAMENTAL_VALUE_SQL = """
INSERT INTO silver.fundamental_values (
    security_id,
    period_end_date,
    fiscal_year,
    fiscal_period,
    period_type,
    statement_type,
    metric_name,
    metric_value,
    currency,
    source_system,
    source_field,
    raw_object_id,
    accepted_at,
    filing_date,
    available_at,
    available_at_policy_id,
    normalized_by_run_id,
    metadata
) VALUES (
    %(security_id)s,
    %(period_end_date)s,
    %(fiscal_year)s,
    %(fiscal_period)s,
    %(period_type)s,
    %(statement_type)s,
    %(metric_name)s,
    %(metric_value)s,
    %(currency)s,
    %(source_system)s,
    %(source_field)s,
    %(raw_object_id)s,
    %(accepted_at)s,
    %(filing_date)s,
    %(available_at)s,
    %(available_at_policy_id)s,
    %(normalized_by_run_id)s,
    %(metadata)s::jsonb
)
ON CONFLICT (
    security_id,
    period_end_date,
    period_type,
    statement_type,
    metric_name,
    source_system
) DO UPDATE SET
    fiscal_year = EXCLUDED.fiscal_year,
    fiscal_period = EXCLUDED.fiscal_period,
    metric_value = EXCLUDED.metric_value,
    currency = EXCLUDED.currency,
    source_field = EXCLUDED.source_field,
    raw_object_id = EXCLUDED.raw_object_id,
    accepted_at = EXCLUDED.accepted_at,
    filing_date = EXCLUDED.filing_date,
    available_at = EXCLUDED.available_at,
    available_at_policy_id = EXCLUDED.available_at_policy_id,
    normalized_by_run_id = EXCLUDED.normalized_by_run_id,
    metadata = EXCLUDED.metadata
WHERE
    silver.fundamental_values.fiscal_year IS DISTINCT FROM EXCLUDED.fiscal_year
    OR silver.fundamental_values.fiscal_period IS DISTINCT FROM EXCLUDED.fiscal_period
    OR silver.fundamental_values.metric_value IS DISTINCT FROM EXCLUDED.metric_value
    OR silver.fundamental_values.currency IS DISTINCT FROM EXCLUDED.currency
    OR silver.fundamental_values.source_field IS DISTINCT FROM EXCLUDED.source_field
    OR silver.fundamental_values.raw_object_id IS DISTINCT FROM EXCLUDED.raw_object_id
    OR silver.fundamental_values.accepted_at IS DISTINCT FROM EXCLUDED.accepted_at
    OR silver.fundamental_values.filing_date IS DISTINCT FROM EXCLUDED.filing_date
    OR silver.fundamental_values.available_at IS DISTINCT FROM EXCLUDED.available_at
    OR silver.fundamental_values.available_at_policy_id IS DISTINCT FROM
        EXCLUDED.available_at_policy_id
    OR silver.fundamental_values.normalized_by_run_id IS DISTINCT FROM
        EXCLUDED.normalized_by_run_id
    OR silver.fundamental_values.metadata IS DISTINCT FROM EXCLUDED.metadata;
""".strip()

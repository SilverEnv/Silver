"""Parse selected FMP normalized financial-statement metrics."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Literal
from zoneinfo import ZoneInfo


StatementType = Literal["income_statement", "cash_flow_statement"]
PeriodType = Literal["annual", "quarterly"]
FMP_NAIVE_TIMESTAMP_ZONE = ZoneInfo("America/New_York")


INCOME_METRICS = {
    "revenue": "revenue",
    "gross_profit": "grossProfit",
    "operating_income": "operatingIncome",
    "net_income": "netIncome",
    "diluted_weighted_average_shares": "weightedAverageShsOutDil",
}
CASH_FLOW_METRICS = {
    "operating_cash_flow": "operatingCashFlow",
    "capital_expenditure": "capitalExpenditure",
    "free_cash_flow": "freeCashFlow",
}


class FmpStatementParseError(ValueError):
    """Raised when an FMP statement response is not safe to normalize."""


@dataclass(frozen=True, slots=True)
class FmpFundamentalValue:
    """One selected normalized fundamental metric from an FMP statement row."""

    symbol: str
    period_end_date: date
    fiscal_year: int
    fiscal_period: str
    period_type: PeriodType
    statement_type: StatementType
    metric_name: str
    metric_value: Decimal
    currency: str
    source_system: str
    source_field: str
    accepted_at: datetime
    filing_date: date
    source_metadata: Mapping[str, Any]


def parse_fmp_income_statement(
    payload: Any,
    *,
    expected_symbol: str,
    period_type: PeriodType,
    lookback_start_year: int,
) -> tuple[FmpFundamentalValue, ...]:
    """Parse selected FMP income-statement metrics."""
    return _parse_statement(
        payload,
        expected_symbol=expected_symbol,
        period_type=period_type,
        lookback_start_year=lookback_start_year,
        statement_type="income_statement",
        metric_fields=INCOME_METRICS,
    )


def parse_fmp_cash_flow_statement(
    payload: Any,
    *,
    expected_symbol: str,
    period_type: PeriodType,
    lookback_start_year: int,
) -> tuple[FmpFundamentalValue, ...]:
    """Parse selected FMP cash-flow-statement metrics."""
    return _parse_statement(
        payload,
        expected_symbol=expected_symbol,
        period_type=period_type,
        lookback_start_year=lookback_start_year,
        statement_type="cash_flow_statement",
        metric_fields=CASH_FLOW_METRICS,
    )


def _parse_statement(
    payload: Any,
    *,
    expected_symbol: str,
    period_type: PeriodType,
    lookback_start_year: int,
    statement_type: StatementType,
    metric_fields: Mapping[str, str],
) -> tuple[FmpFundamentalValue, ...]:
    normalized_symbol = _symbol(expected_symbol)
    normalized_period_type = _period_type(period_type)
    normalized_start_year = _start_year(lookback_start_year)
    rows = _payload_rows(payload)

    parsed: list[FmpFundamentalValue] = []
    for index, row in enumerate(rows):
        row_symbol = _symbol(_optional_row_value(row, "symbol") or normalized_symbol)
        if row_symbol != normalized_symbol:
            raise FmpStatementParseError(
                f"FMP {statement_type} row {index} symbol mismatch; "
                f"expected {normalized_symbol}, got {row_symbol}"
            )
        period_end_date = _row_date(row, "date", index, statement_type)
        fiscal_year = _fiscal_year(row, period_end_date, index, statement_type)
        if fiscal_year < normalized_start_year:
            continue
        fiscal_period = _fiscal_period(row, normalized_period_type, index, statement_type)
        accepted_at = _accepted_at(row, index, statement_type)
        filing_date = _row_date(row, "filingDate", index, statement_type)
        currency = _currency(row, index, statement_type)

        for metric_name, source_field in metric_fields.items():
            parsed.append(
                FmpFundamentalValue(
                    symbol=normalized_symbol,
                    period_end_date=period_end_date,
                    fiscal_year=fiscal_year,
                    fiscal_period=fiscal_period,
                    period_type=normalized_period_type,
                    statement_type=statement_type,
                    metric_name=metric_name,
                    metric_value=_decimal_metric(
                        row,
                        source_field,
                        index,
                        statement_type,
                    ),
                    currency=currency,
                    source_system="fmp",
                    source_field=source_field,
                    accepted_at=accepted_at,
                    filing_date=filing_date,
                    source_metadata={
                        "fmp_symbol": row_symbol,
                        "fmp_fiscal_year": _fiscal_year_value(row),
                        "fmp_period": fiscal_period,
                    },
                )
            )
    return tuple(parsed)


def _payload_rows(payload: Any) -> Sequence[Mapping[str, Any]]:
    if not isinstance(payload, Sequence) or isinstance(payload, (str, bytes, bytearray)):
        raise FmpStatementParseError("FMP statement response must be a JSON array")
    rows = []
    for index, row in enumerate(payload):
        if not isinstance(row, Mapping):
            raise FmpStatementParseError(
                f"FMP statement row {index} must be a JSON object"
            )
        rows.append(row)
    return rows


def _symbol(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FmpStatementParseError("symbol must be a non-empty string")
    return value.strip().upper()


def _period_type(value: str) -> PeriodType:
    if value == "annual":
        return "annual"
    if value == "quarterly":
        return "quarterly"
    raise FmpStatementParseError("period_type must be annual or quarterly")


def _start_year(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FmpStatementParseError("lookback_start_year must be an integer")
    if value < 1900 or value > 2100:
        raise FmpStatementParseError("lookback_start_year must be between 1900 and 2100")
    return value


def _optional_row_value(row: Mapping[str, Any], key: str) -> Any:
    return row.get(key)


def _row_date(
    row: Mapping[str, Any],
    key: str,
    index: int,
    statement_type: str,
) -> date:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        raise FmpStatementParseError(
            f"FMP {statement_type} row {index} missing required {key}"
        )
    try:
        return date.fromisoformat(value.strip())
    except ValueError as exc:
        raise FmpStatementParseError(
            f"FMP {statement_type} row {index} {key} must be YYYY-MM-DD"
        ) from exc


def _fiscal_year(
    row: Mapping[str, Any],
    period_end_date: date,
    index: int,
    statement_type: str,
) -> int:
    value = _fiscal_year_value(row)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    if value is None:
        return period_end_date.year
    raise FmpStatementParseError(
        f"FMP {statement_type} row {index} fiscalYear/calendarYear must be a year"
    )


def _fiscal_year_value(row: Mapping[str, Any]) -> Any:
    value = row.get("fiscalYear")
    if value is not None:
        return value
    return row.get("calendarYear")


def _fiscal_period(
    row: Mapping[str, Any],
    period_type: PeriodType,
    index: int,
    statement_type: str,
) -> str:
    value = row.get("period")
    if not isinstance(value, str) or not value.strip():
        raise FmpStatementParseError(
            f"FMP {statement_type} row {index} missing required period"
        )
    normalized = value.strip().upper()
    if period_type == "annual" and normalized != "FY":
        raise FmpStatementParseError(
            f"FMP {statement_type} row {index} expected FY period"
        )
    if period_type == "quarterly" and normalized not in {"Q1", "Q2", "Q3", "Q4"}:
        raise FmpStatementParseError(
            f"FMP {statement_type} row {index} expected Q1-Q4 period"
        )
    return normalized


def _accepted_at(
    row: Mapping[str, Any],
    index: int,
    statement_type: str,
) -> datetime:
    value = row.get("acceptedDate")
    if not isinstance(value, str) or not value.strip():
        raise FmpStatementParseError(
            f"FMP {statement_type} row {index} missing acceptedDate; "
            "fundamental rows must fail closed without filing availability"
        )
    raw = value.strip().replace(" ", "T", 1)
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise FmpStatementParseError(
            f"FMP {statement_type} row {index} acceptedDate must be a timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=FMP_NAIVE_TIMESTAMP_ZONE)
    return parsed.astimezone(timezone.utc)


def _currency(row: Mapping[str, Any], index: int, statement_type: str) -> str:
    value = row.get("reportedCurrency") or row.get("currency")
    if not isinstance(value, str) or not value.strip():
        raise FmpStatementParseError(
            f"FMP {statement_type} row {index} missing reportedCurrency"
        )
    return value.strip().upper()


def _decimal_metric(
    row: Mapping[str, Any],
    source_field: str,
    index: int,
    statement_type: str,
) -> Decimal:
    value = row.get(source_field)
    if value is None:
        raise FmpStatementParseError(
            f"FMP {statement_type} row {index} missing required {source_field}"
        )
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        raise FmpStatementParseError(
            f"FMP {statement_type} row {index} {source_field} must be numeric"
        )
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise FmpStatementParseError(
            f"FMP {statement_type} row {index} {source_field} must be numeric"
        ) from exc
    if not decimal.is_finite():
        raise FmpStatementParseError(
            f"FMP {statement_type} row {index} {source_field} must be finite"
        )
    return decimal

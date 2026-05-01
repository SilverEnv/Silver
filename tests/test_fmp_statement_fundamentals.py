from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from silver.fundamentals import (
    FmpStatementParseError,
    parse_fmp_cash_flow_statement,
    parse_fmp_income_statement,
)


def test_income_statement_parses_selected_metrics_and_diluted_shares() -> None:
    values = parse_fmp_income_statement(
        [
            {
                "symbol": "AAPL",
                "date": "2024-09-28",
                "calendarYear": "2024",
                "period": "FY",
                "reportedCurrency": "USD",
                "filingDate": "2024-11-01",
                "acceptedDate": "2024-11-01 06:01:36",
                "revenue": 391035000000,
                "grossProfit": 180683000000,
                "operatingIncome": 123216000000,
                "netIncome": 93736000000,
                "weightedAverageShsOutDil": 15408095000,
            }
        ],
        expected_symbol="aapl",
        period_type="annual",
        lookback_start_year=2013,
    )

    assert [value.metric_name for value in values] == [
        "revenue",
        "gross_profit",
        "operating_income",
        "net_income",
        "diluted_weighted_average_shares",
    ]
    assert values[0].metric_value == Decimal("391035000000")
    assert values[-1].metric_value == Decimal("15408095000")
    assert values[0].accepted_at == datetime(
        2024,
        11,
        1,
        10,
        1,
        36,
        tzinfo=timezone.utc,
    )


def test_cash_flow_statement_filters_before_lookback_year() -> None:
    values = parse_fmp_cash_flow_statement(
        [
            _cash_flow_row("2012-09-29", "2012"),
            _cash_flow_row("2024-06-29", "2024", period="Q3"),
        ],
        expected_symbol="AAPL",
        period_type="quarterly",
        lookback_start_year=2013,
    )

    assert [value.metric_name for value in values] == [
        "operating_cash_flow",
        "capital_expenditure",
        "free_cash_flow",
    ]
    assert {value.period_end_date.isoformat() for value in values} == {"2024-06-29"}
    assert all(value.fiscal_period == "Q3" for value in values)


def test_missing_accepted_date_fails_closed() -> None:
    row = _income_row()
    del row["acceptedDate"]

    with pytest.raises(FmpStatementParseError, match="missing acceptedDate"):
        parse_fmp_income_statement(
            [row],
            expected_symbol="AAPL",
            period_type="annual",
            lookback_start_year=2013,
        )


def test_missing_diluted_shares_fails_closed() -> None:
    row = _income_row()
    del row["weightedAverageShsOutDil"]

    with pytest.raises(FmpStatementParseError, match="weightedAverageShsOutDil"):
        parse_fmp_income_statement(
            [row],
            expected_symbol="AAPL",
            period_type="annual",
            lookback_start_year=2013,
        )


def _income_row() -> dict[str, object]:
    return {
        "symbol": "AAPL",
        "date": "2024-09-28",
        "calendarYear": "2024",
        "period": "FY",
        "reportedCurrency": "USD",
        "filingDate": "2024-11-01",
        "acceptedDate": "2024-11-01 06:01:36",
        "revenue": 391035000000,
        "grossProfit": 180683000000,
        "operatingIncome": 123216000000,
        "netIncome": 93736000000,
        "weightedAverageShsOutDil": 15408095000,
    }


def _cash_flow_row(
    period_end: str,
    calendar_year: str,
    *,
    period: str = "Q4",
) -> dict[str, object]:
    return {
        "symbol": "AAPL",
        "date": period_end,
        "calendarYear": calendar_year,
        "period": period,
        "reportedCurrency": "USD",
        "filingDate": "2024-08-02",
        "acceptedDate": "2024-08-02 06:01:36",
        "operatingCashFlow": 118254000000,
        "capitalExpenditure": -9447000000,
        "freeCashFlow": 108807000000,
    }

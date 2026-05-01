from __future__ import annotations

from datetime import date, datetime, timezone

from silver.fundamentals import FundamentalPolicy, filing_available_at
from silver.time.trading_calendar import TradingCalendar, TradingCalendarRow


def test_filing_available_at_uses_next_trading_session_open() -> None:
    calendar = TradingCalendar(
        [
            TradingCalendarRow(date(2024, 11, 1), True, _close(2024, 11, 1)),
            TradingCalendarRow(date(2024, 11, 2), False, None),
            TradingCalendarRow(date(2024, 11, 3), False, None),
            TradingCalendarRow(date(2024, 11, 4), True, _close(2024, 11, 4)),
        ]
    )
    policy = FundamentalPolicy(
        id=2,
        name="sec_10k_filing",
        version=1,
        rule={
            "type": "next_trading_session_time_after_timestamp",
            "base": "accepted_at",
            "trading_days_offset": 1,
            "time": "09:30",
            "timezone": "America/New_York",
            "calendar": "NYSE",
        },
    )

    available_at = filing_available_at(
        datetime(2024, 11, 1, 22, 0, tzinfo=timezone.utc),
        policy=policy,
        calendar=calendar,
    )

    assert available_at == datetime(2024, 11, 4, 14, 30, tzinfo=timezone.utc)


def _close(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, 21, 0, tzinfo=timezone.utc)

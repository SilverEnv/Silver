"""Read-only diagnostics for persisted falsifier input coverage."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Protocol

from silver.features.momentum_12_1 import FEATURE_NAME
from silver.time.trading_calendar import CANONICAL_HORIZONS


class FalsifierDiagnosticsError(ValueError):
    """Raised when falsifier diagnostics inputs or rows are malformed."""


class JsonQueryClient(Protocol):
    """Minimal read-only JSON query client used by diagnostics callers."""

    def fetch_json(self, sql: str) -> Any:
        """Return one decoded JSON value for a SQL query."""
        ...


@dataclass(frozen=True, slots=True)
class FeatureDefinitionDiagnostic:
    """Persisted feature definition targeted by the falsifier."""

    id: int
    name: str
    version: int
    definition_hash: str


@dataclass(frozen=True, slots=True)
class HorizonCoverage:
    """Persisted label coverage for one forward-return horizon."""

    horizon_days: int
    row_count: int
    ticker_count: int
    start_date: date | None
    end_date: date | None


@dataclass(frozen=True, slots=True)
class TickerInputCoverage:
    """Layer-by-layer persisted coverage for one universe member."""

    security_id: int
    ticker: str
    valid_from: date
    valid_to: date | None
    price_rows: int
    price_start: date | None
    price_end: date | None
    feature_rows: int
    feature_start: date | None
    feature_end: date | None
    label_rows: int
    label_start: date | None
    label_end: date | None
    joined_rows: int
    joined_start: date | None
    joined_end: date | None
    label_without_feature_rows: int
    feature_without_label_rows: int

    @property
    def membership_range(self) -> str:
        """Return a stable display string for the membership interval."""
        return _date_range(self.valid_from, self.valid_to, open_ended=True)

    @property
    def price_range(self) -> str:
        return _date_range(self.price_start, self.price_end)

    @property
    def feature_range(self) -> str:
        return _date_range(self.feature_start, self.feature_end)

    @property
    def label_range(self) -> str:
        return _date_range(self.label_start, self.label_end)

    @property
    def joined_range(self) -> str:
        return _date_range(self.joined_start, self.joined_end)


@dataclass(frozen=True, slots=True)
class FalsifierInputDiagnostics:
    """Complete coverage diagnostics for a falsifier input request."""

    universe_name: str
    horizon_days: int
    feature_name: str
    canonical_horizons: tuple[int, ...]
    feature_definition: FeatureDefinitionDiagnostic | None
    tickers: tuple[TickerInputCoverage, ...]
    horizon_coverage: tuple[HorizonCoverage, ...]

    @property
    def universe_member_count(self) -> int:
        return len(self.tickers)

    @property
    def price_rows(self) -> int:
        return sum(ticker.price_rows for ticker in self.tickers)

    @property
    def feature_rows(self) -> int:
        return sum(ticker.feature_rows for ticker in self.tickers)

    @property
    def requested_label_rows(self) -> int:
        return sum(ticker.label_rows for ticker in self.tickers)

    @property
    def joined_rows(self) -> int:
        return sum(ticker.joined_rows for ticker in self.tickers)

    @property
    def usable_tickers(self) -> tuple[str, ...]:
        return tuple(ticker.ticker for ticker in self.tickers if ticker.joined_rows > 0)

    @property
    def missing_price_tickers(self) -> tuple[str, ...]:
        return tuple(ticker.ticker for ticker in self.tickers if ticker.price_rows == 0)

    @property
    def missing_feature_tickers(self) -> tuple[str, ...]:
        return tuple(ticker.ticker for ticker in self.tickers if ticker.feature_rows == 0)

    @property
    def missing_requested_label_tickers(self) -> tuple[str, ...]:
        return tuple(ticker.ticker for ticker in self.tickers if ticker.label_rows == 0)

    @property
    def missing_usable_tickers(self) -> tuple[str, ...]:
        return tuple(ticker.ticker for ticker in self.tickers if ticker.joined_rows == 0)

    @property
    def missing_horizons(self) -> tuple[int, ...]:
        present = {
            coverage.horizon_days
            for coverage in self.horizon_coverage
            if coverage.row_count > 0
        }
        return tuple(
            horizon for horizon in self.canonical_horizons if horizon not in present
        )

    @property
    def usable_asof_start(self) -> date | None:
        dates = [
            ticker.joined_start
            for ticker in self.tickers
            if ticker.joined_start is not None
        ]
        return min(dates) if dates else None

    @property
    def usable_asof_end(self) -> date | None:
        dates = [
            ticker.joined_end
            for ticker in self.tickers
            if ticker.joined_end is not None
        ]
        return max(dates) if dates else None

    @property
    def usable_asof_range(self) -> str:
        return _date_range(self.usable_asof_start, self.usable_asof_end)

    @property
    def usable_ticker_coverage(self) -> str:
        if not self.tickers:
            return "0/0 (none)"
        tickers = _comma_list(self.usable_tickers) if self.usable_tickers else "none"
        return f"{len(self.usable_tickers)}/{len(self.tickers)} ({tickers})"

    @property
    def is_sufficient(self) -> bool:
        return not self.blocking_messages

    @property
    def blocking_messages(self) -> tuple[str, ...]:
        messages: list[str] = []
        if not self.tickers:
            messages.append(
                f"universe `{self.universe_name}` has no persisted membership rows"
            )
        if self.feature_definition is None:
            messages.append(f"feature definition `{self.feature_name}` is not persisted")
        if self.missing_price_tickers:
            messages.append(
                "prices_daily rows missing for tickers: "
                f"{_comma_list(self.missing_price_tickers)}"
            )
        if self.missing_feature_tickers:
            messages.append(
                f"feature_values rows for `{self.feature_name}` missing for tickers: "
                f"{_comma_list(self.missing_feature_tickers)}"
            )
        if self.missing_requested_label_tickers:
            messages.append(
                f"forward_return_labels horizon {self.horizon_days} rows missing "
                f"for tickers: {_comma_list(self.missing_requested_label_tickers)}"
            )
        if self.joined_rows == 0 and self.tickers:
            messages.append(
                "no usable feature/label as-of overlap rows exist for "
                f"horizon {self.horizon_days}"
            )
        elif self.missing_usable_tickers:
            messages.append(
                "usable feature/label as-of overlap missing for tickers: "
                f"{_comma_list(self.missing_usable_tickers)}"
            )
        return tuple(messages)

    @property
    def coverage_gap_messages(self) -> tuple[str, ...]:
        messages: list[str] = []
        label_without_feature = _count_by_ticker(
            self.tickers,
            "label_without_feature_rows",
        )
        if label_without_feature:
            messages.append(
                "requested-horizon label rows without feature values: "
                f"{_count_mapping(label_without_feature)}"
            )
        feature_without_label = _count_by_ticker(
            self.tickers,
            "feature_without_label_rows",
        )
        if feature_without_label:
            messages.append(
                "feature rows without requested-horizon labels: "
                f"{_count_mapping(feature_without_label)}"
            )
        return tuple(messages)


def load_falsifier_input_diagnostics(
    client: JsonQueryClient,
    *,
    universe: str,
    horizon: int,
    feature_name: str = FEATURE_NAME,
    canonical_horizons: Sequence[int] = CANONICAL_HORIZONS,
) -> FalsifierInputDiagnostics:
    """Load persisted falsifier input diagnostics without mutating data."""

    normalized_universe = _required_label(universe, "universe")
    normalized_feature = _required_label(feature_name, "feature_name")
    normalized_horizon = _canonical_horizon(horizon, canonical_horizons)
    normalized_canonical_horizons = _canonical_horizons(canonical_horizons)

    payload = client.fetch_json(
        _diagnostics_sql(
            universe=normalized_universe,
            horizon=normalized_horizon,
            feature_name=normalized_feature,
        )
    )
    if not isinstance(payload, Mapping):
        raise FalsifierDiagnosticsError("diagnostics query returned non-object JSON")

    return FalsifierInputDiagnostics(
        universe_name=normalized_universe,
        horizon_days=normalized_horizon,
        feature_name=normalized_feature,
        canonical_horizons=normalized_canonical_horizons,
        feature_definition=_feature_definition(payload.get("feature_definition")),
        tickers=tuple(
            _ticker_coverage(row)
            for row in _required_list(payload, "ticker_coverage")
        ),
        horizon_coverage=tuple(
            _horizon_coverage(row)
            for row in _required_list(payload, "horizon_coverage")
        ),
    )


def render_falsifier_input_diagnostics(
    diagnostics: FalsifierInputDiagnostics,
) -> str:
    """Render readable diagnostics for humans before running the falsifier."""

    feature_version = (
        "not persisted"
        if diagnostics.feature_definition is None
        else (
            f"v{diagnostics.feature_definition.version} "
            f"({diagnostics.feature_definition.definition_hash})"
        )
    )
    status = "SUFFICIENT" if diagnostics.is_sufficient else "INSUFFICIENT"
    lines = [
        "Falsifier input diagnostics",
        f"Status: {status}",
        "",
        "Request:",
        f"- Universe: {diagnostics.universe_name}",
        f"- Horizon: {diagnostics.horizon_days} trading sessions",
        f"- Feature: {diagnostics.feature_name} {feature_version}",
        "",
        "Row counts:",
        f"- Universe members: {diagnostics.universe_member_count}",
        f"- prices_daily rows: {diagnostics.price_rows}",
        f"- feature_values rows: {diagnostics.feature_rows}",
        (
            f"- forward_return_labels rows for horizon "
            f"{diagnostics.horizon_days}: {diagnostics.requested_label_rows}"
        ),
        f"- Usable feature/label as-of rows: {diagnostics.joined_rows}",
        "",
        "Usable coverage:",
        f"- As-of date range: {diagnostics.usable_asof_range}",
        f"- Ticker coverage: {diagnostics.usable_ticker_coverage}",
        "",
        "Missing:",
        *_missing_lines(diagnostics),
        "",
        "Label horizon coverage:",
        *_horizon_lines(diagnostics),
        "",
        "Per-ticker coverage:",
        _ticker_table(diagnostics),
    ]
    return "\n".join(lines) + "\n"


def _missing_lines(diagnostics: FalsifierInputDiagnostics) -> list[str]:
    messages = list(diagnostics.blocking_messages)
    messages.extend(diagnostics.coverage_gap_messages)
    if diagnostics.missing_horizons:
        messages.append(
            "label horizons with no rows: "
            f"{', '.join(str(horizon) for horizon in diagnostics.missing_horizons)}"
        )
    if not messages:
        return ["- None"]
    return [f"- {message}" for message in messages]


def _horizon_lines(diagnostics: FalsifierInputDiagnostics) -> list[str]:
    by_horizon = {
        coverage.horizon_days: coverage for coverage in diagnostics.horizon_coverage
    }
    lines: list[str] = []
    for horizon in diagnostics.canonical_horizons:
        coverage = by_horizon.get(horizon)
        if coverage is None:
            lines.append(f"- {horizon}: 0 rows, 0 tickers, date range n/a")
            continue
        lines.append(
            f"- {horizon}: {coverage.row_count} rows, "
            f"{coverage.ticker_count} tickers, "
            f"date range {_date_range(coverage.start_date, coverage.end_date)}"
        )
    return lines


def _ticker_table(diagnostics: FalsifierInputDiagnostics) -> str:
    if not diagnostics.tickers:
        return "No universe members."
    rows = [
        (
            ticker.ticker,
            ticker.membership_range,
            f"{ticker.price_rows} ({ticker.price_range})",
            f"{ticker.feature_rows} ({ticker.feature_range})",
            f"{ticker.label_rows} ({ticker.label_range})",
            f"{ticker.joined_rows} ({ticker.joined_range})",
        )
        for ticker in diagnostics.tickers
    ]
    return _table(
        (
            "Ticker",
            "Membership",
            "Prices",
            "Features",
            f"Labels h{diagnostics.horizon_days}",
            "Usable as-of",
        ),
        rows,
    )


def _feature_definition(raw: object) -> FeatureDefinitionDiagnostic | None:
    if raw is None:
        return None
    row = _required_mapping(raw, "feature_definition")
    return FeatureDefinitionDiagnostic(
        id=_required_int(row, "id"),
        name=_required_str(row, "name"),
        version=_required_int(row, "version"),
        definition_hash=_required_str(row, "definition_hash"),
    )


def _ticker_coverage(raw: object) -> TickerInputCoverage:
    row = _required_mapping(raw, "ticker_coverage row")
    return TickerInputCoverage(
        security_id=_required_int(row, "security_id"),
        ticker=_required_str(row, "ticker").upper(),
        valid_from=_required_date(row, "valid_from"),
        valid_to=_optional_date(row.get("valid_to"), "valid_to"),
        price_rows=_required_int(row, "price_rows"),
        price_start=_optional_date(row.get("price_start"), "price_start"),
        price_end=_optional_date(row.get("price_end"), "price_end"),
        feature_rows=_required_int(row, "feature_rows"),
        feature_start=_optional_date(row.get("feature_start"), "feature_start"),
        feature_end=_optional_date(row.get("feature_end"), "feature_end"),
        label_rows=_required_int(row, "label_rows"),
        label_start=_optional_date(row.get("label_start"), "label_start"),
        label_end=_optional_date(row.get("label_end"), "label_end"),
        joined_rows=_required_int(row, "joined_rows"),
        joined_start=_optional_date(row.get("joined_start"), "joined_start"),
        joined_end=_optional_date(row.get("joined_end"), "joined_end"),
        label_without_feature_rows=_required_int(
            row,
            "label_without_feature_rows",
        ),
        feature_without_label_rows=_required_int(
            row,
            "feature_without_label_rows",
        ),
    )


def _horizon_coverage(raw: object) -> HorizonCoverage:
    row = _required_mapping(raw, "horizon_coverage row")
    return HorizonCoverage(
        horizon_days=_required_int(row, "horizon_days"),
        row_count=_required_int(row, "row_count"),
        ticker_count=_required_int(row, "ticker_count"),
        start_date=_optional_date(row.get("start_date"), "start_date"),
        end_date=_optional_date(row.get("end_date"), "end_date"),
    )


def _diagnostics_sql(*, universe: str, horizon: int, feature_name: str) -> str:
    return f"""
WITH universe_rows AS (
    SELECT
        s.id AS security_id,
        s.ticker,
        um.valid_from,
        um.valid_to
    FROM silver.universe_membership um
    JOIN silver.securities s ON s.id = um.security_id
    WHERE um.universe_name = {_sql_literal(universe)}
),
feature_definition AS (
    SELECT id, name, version, definition_hash
    FROM silver.feature_definitions
    WHERE name = {_sql_literal(feature_name)}
    ORDER BY version DESC
    LIMIT 1
),
price_rows AS (
    SELECT u.security_id, p.date
    FROM universe_rows u
    JOIN silver.prices_daily p
      ON p.security_id = u.security_id
     AND p.date >= u.valid_from
     AND (u.valid_to IS NULL OR p.date <= u.valid_to)
),
feature_rows AS (
    SELECT u.security_id, fv.asof_date
    FROM universe_rows u
    JOIN silver.feature_values fv
      ON fv.security_id = u.security_id
     AND fv.asof_date >= u.valid_from
     AND (u.valid_to IS NULL OR fv.asof_date <= u.valid_to)
    JOIN feature_definition fd
      ON fd.id = fv.feature_definition_id
),
all_horizon_label_rows AS (
    SELECT u.security_id, frl.label_date, frl.horizon_days
    FROM universe_rows u
    JOIN silver.forward_return_labels frl
      ON frl.security_id = u.security_id
     AND frl.label_date >= u.valid_from
     AND (u.valid_to IS NULL OR frl.label_date <= u.valid_to)
),
requested_label_rows AS (
    SELECT security_id, label_date
    FROM all_horizon_label_rows
    WHERE horizon_days = {horizon}
),
joined_rows AS (
    SELECT f.security_id, f.asof_date
    FROM feature_rows f
    JOIN requested_label_rows l
      ON l.security_id = f.security_id
     AND l.label_date = f.asof_date
),
ticker_coverage AS (
    SELECT
        u.security_id,
        u.ticker,
        u.valid_from::text AS valid_from,
        u.valid_to::text AS valid_to,
        (
            SELECT count(*)::integer
            FROM price_rows p
            WHERE p.security_id = u.security_id
        ) AS price_rows,
        (
            SELECT min(p.date)::text
            FROM price_rows p
            WHERE p.security_id = u.security_id
        ) AS price_start,
        (
            SELECT max(p.date)::text
            FROM price_rows p
            WHERE p.security_id = u.security_id
        ) AS price_end,
        (
            SELECT count(*)::integer
            FROM feature_rows f
            WHERE f.security_id = u.security_id
        ) AS feature_rows,
        (
            SELECT min(f.asof_date)::text
            FROM feature_rows f
            WHERE f.security_id = u.security_id
        ) AS feature_start,
        (
            SELECT max(f.asof_date)::text
            FROM feature_rows f
            WHERE f.security_id = u.security_id
        ) AS feature_end,
        (
            SELECT count(*)::integer
            FROM requested_label_rows l
            WHERE l.security_id = u.security_id
        ) AS label_rows,
        (
            SELECT min(l.label_date)::text
            FROM requested_label_rows l
            WHERE l.security_id = u.security_id
        ) AS label_start,
        (
            SELECT max(l.label_date)::text
            FROM requested_label_rows l
            WHERE l.security_id = u.security_id
        ) AS label_end,
        (
            SELECT count(*)::integer
            FROM joined_rows j
            WHERE j.security_id = u.security_id
        ) AS joined_rows,
        (
            SELECT min(j.asof_date)::text
            FROM joined_rows j
            WHERE j.security_id = u.security_id
        ) AS joined_start,
        (
            SELECT max(j.asof_date)::text
            FROM joined_rows j
            WHERE j.security_id = u.security_id
        ) AS joined_end,
        (
            SELECT count(*)::integer
            FROM requested_label_rows l
            LEFT JOIN feature_rows f
              ON f.security_id = l.security_id
             AND f.asof_date = l.label_date
            WHERE l.security_id = u.security_id
              AND f.security_id IS NULL
        ) AS label_without_feature_rows,
        (
            SELECT count(*)::integer
            FROM feature_rows f
            LEFT JOIN requested_label_rows l
              ON l.security_id = f.security_id
             AND l.label_date = f.asof_date
            WHERE f.security_id = u.security_id
              AND l.security_id IS NULL
        ) AS feature_without_label_rows
    FROM universe_rows u
),
horizon_coverage AS (
    SELECT
        horizon_days,
        count(*)::integer AS row_count,
        count(DISTINCT security_id)::integer AS ticker_count,
        min(label_date)::text AS start_date,
        max(label_date)::text AS end_date
    FROM all_horizon_label_rows
    GROUP BY horizon_days
)
SELECT jsonb_build_object(
    'feature_definition', (SELECT to_jsonb(fd) FROM feature_definition fd),
    'ticker_coverage', (
        SELECT COALESCE(
            jsonb_agg(to_jsonb(row) ORDER BY row.ticker),
            '[]'::jsonb
        )
        FROM ticker_coverage row
    ),
    'horizon_coverage', (
        SELECT COALESCE(
            jsonb_agg(to_jsonb(row) ORDER BY row.horizon_days),
            '[]'::jsonb
        )
        FROM horizon_coverage row
    )
)::text;
""".strip()


def _required_mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise FalsifierDiagnosticsError(f"{name} must be an object")
    return value


def _required_list(row: Mapping[str, Any], key: str) -> list[object]:
    value = row.get(key)
    if not isinstance(value, list):
        raise FalsifierDiagnosticsError(f"diagnostics field `{key}` must be a list")
    return value


def _required_str(row: Mapping[str, Any], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        raise FalsifierDiagnosticsError(f"diagnostics field `{key}` must be a string")
    return value.strip()


def _required_int(row: Mapping[str, Any], key: str) -> int:
    value = row.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise FalsifierDiagnosticsError(f"diagnostics field `{key}` must be an integer")
    if value < 0:
        raise FalsifierDiagnosticsError(
            f"diagnostics field `{key}` must be non-negative"
        )
    return value


def _required_date(row: Mapping[str, Any], key: str) -> date:
    parsed = _optional_date(row.get(key), key)
    if parsed is None:
        raise FalsifierDiagnosticsError(f"diagnostics field `{key}` must be a date")
    return parsed


def _optional_date(value: object, name: str) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        raise FalsifierDiagnosticsError(f"diagnostics field `{name}` must be a date")
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return date.fromisoformat(value.strip())
        except ValueError as exc:
            raise FalsifierDiagnosticsError(
                f"diagnostics field `{name}` must use YYYY-MM-DD"
            ) from exc
    raise FalsifierDiagnosticsError(f"diagnostics field `{name}` must be a date")


def _required_label(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FalsifierDiagnosticsError(f"{name} must be a non-empty string")
    return value.strip()


def _canonical_horizon(value: object, canonical_horizons: Sequence[int]) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FalsifierDiagnosticsError("horizon must be an integer")
    normalized_horizons = _canonical_horizons(canonical_horizons)
    if value not in normalized_horizons:
        allowed = ", ".join(str(horizon) for horizon in normalized_horizons)
        raise FalsifierDiagnosticsError(f"horizon must be one of {allowed}; got {value}")
    return value


def _canonical_horizons(values: Sequence[int]) -> tuple[int, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise FalsifierDiagnosticsError("canonical_horizons must be a sequence")
    normalized = tuple(values)
    if not normalized:
        raise FalsifierDiagnosticsError("canonical_horizons must not be empty")
    for value in normalized:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise FalsifierDiagnosticsError(
                "canonical_horizons must contain positive integers"
            )
    return normalized


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _date_range(
    start: date | None,
    end: date | None,
    *,
    open_ended: bool = False,
) -> str:
    if start is None:
        return "n/a"
    if end is None:
        return f"{start.isoformat()} to open" if open_ended else start.isoformat()
    if start == end:
        return start.isoformat()
    return f"{start.isoformat()} to {end.isoformat()}"


def _comma_list(values: Sequence[str]) -> str:
    return ", ".join(values)


def _count_by_ticker(
    tickers: Sequence[TickerInputCoverage],
    attr: str,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for ticker in tickers:
        value = getattr(ticker, attr)
        if value > 0:
            counts[ticker.ticker] = value
    return counts


def _count_mapping(values: Mapping[str, int]) -> str:
    return ", ".join(f"{ticker}={count}" for ticker, count in sorted(values.items()))


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    header_line = "| " + " | ".join(headers) + " |"
    separator = "| " + " | ".join("---" for _ in headers) + " |"
    row_lines = ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join((header_line, separator, *row_lines))

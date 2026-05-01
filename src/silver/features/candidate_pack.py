"""Materialize the first deterministic feature-candidate pack."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime, timezone
from typing import Literal, Protocol

from silver.features.dollar_volume import (
    AVG_DOLLAR_VOLUME_63_DEFINITION,
    AdjustedPriceVolumeObservation,
    compute_avg_dollar_volume_63,
)
from silver.features.momentum_12_1 import (
    DAILY_PRICE_POLICY_NAME,
    DAILY_PRICE_POLICY_VERSION,
    LONG_LOOKBACK_SESSIONS,
    MOMENTUM_12_1_DEFINITION,
    SKIP_RECENT_SESSIONS,
    AdjustedDailyPriceObservation,
    NumericFeatureDefinition,
    compute_momentum_12_1,
    daily_price_available_at,
)
from silver.features.realized_volatility import (
    REALIZED_VOLATILITY_63_DEFINITION,
    RETURN_WINDOW_SESSIONS,
    compute_realized_volatility_63,
)
from silver.features.repository import (
    AvailableAtPolicyRecord,
    FeatureDefinitionRecord,
    FeatureStoreError,
    FeatureValueWrite,
    UniverseMembershipRecord,
)
from silver.time.trading_calendar import TradingCalendar, TradingCalendarRow


SelectionDirection = Literal["high", "low"]
CandidateMaterializer = Literal[
    "momentum_12_1",
    "avg_dollar_volume_63",
    "realized_volatility_63",
]


class CandidateFeatureRepository(Protocol):
    def ensure_feature_definition(
        self,
        definition: NumericFeatureDefinition,
        *,
        notes: str | None = None,
    ) -> FeatureDefinitionRecord:
        ...

    def load_available_at_policy(
        self,
        *,
        name: str,
        version: int,
    ) -> AvailableAtPolicyRecord:
        ...

    def load_universe_memberships(
        self,
        *,
        universe_name: str,
        start_date: date | None,
        end_date: date | None,
    ) -> tuple[UniverseMembershipRecord, ...]:
        ...

    def load_trading_calendar(
        self,
        *,
        end_date: date | None,
    ) -> tuple[TradingCalendarRow, ...]:
        ...

    def load_adjusted_prices(
        self,
        *,
        security_ids: Sequence[int],
        end_date: date | None,
        available_at_policy_id: int,
    ) -> tuple[tuple[int, AdjustedDailyPriceObservation], ...]:
        ...

    def load_adjusted_price_volumes(
        self,
        *,
        security_ids: Sequence[int],
        end_date: date | None,
        available_at_policy_id: int,
    ) -> tuple[tuple[int, AdjustedPriceVolumeObservation], ...]:
        ...

    def write_feature_values(
        self,
        values: Sequence[FeatureValueWrite],
    ) -> object:
        ...


@dataclass(frozen=True, slots=True)
class FeatureCandidate:
    hypothesis_key: str
    name: str
    thesis: str
    signal_name: str
    mechanism: str
    definition: NumericFeatureDefinition
    materializer: CandidateMaterializer
    selection_direction: SelectionDirection
    notes: str


@dataclass(frozen=True, slots=True)
class CandidateMaterializationSummary:
    candidate_key: str
    feature_definition_id: int
    universe_name: str
    requested_start_date: date | None
    requested_end_date: date | None
    materialized_start_date: date | None
    materialized_end_date: date | None
    securities_seen: int
    eligible_security_dates: int
    values_written: int
    skipped_by_reason: Mapping[str, int]

    @property
    def skipped_total(self) -> int:
        return sum(self.skipped_by_reason.values())


FEATURE_CANDIDATES: tuple[FeatureCandidate, ...] = (
    FeatureCandidate(
        hypothesis_key="momentum_12_1",
        name="Momentum 12-1",
        thesis=(
            "Securities with stronger prior 12-month returns, skipping the most "
            "recent month, may continue to outperform over the next quarter."
        ),
        signal_name=MOMENTUM_12_1_DEFINITION.name,
        mechanism=(
            "Trend persistence and delayed investor reaction can make medium-term "
            "relative strength informative after costs."
        ),
        definition=MOMENTUM_12_1_DEFINITION,
        materializer="momentum_12_1",
        selection_direction="high",
        notes="Deterministic 12-1 simple-return momentum from adjusted closes.",
    ),
    FeatureCandidate(
        hypothesis_key="avg_dollar_volume_63",
        name="Average Dollar Volume 63",
        thesis=(
            "Securities with higher recent dollar volume may have stronger "
            "institutional sponsorship and cleaner tradability over the next "
            "quarter."
        ),
        signal_name=AVG_DOLLAR_VOLUME_63_DEFINITION.name,
        mechanism=(
            "Liquidity can proxy for investor attention, lower trading friction, "
            "and broader participation."
        ),
        definition=AVG_DOLLAR_VOLUME_63_DEFINITION,
        materializer="avg_dollar_volume_63",
        selection_direction="high",
        notes="Trailing 63-session average of adjusted close times volume.",
    ),
    FeatureCandidate(
        hypothesis_key="low_realized_volatility_63",
        name="Low Realized Volatility 63",
        thesis=(
            "Securities with lower recent realized volatility may deliver better "
            "risk-adjusted forward returns than high-volatility peers."
        ),
        signal_name=REALIZED_VOLATILITY_63_DEFINITION.name,
        mechanism=(
            "The low-volatility effect can appear when investors overpay for "
            "high-beta or lottery-like stocks."
        ),
        definition=REALIZED_VOLATILITY_63_DEFINITION,
        materializer="realized_volatility_63",
        selection_direction="low",
        notes="Trailing 63-session annualized sample standard deviation of returns.",
    ),
)


def feature_candidate_keys() -> tuple[str, ...]:
    return tuple(candidate.hypothesis_key for candidate in FEATURE_CANDIDATES)


def feature_candidate_by_key(key: str) -> FeatureCandidate:
    normalized = _candidate_key(key)
    for candidate in FEATURE_CANDIDATES:
        if candidate.hypothesis_key == normalized:
            return candidate
    allowed = ", ".join(feature_candidate_keys())
    raise FeatureStoreError(f"unknown feature candidate {normalized}; choose {allowed}")


def feature_candidates_for_keys(keys: Sequence[str] | None) -> tuple[FeatureCandidate, ...]:
    if not keys:
        return FEATURE_CANDIDATES
    return tuple(feature_candidate_by_key(key) for key in keys)


def materialize_feature_candidate(
    repository: CandidateFeatureRepository,
    candidate: FeatureCandidate,
    *,
    universe_name: str,
    start_date: date | None,
    end_date: date | None,
    computed_by_run_id: int,
    dry_run: bool = False,
    available_at_cutoff: datetime | None = None,
) -> CandidateMaterializationSummary:
    _validate_date_bounds(start_date=start_date, end_date=end_date)
    if computed_by_run_id <= 0:
        raise FeatureStoreError("computed_by_run_id must be a positive integer")
    cutoff = available_at_cutoff or datetime.now(timezone.utc)
    _require_aware(cutoff, "available_at_cutoff")

    definition = repository.ensure_feature_definition(
        candidate.definition,
        notes=candidate.notes,
    )
    policy = repository.load_available_at_policy(
        name=DAILY_PRICE_POLICY_NAME,
        version=DAILY_PRICE_POLICY_VERSION,
    )
    memberships = repository.load_universe_memberships(
        universe_name=universe_name,
        start_date=start_date,
        end_date=end_date,
    )
    if not memberships:
        raise FeatureStoreError(f"universe {universe_name} has no eligible members")

    security_ids = tuple(sorted({membership.security_id for membership in memberships}))
    calendar_rows = repository.load_trading_calendar(end_date=end_date)
    calendar = TradingCalendar(calendar_rows)
    candidate_dates = _candidate_asof_dates(
        calendar_rows=calendar.rows,
        start_date=start_date,
        end_date=end_date,
        available_at_cutoff=cutoff,
    )

    if candidate.materializer == "avg_dollar_volume_63":
        writes, skipped, eligible = _materialize_dollar_volume(
            repository,
            candidate=candidate,
            definition=definition,
            policy=policy,
            memberships=memberships,
            security_ids=security_ids,
            calendar=calendar,
            candidate_dates=candidate_dates,
            universe_name=universe_name,
            end_date=end_date,
            computed_by_run_id=computed_by_run_id,
        )
    else:
        writes, skipped, eligible = _materialize_price_only_candidate(
            repository,
            candidate=candidate,
            definition=definition,
            policy=policy,
            memberships=memberships,
            security_ids=security_ids,
            calendar=calendar,
            candidate_dates=candidate_dates,
            universe_name=universe_name,
            end_date=end_date,
            computed_by_run_id=computed_by_run_id,
        )

    if not dry_run:
        repository.write_feature_values(writes)

    return CandidateMaterializationSummary(
        candidate_key=candidate.hypothesis_key,
        feature_definition_id=definition.id,
        universe_name=universe_name,
        requested_start_date=start_date,
        requested_end_date=end_date,
        materialized_start_date=candidate_dates[0] if candidate_dates else None,
        materialized_end_date=candidate_dates[-1] if candidate_dates else None,
        securities_seen=len(security_ids),
        eligible_security_dates=eligible,
        values_written=len(writes),
        skipped_by_reason=dict(sorted(skipped.items())),
    )


def _materialize_price_only_candidate(
    repository: CandidateFeatureRepository,
    *,
    candidate: FeatureCandidate,
    definition: FeatureDefinitionRecord,
    policy: AvailableAtPolicyRecord,
    memberships: Sequence[UniverseMembershipRecord],
    security_ids: Sequence[int],
    calendar: TradingCalendar,
    candidate_dates: Sequence[date],
    universe_name: str,
    end_date: date | None,
    computed_by_run_id: int,
) -> tuple[list[FeatureValueWrite], Counter[str], int]:
    prices = repository.load_adjusted_prices(
        security_ids=security_ids,
        end_date=end_date,
        available_at_policy_id=policy.id,
    )
    price_lookup_by_security = _price_lookup_by_security(prices)
    session_dates = tuple(row.date for row in calendar.rows if row.is_session)
    session_index = {session: index for index, session in enumerate(session_dates)}
    writes: list[FeatureValueWrite] = []
    skipped: Counter[str] = Counter()
    eligible_security_dates = 0

    for asof_date in candidate_dates:
        asof = daily_price_available_at(asof_date).astimezone(timezone.utc)
        for membership in memberships:
            if not membership.is_active_on(asof_date):
                continue
            eligible_security_dates += 1
            security_prices = price_lookup_by_security.get(membership.security_id, {})
            if candidate.materializer == "momentum_12_1":
                observations = _momentum_boundary_prices(
                    security_prices=security_prices,
                    session_dates=session_dates,
                    session_index=session_index,
                    asof_date=asof_date,
                )
                if observations is None:
                    skipped["insufficient_history"] += 1
                    continue
                result = compute_momentum_12_1(
                    security_id=membership.security_id,
                    asof=asof,
                    prices=observations,
                    calendar=calendar,
                )
            elif candidate.materializer == "realized_volatility_63":
                observations = _rolling_price_window(
                    security_prices=security_prices,
                    session_dates=session_dates,
                    session_index=session_index,
                    asof_date=asof_date,
                    required_observations=RETURN_WINDOW_SESSIONS + 1,
                )
                if observations is None:
                    skipped["insufficient_history"] += 1
                    continue
                result = compute_realized_volatility_63(
                    security_id=membership.security_id,
                    asof=asof,
                    prices=observations,
                    calendar=calendar,
                )
            else:  # pragma: no cover - guarded by caller branch.
                raise FeatureStoreError(f"unsupported candidate {candidate.materializer}")

            if result.status != "ok" or result.value is None:
                skipped[result.status] += 1
                continue
            writes.append(
                _feature_value_write(
                    result=result,
                    definition=definition,
                    policy=policy,
                    candidate=candidate,
                    universe_name=universe_name,
                    computed_by_run_id=computed_by_run_id,
                )
            )
    return writes, skipped, eligible_security_dates


def _materialize_dollar_volume(
    repository: CandidateFeatureRepository,
    *,
    candidate: FeatureCandidate,
    definition: FeatureDefinitionRecord,
    policy: AvailableAtPolicyRecord,
    memberships: Sequence[UniverseMembershipRecord],
    security_ids: Sequence[int],
    calendar: TradingCalendar,
    candidate_dates: Sequence[date],
    universe_name: str,
    end_date: date | None,
    computed_by_run_id: int,
) -> tuple[list[FeatureValueWrite], Counter[str], int]:
    rows = repository.load_adjusted_price_volumes(
        security_ids=security_ids,
        end_date=end_date,
        available_at_policy_id=policy.id,
    )
    price_lookup_by_security = _price_volume_lookup_by_security(rows)
    session_dates = tuple(row.date for row in calendar.rows if row.is_session)
    session_index = {session: index for index, session in enumerate(session_dates)}
    writes: list[FeatureValueWrite] = []
    skipped: Counter[str] = Counter()
    eligible_security_dates = 0

    for asof_date in candidate_dates:
        asof = daily_price_available_at(asof_date).astimezone(timezone.utc)
        for membership in memberships:
            if not membership.is_active_on(asof_date):
                continue
            eligible_security_dates += 1
            observations = _rolling_price_volume_window(
                security_rows=price_lookup_by_security.get(membership.security_id, {}),
                session_dates=session_dates,
                session_index=session_index,
                asof_date=asof_date,
                required_observations=63,
            )
            if observations is None:
                skipped["insufficient_history"] += 1
                continue
            result = compute_avg_dollar_volume_63(
                security_id=membership.security_id,
                asof=asof,
                observations=observations,
                calendar=calendar,
            )
            if result.status != "ok" or result.value is None:
                skipped[result.status] += 1
                continue
            writes.append(
                _feature_value_write(
                    result=result,
                    definition=definition,
                    policy=policy,
                    candidate=candidate,
                    universe_name=universe_name,
                    computed_by_run_id=computed_by_run_id,
                )
            )
    return writes, skipped, eligible_security_dates


def _feature_value_write(
    *,
    result: object,
    definition: FeatureDefinitionRecord,
    policy: AvailableAtPolicyRecord,
    candidate: FeatureCandidate,
    universe_name: str,
    computed_by_run_id: int,
) -> FeatureValueWrite:
    return FeatureValueWrite(
        security_id=getattr(result, "security_id"),
        asof_date=getattr(result, "asof_date"),
        feature_definition_id=definition.id,
        value=float(getattr(result, "value")),
        available_at=getattr(result, "available_at"),
        available_at_policy_id=policy.id,
        computed_by_run_id=computed_by_run_id,
        source_metadata={
            "source": f"silver.features.{candidate.materializer}",
            "candidate_key": candidate.hypothesis_key,
            "selection_direction": candidate.selection_direction,
            "universe_name": universe_name,
            "available_at": getattr(result, "available_at").isoformat(),
            "daily_price_policy": {"name": policy.name, "version": policy.version},
            "window": _metadata_value(getattr(result, "window")),
        },
    )


def _candidate_asof_dates(
    *,
    calendar_rows: Sequence[TradingCalendarRow],
    start_date: date | None,
    end_date: date | None,
    available_at_cutoff: datetime,
) -> tuple[date, ...]:
    return tuple(
        row.date
        for row in calendar_rows
        if row.is_session
        and (start_date is None or row.date >= start_date)
        and (end_date is None or row.date <= end_date)
        and daily_price_available_at(row.date).astimezone(timezone.utc)
        <= available_at_cutoff
    )


def _momentum_boundary_prices(
    *,
    security_prices: Mapping[date, AdjustedDailyPriceObservation],
    session_dates: Sequence[date],
    session_index: Mapping[date, int],
    asof_date: date,
) -> tuple[AdjustedDailyPriceObservation, ...] | None:
    index = session_index[asof_date]
    if index < LONG_LOOKBACK_SESSIONS:
        return None
    boundary_dates = (
        session_dates[index - LONG_LOOKBACK_SESSIONS],
        session_dates[index - SKIP_RECENT_SESSIONS],
    )
    return tuple(
        price
        for boundary_date in boundary_dates
        for price in [security_prices.get(boundary_date)]
        if price is not None
    )


def _rolling_price_window(
    *,
    security_prices: Mapping[date, AdjustedDailyPriceObservation],
    session_dates: Sequence[date],
    session_index: Mapping[date, int],
    asof_date: date,
    required_observations: int,
) -> tuple[AdjustedDailyPriceObservation, ...] | None:
    index = session_index[asof_date]
    if index < required_observations - 1:
        return None
    window_dates = session_dates[index - required_observations + 1 : index + 1]
    return tuple(
        price
        for window_date in window_dates
        for price in [security_prices.get(window_date)]
        if price is not None
    )


def _rolling_price_volume_window(
    *,
    security_rows: Mapping[date, AdjustedPriceVolumeObservation],
    session_dates: Sequence[date],
    session_index: Mapping[date, int],
    asof_date: date,
    required_observations: int,
) -> tuple[AdjustedPriceVolumeObservation, ...] | None:
    index = session_index[asof_date]
    if index < required_observations - 1:
        return None
    window_dates = session_dates[index - required_observations + 1 : index + 1]
    return tuple(
        row
        for window_date in window_dates
        for row in [security_rows.get(window_date)]
        if row is not None
    )


def _price_lookup_by_security(
    rows: Sequence[tuple[int, AdjustedDailyPriceObservation]],
) -> dict[int, dict[date, AdjustedDailyPriceObservation]]:
    grouped: defaultdict[int, dict[date, AdjustedDailyPriceObservation]] = defaultdict(
        dict
    )
    for security_id, price in rows:
        grouped[security_id][price.price_date] = price
    return dict(grouped)


def _price_volume_lookup_by_security(
    rows: Sequence[tuple[int, AdjustedPriceVolumeObservation]],
) -> dict[int, dict[date, AdjustedPriceVolumeObservation]]:
    grouped: defaultdict[int, dict[date, AdjustedPriceVolumeObservation]] = defaultdict(
        dict
    )
    for security_id, row in rows:
        grouped[security_id][row.price_date] = row
    return dict(grouped)


def _metadata_value(value: object) -> object:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if is_dataclass(value):
        return {
            field.name: _metadata_value(getattr(value, field.name))
            for field in fields(value)
            if getattr(value, field.name) not in (None, ())
        }
    if isinstance(value, Mapping):
        return {
            str(key): _metadata_value(item)
            for key, item in value.items()
            if item not in (None, ())
        }
    if isinstance(value, tuple):
        return [_metadata_value(item) for item in value]
    return value


def _candidate_key(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FeatureStoreError("candidate key must be a non-empty string")
    return value.strip()


def _validate_date_bounds(*, start_date: date | None, end_date: date | None) -> None:
    if start_date is not None and not isinstance(start_date, date):
        raise FeatureStoreError("start_date must be a date")
    if end_date is not None and not isinstance(end_date, date):
        raise FeatureStoreError("end_date must be a date")
    if start_date is not None and end_date is not None and end_date < start_date:
        raise FeatureStoreError("end_date must be on or after start_date")


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise FeatureStoreError(f"{field_name} must be timezone-aware")

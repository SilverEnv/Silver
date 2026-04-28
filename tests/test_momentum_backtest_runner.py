from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone

import pytest

from silver.backtest import (
    MomentumBacktestConfig,
    MomentumBacktestError,
    PersistedForwardReturnLabel,
    PersistedMomentumFeatureValue,
    TransactionCostConfig,
    WalkForwardConfig,
    run_momentum_12_1_backtest,
)
from silver.time.trading_calendar import TradingCalendarRow


def test_runner_returns_deterministic_structured_results() -> None:
    rows, sessions = _calendar(13)
    features, labels = _momentum_inputs(sessions)
    costs = TransactionCostConfig(
        half_spread_bps=5.0,
        market_impact_bps=1.0,
        borrow_cost_bps_annualized=25.0,
    )
    config = _config(transaction_costs=costs)

    first = run_momentum_12_1_backtest(
        features=list(reversed(features)),
        labels=list(reversed(labels)),
        calendar=tuple(reversed(rows)),
        config=config,
    )
    second = run_momentum_12_1_backtest(
        features=features,
        labels=labels,
        calendar=rows,
        config=config,
    )

    assert first == second
    assert first.horizon_days == 2
    assert first.feature_name == "momentum_12_1"
    assert first.feature_version == 1
    assert first.model_name == "momentum_12_1_univariate_ols"
    assert first.transaction_costs == costs
    assert len(first.splits) == 3
    assert first.splits[0].train_window == (sessions[0], sessions[2])
    assert first.splits[0].test_window == (sessions[5], sessions[6])
    assert first.sample_counts.train_pairs == 45
    assert first.sample_counts.test_pairs == 18
    assert first.metrics.rank_correlation == pytest.approx(1.0)
    assert first.metrics.long_short_spread_gross == pytest.approx(0.10)
    assert first.metrics.long_short_spread_net == pytest.approx(
        0.10 - costs.long_short_cost(2)
    )
    assert first.baseline_comparison.name == "momentum_12_1_rank"
    assert first.baseline_comparison.metrics.rank_correlation == pytest.approx(1.0)
    assert first.baseline_comparison.long_short_spread_net_delta == pytest.approx(0.0)


def test_training_labels_must_be_available_before_test_window() -> None:
    rows, sessions = _calendar(13)
    features, labels = _momentum_inputs(sessions)
    labels = [
        replace(label, available_at=_close(sessions[5]))
        if label.security_id == 1 and label.label_date == sessions[2]
        else label
        for label in labels
    ]

    with pytest.raises(MomentumBacktestError, match="unavailable before split 0"):
        run_momentum_12_1_backtest(
            features=features,
            labels=labels,
            calendar=rows,
            config=_config(),
        )


def test_test_labels_must_be_available_by_split_label_outcome_end() -> None:
    rows, sessions = _calendar(13)
    features, labels = _momentum_inputs(sessions)
    labels = [
        replace(label, available_at=_close(sessions[10]))
        if label.security_id == 1 and label.label_date == sessions[5]
        else label
        for label in labels
    ]

    with pytest.raises(MomentumBacktestError, match="unavailable by split 0"):
        run_momentum_12_1_backtest(
            features=features,
            labels=labels,
            calendar=rows,
            config=_config(),
        )


def _config(
    *,
    transaction_costs: TransactionCostConfig | None = None,
) -> MomentumBacktestConfig:
    return MomentumBacktestConfig(
        horizon_days=2,
        walk_forward=WalkForwardConfig(
            min_train_sessions=3,
            test_sessions=2,
            step_sessions=2,
            label_horizon_sessions=2,
        ),
        long_short_quantile=1 / 3,
        transaction_costs=transaction_costs or TransactionCostConfig(),
    )


def _momentum_inputs(
    sessions: list[date],
) -> tuple[list[PersistedMomentumFeatureValue], list[PersistedForwardReturnLabel]]:
    features: list[PersistedMomentumFeatureValue] = []
    labels: list[PersistedForwardReturnLabel] = []
    for date_index, session in enumerate(sessions[:-2]):
        for security_id, base_momentum in ((1, -1.0), (2, 0.0), (3, 1.0)):
            feature_value = base_momentum + date_index * 0.001
            features.append(
                PersistedMomentumFeatureValue(
                    security_id=security_id,
                    asof_date=session,
                    value=feature_value,
                    available_at=_close(session),
                )
            )
            labels.append(
                PersistedForwardReturnLabel(
                    security_id=security_id,
                    label_date=session,
                    horizon_days=2,
                    realized_raw_return=feature_value * 0.05,
                    available_at=_close(sessions[date_index + 2]),
                )
            )
    return features, labels


def _calendar(session_count: int) -> tuple[tuple[TradingCalendarRow, ...], list[date]]:
    start = date(2024, 1, 2)
    sessions = [start + timedelta(days=offset) for offset in range(session_count)]
    rows = tuple(
        TradingCalendarRow(
            date=session,
            is_session=True,
            session_close=_close(session),
        )
        for session in sessions
    )
    return rows, sessions


def _close(day: date) -> datetime:
    return datetime.combine(day, time(21), tzinfo=timezone.utc)

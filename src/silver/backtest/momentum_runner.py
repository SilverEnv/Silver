"""Walk-forward runner for the first ``momentum_12_1`` backtest."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from math import isfinite, sqrt
from typing import Literal

from silver.backtest.walk_forward import (
    WalkForwardConfig,
    WalkForwardSplit,
    plan_walk_forward_splits,
)
from silver.features.momentum_12_1 import FEATURE_NAME, FEATURE_VERSION
from silver.time.trading_calendar import TradingCalendar, TradingCalendarRow


TargetKind = Literal["raw_return", "excess_return"]


class MomentumBacktestError(ValueError):
    """Raised when momentum backtest inputs would violate PIT rules."""


@dataclass(frozen=True, slots=True)
class PersistedMomentumFeatureValue:
    """Persisted ``feature_values`` row needed by the momentum runner."""

    security_id: int
    asof_date: date
    value: float
    available_at: datetime
    feature_name: str = FEATURE_NAME
    feature_version: int = FEATURE_VERSION


@dataclass(frozen=True, slots=True)
class PersistedForwardReturnLabel:
    """Persisted ``forward_return_labels`` row needed by the runner."""

    security_id: int
    label_date: date
    horizon_days: int
    realized_raw_return: float
    available_at: datetime
    label_version: int = 1
    realized_excess_return: float | None = None


@dataclass(frozen=True, slots=True)
class TransactionCostConfig:
    """Minimal execution assumptions carried by every backtest result."""

    half_spread_bps: float = 5.0
    market_impact_bps: float = 0.0
    borrow_cost_bps_annualized: float = 25.0
    assumed_turnover: float = 1.0
    fill_convention: str = "next_open"

    def __post_init__(self) -> None:
        _require_non_negative_finite(self.half_spread_bps, "half_spread_bps")
        _require_non_negative_finite(self.market_impact_bps, "market_impact_bps")
        _require_non_negative_finite(
            self.borrow_cost_bps_annualized,
            "borrow_cost_bps_annualized",
        )
        _require_non_negative_finite(self.assumed_turnover, "assumed_turnover")
        if not self.fill_convention.strip():
            raise MomentumBacktestError("fill_convention must be non-empty")

    def long_short_cost(self, horizon_days: int) -> float:
        """Return simple long-short cost drag as a decimal return."""

        traded_cost = (
            2.0
            * self.assumed_turnover
            * (self.half_spread_bps + self.market_impact_bps)
            / 10_000.0
        )
        borrow_cost = (
            self.borrow_cost_bps_annualized / 10_000.0 * horizon_days / 252.0
        )
        return traded_cost + borrow_cost


@dataclass(frozen=True, slots=True)
class MomentumBacktestConfig:
    """Configuration for the first deterministic momentum backtest runner."""

    horizon_days: int
    walk_forward: WalkForwardConfig
    target_kind: TargetKind = "raw_return"
    feature_name: str = FEATURE_NAME
    feature_version: int = FEATURE_VERSION
    label_version: int = 1
    long_short_quantile: float = 0.2
    transaction_costs: TransactionCostConfig = field(
        default_factory=TransactionCostConfig
    )

    def __post_init__(self) -> None:
        _require_positive_int(self.horizon_days, "horizon_days")
        _require_positive_int(self.feature_version, "feature_version")
        _require_positive_int(self.label_version, "label_version")
        if self.walk_forward.label_horizon_sessions != self.horizon_days:
            raise MomentumBacktestError(
                "walk_forward.label_horizon_sessions must match horizon_days"
            )
        if self.target_kind not in ("raw_return", "excess_return"):
            raise MomentumBacktestError("target_kind must be raw_return or excess_return")
        if not self.feature_name.strip():
            raise MomentumBacktestError("feature_name must be non-empty")
        if not 0 < self.long_short_quantile <= 0.5:
            raise MomentumBacktestError(
                "long_short_quantile must be greater than 0 and <= 0.5"
            )


@dataclass(frozen=True, slots=True)
class BacktestSampleCounts:
    """Paired and skipped sample counts for train/test windows."""

    train_pairs: int = 0
    test_pairs: int = 0
    skipped_train_missing_feature: int = 0
    skipped_train_missing_label: int = 0
    skipped_test_missing_feature: int = 0
    skipped_test_missing_label: int = 0


@dataclass(frozen=True, slots=True)
class LinearMomentumModel:
    """Deterministic one-feature linear relation fit inside a split."""

    intercept: float
    slope: float
    train_rank_correlation: float | None
    train_sample_count: int
    fitted: bool


@dataclass(frozen=True, slots=True)
class PredictiveMetrics:
    """Simple out-of-sample metrics for a score column."""

    rank_correlation: float | None
    long_short_spread_gross: float | None
    long_short_spread_net: float | None


@dataclass(frozen=True, slots=True)
class BaselineComparison:
    """Numeric comparison against the raw ``momentum_12_1`` rank baseline."""

    name: str
    metrics: PredictiveMetrics
    rank_correlation_delta: float | None
    long_short_spread_net_delta: float | None


@dataclass(frozen=True, slots=True)
class MomentumBacktestPrediction:
    """One scored out-of-sample feature/label pair."""

    split_index: int
    security_id: int
    asof_date: date
    feature_value: float
    prediction_score: float
    baseline_score: float
    realized_return: float
    label_available_at: datetime


@dataclass(frozen=True, slots=True)
class MomentumBacktestSplitResult:
    """Structured result for one walk-forward split."""

    split_index: int
    horizon_days: int
    train_window: tuple[date, date]
    test_window: tuple[date, date]
    train_label_outcome_end: date
    test_label_outcome_end: date
    sample_counts: BacktestSampleCounts
    model: LinearMomentumModel
    metrics: PredictiveMetrics
    baseline_comparison: BaselineComparison
    predictions: tuple[MomentumBacktestPrediction, ...]


@dataclass(frozen=True, slots=True)
class MomentumBacktestResult:
    """Structured in-memory result for the momentum walk-forward run."""

    horizon_days: int
    feature_name: str
    feature_version: int
    target_kind: TargetKind
    model_name: str
    sample_counts: BacktestSampleCounts
    metrics: PredictiveMetrics
    baseline_comparison: BaselineComparison
    transaction_costs: TransactionCostConfig
    splits: tuple[MomentumBacktestSplitResult, ...]
    predictions: tuple[MomentumBacktestPrediction, ...]


@dataclass(frozen=True, slots=True)
class _IndexedInputs:
    features_by_date: dict[date, dict[int, PersistedMomentumFeatureValue]]
    labels_by_date: dict[date, dict[int, PersistedForwardReturnLabel]]


@dataclass(frozen=True, slots=True)
class _PairedSample:
    security_id: int
    asof_date: date
    feature_value: float
    realized_return: float
    label_available_at: datetime


def run_momentum_12_1_backtest(
    *,
    features: Sequence[PersistedMomentumFeatureValue],
    labels: Sequence[PersistedForwardReturnLabel],
    calendar: TradingCalendar | Sequence[TradingCalendarRow],
    config: MomentumBacktestConfig,
) -> MomentumBacktestResult:
    """Run a deterministic walk-forward momentum backtest in memory.

    The runner consumes rows shaped like persisted ``feature_values`` and
    ``forward_return_labels`` records. It fails closed if a training label is
    not available before the split's test window, or if a test label is not
    available by the split's label-outcome boundary.
    """

    splits = plan_walk_forward_splits(calendar, config.walk_forward)
    indexed = _index_inputs(features=features, labels=labels, config=config)
    split_results = tuple(
        _run_split(split=split, indexed=indexed, config=config) for split in splits
    )
    predictions = tuple(
        prediction
        for split_result in split_results
        for prediction in split_result.predictions
    )
    metrics = _calculate_metrics(
        predictions=predictions,
        score=lambda prediction: prediction.prediction_score,
        config=config,
    )
    baseline_metrics = _calculate_metrics(
        predictions=predictions,
        score=lambda prediction: prediction.baseline_score,
        config=config,
    )

    return MomentumBacktestResult(
        horizon_days=config.horizon_days,
        feature_name=config.feature_name,
        feature_version=config.feature_version,
        target_kind=config.target_kind,
        model_name="momentum_12_1_univariate_ols",
        sample_counts=_sum_counts(
            split_result.sample_counts for split_result in split_results
        ),
        metrics=metrics,
        baseline_comparison=_compare_to_baseline(metrics, baseline_metrics),
        transaction_costs=config.transaction_costs,
        splits=split_results,
        predictions=predictions,
    )


def _run_split(
    *,
    split: WalkForwardSplit,
    indexed: _IndexedInputs,
    config: MomentumBacktestConfig,
) -> MomentumBacktestSplitResult:
    train_samples, train_counts = _paired_samples(
        split=split,
        sessions=split.train_sessions,
        indexed=indexed,
        config=config,
        phase="train",
    )
    test_samples, test_counts = _paired_samples(
        split=split,
        sessions=split.test_sessions,
        indexed=indexed,
        config=config,
        phase="test",
    )
    model = _fit_linear_momentum_model(train_samples)
    predictions = tuple(
        MomentumBacktestPrediction(
            split_index=split.index,
            security_id=sample.security_id,
            asof_date=sample.asof_date,
            feature_value=sample.feature_value,
            prediction_score=model.intercept + model.slope * sample.feature_value,
            baseline_score=sample.feature_value,
            realized_return=sample.realized_return,
            label_available_at=sample.label_available_at,
        )
        for sample in test_samples
    )
    metrics = _calculate_metrics(
        predictions=predictions,
        score=lambda prediction: prediction.prediction_score,
        config=config,
    )
    baseline_metrics = _calculate_metrics(
        predictions=predictions,
        score=lambda prediction: prediction.baseline_score,
        config=config,
    )

    return MomentumBacktestSplitResult(
        split_index=split.index,
        horizon_days=config.horizon_days,
        train_window=(split.train_start, split.train_end),
        test_window=(split.test_start, split.test_end),
        train_label_outcome_end=split.train_label_outcome_end,
        test_label_outcome_end=split.test_label_outcome_end,
        sample_counts=_add_counts(train_counts, test_counts),
        model=model,
        metrics=metrics,
        baseline_comparison=_compare_to_baseline(metrics, baseline_metrics),
        predictions=predictions,
    )


def _index_inputs(
    *,
    features: Sequence[PersistedMomentumFeatureValue],
    labels: Sequence[PersistedForwardReturnLabel],
    config: MomentumBacktestConfig,
) -> _IndexedInputs:
    features_by_date: dict[date, dict[int, PersistedMomentumFeatureValue]] = {}
    for feature in features:
        _validate_feature(feature)
        if (
            feature.feature_name != config.feature_name
            or feature.feature_version != config.feature_version
        ):
            continue
        by_security = features_by_date.setdefault(feature.asof_date, {})
        if feature.security_id in by_security:
            raise MomentumBacktestError(
                "duplicate momentum feature row for "
                f"security {feature.security_id} on {feature.asof_date.isoformat()}"
            )
        by_security[feature.security_id] = feature

    labels_by_date: dict[date, dict[int, PersistedForwardReturnLabel]] = {}
    for label in labels:
        _validate_label(label)
        if (
            label.horizon_days != config.horizon_days
            or label.label_version != config.label_version
        ):
            continue
        by_security = labels_by_date.setdefault(label.label_date, {})
        if label.security_id in by_security:
            raise MomentumBacktestError(
                "duplicate forward-return label row for "
                f"security {label.security_id} on {label.label_date.isoformat()} "
                f"horizon {label.horizon_days}"
            )
        by_security[label.security_id] = label

    return _IndexedInputs(features_by_date=features_by_date, labels_by_date=labels_by_date)


def _paired_samples(
    *,
    split: WalkForwardSplit,
    sessions: Sequence[date],
    indexed: _IndexedInputs,
    config: MomentumBacktestConfig,
    phase: Literal["train", "test"],
) -> tuple[tuple[_PairedSample, ...], BacktestSampleCounts]:
    samples: list[_PairedSample] = []
    missing_features = 0
    missing_labels = 0

    for session in sessions:
        features = indexed.features_by_date.get(session, {})
        labels = indexed.labels_by_date.get(session, {})
        for security_id in sorted(features.keys() | labels.keys()):
            feature = features.get(security_id)
            label = labels.get(security_id)
            if feature is None:
                missing_features += 1
                continue
            if label is None:
                missing_labels += 1
                continue

            _validate_feature_available_asof(feature)
            _validate_label_available_for_split(
                label=label,
                split=split,
                phase=phase,
            )
            samples.append(
                _PairedSample(
                    security_id=security_id,
                    asof_date=session,
                    feature_value=feature.value,
                    realized_return=_label_target(label, config.target_kind),
                    label_available_at=label.available_at,
                )
            )

    if phase == "train":
        counts = BacktestSampleCounts(
            train_pairs=len(samples),
            skipped_train_missing_feature=missing_features,
            skipped_train_missing_label=missing_labels,
        )
    else:
        counts = BacktestSampleCounts(
            test_pairs=len(samples),
            skipped_test_missing_feature=missing_features,
            skipped_test_missing_label=missing_labels,
        )
    return tuple(samples), counts


def _fit_linear_momentum_model(
    samples: Sequence[_PairedSample],
) -> LinearMomentumModel:
    if not samples:
        return LinearMomentumModel(
            intercept=0.0,
            slope=0.0,
            train_rank_correlation=None,
            train_sample_count=0,
            fitted=False,
        )

    xs = [sample.feature_value for sample in samples]
    ys = [sample.realized_return for sample in samples]
    mean_x = _mean(xs)
    mean_y = _mean(ys)
    centered_x = [value - mean_x for value in xs]
    denominator = sum(value * value for value in centered_x)
    if denominator == 0:
        return LinearMomentumModel(
            intercept=mean_y,
            slope=0.0,
            train_rank_correlation=_rank_correlation(xs, ys),
            train_sample_count=len(samples),
            fitted=False,
        )

    slope = sum(
        x_delta * (y - mean_y)
        for x_delta, y in zip(centered_x, ys, strict=True)
    ) / denominator
    return LinearMomentumModel(
        intercept=mean_y - slope * mean_x,
        slope=slope,
        train_rank_correlation=_rank_correlation(xs, ys),
        train_sample_count=len(samples),
        fitted=True,
    )


def _calculate_metrics(
    *,
    predictions: Sequence[MomentumBacktestPrediction],
    score: Callable[[MomentumBacktestPrediction], float],
    config: MomentumBacktestConfig,
) -> PredictiveMetrics:
    scores = [score(prediction) for prediction in predictions]
    realized_returns = [prediction.realized_return for prediction in predictions]
    gross_spread = _long_short_spread(
        predictions=predictions,
        score=score,
        quantile=config.long_short_quantile,
    )
    net_spread = (
        None
        if gross_spread is None
        else gross_spread - config.transaction_costs.long_short_cost(config.horizon_days)
    )
    return PredictiveMetrics(
        rank_correlation=_rank_correlation(scores, realized_returns),
        long_short_spread_gross=gross_spread,
        long_short_spread_net=net_spread,
    )


def _long_short_spread(
    *,
    predictions: Sequence[MomentumBacktestPrediction],
    score: Callable[[MomentumBacktestPrediction], float],
    quantile: float,
) -> float | None:
    predictions_by_date: dict[date, list[MomentumBacktestPrediction]] = {}
    for prediction in predictions:
        predictions_by_date.setdefault(prediction.asof_date, []).append(prediction)

    spreads: list[float] = []
    for asof_date in sorted(predictions_by_date):
        date_predictions = sorted(
            predictions_by_date[asof_date],
            key=lambda prediction: (score(prediction), prediction.security_id),
        )
        bucket_size = int(len(date_predictions) * quantile)
        bucket_size = max(1, bucket_size)
        if bucket_size * 2 > len(date_predictions):
            bucket_size = len(date_predictions) // 2
        if bucket_size < 1:
            continue

        short_bucket = date_predictions[:bucket_size]
        long_bucket = date_predictions[-bucket_size:]
        spreads.append(
            _mean(prediction.realized_return for prediction in long_bucket)
            - _mean(prediction.realized_return for prediction in short_bucket)
        )

    if not spreads:
        return None
    return _mean(spreads)


def _compare_to_baseline(
    metrics: PredictiveMetrics,
    baseline_metrics: PredictiveMetrics,
) -> BaselineComparison:
    return BaselineComparison(
        name="momentum_12_1_rank",
        metrics=baseline_metrics,
        rank_correlation_delta=_difference_or_none(
            metrics.rank_correlation,
            baseline_metrics.rank_correlation,
        ),
        long_short_spread_net_delta=_difference_or_none(
            metrics.long_short_spread_net,
            baseline_metrics.long_short_spread_net,
        ),
    )


def _rank_correlation(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    if len(xs) != len(ys):
        raise MomentumBacktestError("rank correlation inputs must have equal length")
    if len(xs) < 2:
        return None
    return _pearson(_average_ranks(xs), _average_ranks(ys))


def _average_ranks(values: Sequence[float]) -> tuple[float, ...]:
    indexed = sorted((value, index) for index, value in enumerate(values))
    ranks = [0.0] * len(indexed)
    start = 0
    while start < len(indexed):
        end = start + 1
        while end < len(indexed) and indexed[end][0] == indexed[start][0]:
            end += 1
        average_rank = (start + 1 + end) / 2.0
        for _, original_index in indexed[start:end]:
            ranks[original_index] = average_rank
        start = end
    return tuple(ranks)


def _pearson(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    mean_x = _mean(xs)
    mean_y = _mean(ys)
    x_deltas = [value - mean_x for value in xs]
    y_deltas = [value - mean_y for value in ys]
    denominator = sqrt(
        sum(value * value for value in x_deltas)
        * sum(value * value for value in y_deltas)
    )
    if denominator == 0:
        return None
    return sum(x * y for x, y in zip(x_deltas, y_deltas, strict=True)) / denominator


def _label_target(label: PersistedForwardReturnLabel, target_kind: TargetKind) -> float:
    if target_kind == "raw_return":
        return label.realized_raw_return
    if label.realized_excess_return is None:
        raise MomentumBacktestError(
            "realized_excess_return is required for excess_return target"
        )
    return label.realized_excess_return


def _validate_feature(feature: PersistedMomentumFeatureValue) -> None:
    _require_positive_int(feature.security_id, "feature.security_id")
    _require_aware(feature.available_at, "feature.available_at")
    _require_finite(feature.value, "feature.value")
    _require_positive_int(feature.feature_version, "feature.feature_version")
    if not feature.feature_name.strip():
        raise MomentumBacktestError("feature.feature_name must be non-empty")


def _validate_label(label: PersistedForwardReturnLabel) -> None:
    _require_positive_int(label.security_id, "label.security_id")
    _require_positive_int(label.horizon_days, "label.horizon_days")
    _require_positive_int(label.label_version, "label.label_version")
    _require_aware(label.available_at, "label.available_at")
    _require_finite(label.realized_raw_return, "label.realized_raw_return")
    if label.realized_excess_return is not None:
        _require_finite(label.realized_excess_return, "label.realized_excess_return")


def _validate_feature_available_asof(
    feature: PersistedMomentumFeatureValue,
) -> None:
    if feature.available_at.date() > feature.asof_date:
        raise MomentumBacktestError(
            "feature row for "
            f"security {feature.security_id} on {feature.asof_date.isoformat()} "
            "is not available as of its asof_date"
        )


def _validate_label_available_for_split(
    *,
    label: PersistedForwardReturnLabel,
    split: WalkForwardSplit,
    phase: Literal["train", "test"],
) -> None:
    if phase == "train":
        if label.available_at.date() >= split.test_start:
            raise MomentumBacktestError(
                "label for "
                f"security {label.security_id} on {label.label_date.isoformat()} "
                f"horizon {label.horizon_days} is unavailable before split "
                f"{split.index} test_start {split.test_start.isoformat()}"
            )
        return

    if label.available_at.date() > split.test_label_outcome_end:
        raise MomentumBacktestError(
            "label for "
            f"security {label.security_id} on {label.label_date.isoformat()} "
            f"horizon {label.horizon_days} is unavailable by split {split.index} "
            f"test_label_outcome_end {split.test_label_outcome_end.isoformat()}"
        )


def _add_counts(
    left: BacktestSampleCounts,
    right: BacktestSampleCounts,
) -> BacktestSampleCounts:
    return BacktestSampleCounts(
        train_pairs=left.train_pairs + right.train_pairs,
        test_pairs=left.test_pairs + right.test_pairs,
        skipped_train_missing_feature=(
            left.skipped_train_missing_feature
            + right.skipped_train_missing_feature
        ),
        skipped_train_missing_label=(
            left.skipped_train_missing_label + right.skipped_train_missing_label
        ),
        skipped_test_missing_feature=(
            left.skipped_test_missing_feature + right.skipped_test_missing_feature
        ),
        skipped_test_missing_label=(
            left.skipped_test_missing_label + right.skipped_test_missing_label
        ),
    )


def _sum_counts(counts: Iterable[BacktestSampleCounts]) -> BacktestSampleCounts:
    total = BacktestSampleCounts()
    for count in counts:
        total = _add_counts(total, count)
    return total


def _difference_or_none(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return left - right


def _mean(values: Iterable[float]) -> float:
    values_tuple = tuple(values)
    if not values_tuple:
        raise MomentumBacktestError("cannot calculate mean of empty sequence")
    return sum(values_tuple) / len(values_tuple)


def _require_positive_int(value: int, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise MomentumBacktestError(f"{field} must be a positive integer")


def _require_finite(value: float, field: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not isfinite(value)
    ):
        raise MomentumBacktestError(f"{field} must be finite")


def _require_non_negative_finite(value: float, field: str) -> None:
    _require_finite(value, field)
    if value < 0:
        raise MomentumBacktestError(f"{field} must be non-negative")


def _require_aware(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise MomentumBacktestError(f"{field} must be timezone-aware")

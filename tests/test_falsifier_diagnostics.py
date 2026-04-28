from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

from silver.analytics.falsifier_diagnostics import (
    load_falsifier_input_diagnostics,
    render_falsifier_input_diagnostics,
)


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_falsifier_inputs.py"


def test_sufficient_diagnostics_report_usable_range_and_ticker_coverage() -> None:
    client = FakeJsonClient(
        {
            "feature_definition": {
                "id": 7,
                "name": "momentum_12_1",
                "version": 1,
                "definition_hash": "a" * 64,
            },
            "ticker_coverage": [
                _coverage_row(
                    1,
                    "AAPL",
                    price_rows=4,
                    feature_rows=2,
                    label_rows=2,
                    joined_rows=2,
                    joined_start="2024-01-02",
                    joined_end="2024-01-03",
                ),
                _coverage_row(
                    2,
                    "MSFT",
                    price_rows=4,
                    feature_rows=2,
                    label_rows=2,
                    joined_rows=2,
                    joined_start="2024-01-02",
                    joined_end="2024-01-03",
                ),
            ],
            "horizon_coverage": [
                {
                    "horizon_days": 63,
                    "row_count": 4,
                    "ticker_count": 2,
                    "start_date": "2024-01-02",
                    "end_date": "2024-01-03",
                }
            ],
        }
    )

    diagnostics = load_falsifier_input_diagnostics(
        client,
        universe="falsifier_seed",
        horizon=63,
    )
    rendered = render_falsifier_input_diagnostics(diagnostics)

    assert diagnostics.is_sufficient
    assert "Status: SUFFICIENT" in rendered
    assert "- As-of date range: 2024-01-02 to 2024-01-03" in rendered
    assert "- Ticker coverage: 2/2 (AAPL, MSFT)" in rendered
    assert "- label horizons with no rows: 5, 21, 126, 252" in rendered
    assert client.sql is not None
    assert client.sql.startswith("WITH universe_rows AS")
    assert "INSERT " not in client.sql
    assert "UPDATE " not in client.sql
    assert "DELETE " not in client.sql


def test_insufficient_diagnostics_name_missing_inputs_exactly() -> None:
    client = FakeJsonClient(
        {
            "feature_definition": {
                "id": 7,
                "name": "momentum_12_1",
                "version": 1,
                "definition_hash": "b" * 64,
            },
            "ticker_coverage": [
                _coverage_row(
                    1,
                    "AAPL",
                    price_rows=0,
                    feature_rows=0,
                    label_rows=2,
                    joined_rows=0,
                    label_without_feature_rows=2,
                ),
                _coverage_row(
                    2,
                    "MSFT",
                    price_rows=3,
                    feature_rows=0,
                    label_rows=2,
                    joined_rows=0,
                    label_without_feature_rows=2,
                ),
                _coverage_row(
                    3,
                    "NVDA",
                    price_rows=3,
                    feature_rows=2,
                    label_rows=0,
                    joined_rows=0,
                    feature_without_label_rows=2,
                ),
            ],
            "horizon_coverage": [
                {
                    "horizon_days": 63,
                    "row_count": 4,
                    "ticker_count": 2,
                    "start_date": "2024-01-02",
                    "end_date": "2024-01-03",
                }
            ],
        }
    )

    diagnostics = load_falsifier_input_diagnostics(
        client,
        universe="falsifier_seed",
        horizon=63,
    )
    rendered = render_falsifier_input_diagnostics(diagnostics)

    assert not diagnostics.is_sufficient
    assert "Status: INSUFFICIENT" in rendered
    assert "- prices_daily rows missing for tickers: AAPL" in rendered
    assert (
        "- feature_values rows for `momentum_12_1` missing for tickers: "
        "AAPL, MSFT"
    ) in rendered
    assert (
        "- forward_return_labels horizon 63 rows missing for tickers: NVDA"
    ) in rendered
    assert (
        "- no usable feature/label as-of overlap rows exist for horizon 63"
    ) in rendered
    assert (
        "- requested-horizon label rows without feature values: AAPL=2, MSFT=2"
    ) in rendered
    assert "- feature rows without requested-horizon labels: NVDA=2" in rendered


def test_diagnostics_report_missing_feature_definition() -> None:
    client = FakeJsonClient(
        {
            "feature_definition": None,
            "ticker_coverage": [
                _coverage_row(
                    1,
                    "AAPL",
                    price_rows=3,
                    feature_rows=0,
                    label_rows=2,
                    joined_rows=0,
                    label_without_feature_rows=2,
                )
            ],
            "horizon_coverage": [
                {
                    "horizon_days": 63,
                    "row_count": 2,
                    "ticker_count": 1,
                    "start_date": "2024-01-02",
                    "end_date": "2024-01-03",
                }
            ],
        }
    )

    diagnostics = load_falsifier_input_diagnostics(
        client,
        universe="falsifier_seed",
        horizon=63,
    )
    rendered = render_falsifier_input_diagnostics(diagnostics)

    assert not diagnostics.is_sufficient
    assert "- feature definition `momentum_12_1` is not persisted" in rendered


def test_check_cli_validates_without_database_url() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--check"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert "OK: falsifier input diagnostics check passed" in result.stdout


def test_apply_cli_requires_database_url() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT)],
        text=True,
        capture_output=True,
        check=False,
        env={},
    )

    assert result.returncode == 1
    assert "DATABASE_URL is required unless --check is used" in result.stderr


def _coverage_row(
    security_id: int,
    ticker: str,
    *,
    price_rows: int,
    feature_rows: int,
    label_rows: int,
    joined_rows: int,
    joined_start: str | None = None,
    joined_end: str | None = None,
    label_without_feature_rows: int = 0,
    feature_without_label_rows: int = 0,
) -> dict[str, Any]:
    return {
        "security_id": security_id,
        "ticker": ticker,
        "valid_from": "2014-04-03",
        "valid_to": None,
        "price_rows": price_rows,
        "price_start": "2024-01-02" if price_rows else None,
        "price_end": "2024-01-05" if price_rows else None,
        "feature_rows": feature_rows,
        "feature_start": "2024-01-02" if feature_rows else None,
        "feature_end": "2024-01-03" if feature_rows else None,
        "label_rows": label_rows,
        "label_start": "2024-01-02" if label_rows else None,
        "label_end": "2024-01-03" if label_rows else None,
        "joined_rows": joined_rows,
        "joined_start": joined_start,
        "joined_end": joined_end,
        "label_without_feature_rows": label_without_feature_rows,
        "feature_without_label_rows": feature_without_label_rows,
    }


class FakeJsonClient:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.sql: str | None = None

    def fetch_json(self, sql: str) -> Any:
        self.sql = sql
        return self.payload

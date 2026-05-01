from __future__ import annotations

import importlib.util
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from silver.ingest import FmpFundamentalsIngestError, RawVault, ingest_fmp_fundamentals
from silver.sources.fmp import FMPClient, FMPTransportResponse


CLI_PATH = Path(__file__).resolve().parents[1] / "scripts" / "ingest_fmp_fundamentals.py"
CLI_SPEC = importlib.util.spec_from_file_location("ingest_fmp_fundamentals_cli", CLI_PATH)
assert CLI_SPEC is not None
assert CLI_SPEC.loader is not None
cli = importlib.util.module_from_spec(CLI_SPEC)
CLI_SPEC.loader.exec_module(cli)

SEED_TICKERS = ("AAPL", "MSFT")
FILING_RULE = {
    "type": "next_trading_session_time_after_timestamp",
    "base": "accepted_at",
    "trading_days_offset": 1,
    "time": "09:30",
    "timezone": "America/New_York",
    "calendar": "NYSE",
}


def test_ingest_raw_vaults_responses_then_writes_selected_metrics() -> None:
    connection = FakeConnection(tickers=("AAPL",))
    client = _client(
        connection,
        [
            FMPTransportResponse(
                status_code=200,
                body=json.dumps([_income_row("AAPL")]).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            ),
            FMPTransportResponse(
                status_code=200,
                body=json.dumps([_cash_flow_row("AAPL")]).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            ),
        ],
    )

    result = ingest_fmp_fundamentals(
        connection=connection,
        client=client,
        universe="falsifier_seed",
        tickers=("AAPL",),
        statement_types=("income_statement", "cash_flow_statement"),
        period_types=("annual",),
        code_git_sha="abc1234",
        sleep_seconds=0,
    )

    assert result.tickers == ("AAPL",)
    assert result.raw_responses_captured == 2
    assert result.rows_written == 8
    assert connection.analytics_runs[0]["run_kind"] == "fmp_fundamentals_normalization"
    assert connection.analytics_runs[0]["available_at_policy_versions"] == {
        "sec_10k_filing": 1,
        "sec_10q_filing": 1,
    }
    assert connection.analytics_runs[0]["status"] == "succeeded"
    assert len(connection.raw_objects) == 2
    assert len(connection.fundamental_values) == 8
    assert {
        row["metric_name"] for row in connection.fundamental_values
    } == {
        "revenue",
        "gross_profit",
        "operating_income",
        "net_income",
        "diluted_weighted_average_shares",
        "operating_cash_flow",
        "capital_expenditure",
        "free_cash_flow",
    }
    assert all(row["available_at_policy_id"] == 2 for row in connection.fundamental_values)

    first_fundamental = connection.events.index("fundamental:revenue")
    assert connection.events.index("raw:AAPL:annual") < first_fundamental
    assert connection.events.index("raw:AAPL:annual:2") < first_fundamental


def test_parse_failure_keeps_raw_capture_and_marks_run_failed_without_values() -> None:
    connection = FakeConnection(tickers=("AAPL",))
    bad_row = _income_row("AAPL")
    del bad_row["acceptedDate"]
    client = _client(
        connection,
        [
            FMPTransportResponse(
                status_code=200,
                body=json.dumps([bad_row]).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
        ],
    )

    with pytest.raises(FmpFundamentalsIngestError, match="acceptedDate"):
        ingest_fmp_fundamentals(
            connection=connection,
            client=client,
            universe="falsifier_seed",
            tickers=("AAPL",),
            statement_types=("income_statement",),
            period_types=("annual",),
            code_git_sha="abc1234",
            sleep_seconds=0,
        )

    assert len(connection.raw_objects) == 1
    assert connection.fundamental_values == []
    assert connection.analytics_runs[0]["status"] == "failed"


def test_dry_run_reads_persisted_universe_without_fetching_or_writing() -> None:
    connection = FakeConnection()

    result = ingest_fmp_fundamentals(
        connection=connection,
        client=None,
        universe="falsifier_seed",
        limit=1,
        statement_types=("income_statement",),
        period_types=("annual",),
        code_git_sha="abc1234",
        dry_run=True,
    )

    assert result.dry_run is True
    assert result.tickers == ("AAPL",)
    assert result.planned_requests == 1
    assert connection.raw_objects == []
    assert connection.fundamental_values == []
    assert connection.analytics_runs == []


def test_check_config_uses_seed_reference_data() -> None:
    message = cli.check_config(
        universe="falsifier_seed",
        tickers=("AAPL", "MSFT"),
        limit=None,
        statements=("income_statement",),
        periods=("annual", "quarterly"),
        seed_config_path=cli.DEFAULT_CONFIG_PATH,
    )

    assert message == (
        "OK: checked FMP fundamentals ingest config for falsifier_seed "
        "with 2 seed ticker(s), statements=income_statement, "
        "periods=annual,quarterly, planned_requests=4"
    )


def test_missing_fmp_api_key_fails_before_database_connection(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = "postgresql://user:password@localhost:5432/silver"
    monkeypatch.delenv("FMP_API_KEY", raising=False)

    code = cli.main(["--database-url", database_url, "--ticker", "AAPL"])

    assert code == 1
    captured = capsys.readouterr()
    assert "FMP_API_KEY is required" in captured.err
    assert database_url not in captured.err


def _client(
    connection: FakeConnection,
    responses: list[FMPTransportResponse],
) -> FMPClient:
    return FMPClient(
        api_key="real-secret",
        raw_vault=RawVault(connection),
        transport=FakeTransport(responses),
        now=lambda: datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
        sleep=lambda _seconds: None,
    )


def _income_row(ticker: str) -> dict[str, object]:
    return {
        "symbol": ticker,
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


def _cash_flow_row(ticker: str) -> dict[str, object]:
    return {
        "symbol": ticker,
        "date": "2024-09-28",
        "calendarYear": "2024",
        "period": "FY",
        "reportedCurrency": "USD",
        "filingDate": "2024-11-01",
        "acceptedDate": "2024-11-01 06:01:36",
        "operatingCashFlow": 118254000000,
        "capitalExpenditure": -9447000000,
        "freeCashFlow": 108807000000,
    }


class FakeTransport:
    def __init__(self, responses: list[FMPTransportResponse]) -> None:
        self._responses = list(responses)

    def get(self, url: str, *, timeout: float) -> FMPTransportResponse:
        if not self._responses:
            raise AssertionError(f"unexpected request: {url} {timeout}")
        return self._responses.pop(0)


class FakeConnection:
    def __init__(self, *, tickers: tuple[str, ...] = SEED_TICKERS) -> None:
        self.securities = {
            ticker: security_id
            for security_id, ticker in enumerate(tickers, start=101)
        }
        self.memberships = [
            {
                "security_id": security_id,
                "ticker": ticker,
                "universe_name": "falsifier_seed",
                "valid_from": date(2014, 4, 3),
                "valid_to": None,
            }
            for ticker, security_id in self.securities.items()
        ]
        self.calendar = [
            {
                "date": date(2024, 11, 1),
                "is_session": True,
                "session_close": datetime(2024, 11, 1, 20, 0, tzinfo=timezone.utc),
                "is_early_close": False,
            },
            {
                "date": date(2024, 11, 2),
                "is_session": False,
                "session_close": None,
                "is_early_close": False,
            },
            {
                "date": date(2024, 11, 3),
                "is_session": False,
                "session_close": None,
                "is_early_close": False,
            },
            {
                "date": date(2024, 11, 4),
                "is_session": True,
                "session_close": datetime(2024, 11, 4, 21, 0, tzinfo=timezone.utc),
                "is_early_close": False,
            },
        ]
        self.policies = {
            2: {
                "id": 2,
                "name": "sec_10k_filing",
                "version": 1,
                "rule": FILING_RULE,
            },
            3: {
                "id": 3,
                "name": "sec_10q_filing",
                "version": 1,
                "rule": FILING_RULE,
            },
        }
        self.raw_objects: list[dict[str, Any]] = []
        self.fundamental_values: list[dict[str, Any]] = []
        self.analytics_runs: list[dict[str, Any]] = []
        self.events: list[str] = []
        self.commits = 0
        self.rollbacks = 0
        self._next_raw_id = 1
        self._next_run_id = 1

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1
        self.events.append("commit")

    def rollback(self) -> None:
        self.rollbacks += 1
        self.events.append("rollback")

    def next_raw_id(self) -> int:
        raw_object_id = self._next_raw_id
        self._next_raw_id += 1
        return raw_object_id

    def next_run_id(self) -> int:
        run_id = self._next_run_id
        self._next_run_id += 1
        return run_id


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self._one: dict[str, Any] | tuple[int] | None = None
        self._many: list[dict[str, Any]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, sql: str, params: dict[str, Any]) -> None:
        if sql.startswith("SELECT\n    security.id AS security_id"):
            self._select_members(params)
            return
        if sql.startswith("SELECT id, name, version, rule"):
            self._select_policies(params)
            return
        if sql.startswith("SELECT date, is_session"):
            self._many = list(self.connection.calendar)
            return
        if sql.startswith("INSERT INTO silver.analytics_runs"):
            self._insert_run(params)
            return
        if sql.startswith("UPDATE silver.analytics_runs"):
            self._finish_run(params)
            return
        if sql.startswith("INSERT INTO silver.raw_objects"):
            self._insert_raw(params)
            return
        if sql.startswith("SELECT id\nFROM silver.raw_objects"):
            self._select_raw(params)
            return
        if sql.startswith("INSERT INTO silver.fundamental_values"):
            self._upsert_fundamental(params)
            return
        raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self) -> dict[str, Any] | tuple[int] | None:
        return self._one

    def fetchall(self) -> list[dict[str, Any]]:
        return self._many

    def _select_members(self, params: dict[str, Any]) -> None:
        self._many = [
            membership
            for membership in sorted(
                self.connection.memberships,
                key=lambda item: (item["ticker"], item["valid_from"]),
            )
            if membership["universe_name"] == params["universe_name"]
        ]

    def _select_policies(self, params: dict[str, Any]) -> None:
        names = set(params["names"])
        version = params["version"]
        self._many = [
            policy
            for policy in self.connection.policies.values()
            if policy["name"] in names and policy["version"] == version
        ]

    def _insert_run(self, params: dict[str, Any]) -> None:
        row = {
            "id": self.connection.next_run_id(),
            "run_kind": params["run_kind"],
            "status": "running",
            "available_at_policy_versions": json.loads(
                params["available_at_policy_versions"]
            ),
            "parameters": json.loads(params["parameters"]),
            "input_fingerprints": json.loads(params["input_fingerprints"]),
        }
        self.connection.analytics_runs.append(row)
        self.connection.events.append("run:create")
        self._one = row

    def _finish_run(self, params: dict[str, Any]) -> None:
        run_id = params["run_id"]
        for row in self.connection.analytics_runs:
            if row["id"] == run_id:
                row["status"] = params["status"]
                self.connection.events.append(f"run:{params['status']}")
                self._one = row
                return
        self._one = None

    def _insert_raw(self, params: dict[str, Any]) -> None:
        existing = self._find_raw(params)
        if existing is not None:
            self._one = None
            return
        row = dict(params)
        row["id"] = self.connection.next_raw_id()
        row["params"] = json.loads(params["params"])
        row["metadata"] = json.loads(params["metadata"])
        raw_count = len(self.connection.raw_objects) + 1
        self.connection.raw_objects.append(row)
        self.connection.events.append(
            f"raw:{row['params']['symbol']}:{row['params']['period']}"
            + ("" if raw_count == 1 else f":{raw_count}")
        )
        self._one = (row["id"],)

    def _select_raw(self, params: dict[str, Any]) -> None:
        existing = self._find_raw(params)
        self._one = None if existing is None else (existing["id"],)

    def _find_raw(self, params: dict[str, Any]) -> dict[str, Any] | None:
        for row in self.connection.raw_objects:
            if (
                row["vendor"] == params["vendor"]
                and row["endpoint"] == params["endpoint"]
                and row["params_hash"] == params["params_hash"]
                and row["raw_hash"] == params["raw_hash"]
            ):
                return row
        return None

    def _upsert_fundamental(self, params: dict[str, Any]) -> None:
        row = dict(params)
        row["metadata"] = json.loads(params["metadata"])
        self.connection.fundamental_values.append(row)
        self.connection.events.append(f"fundamental:{row['metric_name']}")

"""Юнит-тесты метрик запусков (REQ-007): запись в dag_metrics, callback."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

import shared.metrics as metrics  # noqa: E402


class FakePostgresHook:
    def __init__(self):
        self.calls = []

    def run(self, sql, parameters=None):
        self.calls.append((sql, parameters))


def _insert_call(fake):
    return [c for c in fake.calls if "INSERT INTO dag_metrics" in c[0]][0]


def _fake_config(monkeypatch):
    monkeypatch.setattr(
        metrics, "DAGConfig", lambda: SimpleNamespace(get=lambda key, **kw: "pc")
    )


def test_record_metric_inserts_row(monkeypatch):
    fake = FakePostgresHook()
    monkeypatch.setattr(metrics, "PostgresHook", lambda **kw: fake)

    metrics.record_metric(
        "daily_sales_report", "run-1", "success", 12.5, rows_written=20, conn_id="pc"
    )

    sql, params = _insert_call(fake)
    assert "INSERT INTO dag_metrics" in sql
    assert params == ("daily_sales_report", "run-1", "success", 12.5, 20, None)


def test_record_metric_creates_table_first(monkeypatch):
    fake = FakePostgresHook()
    monkeypatch.setattr(metrics, "PostgresHook", lambda **kw: fake)

    metrics.record_metric("d", "r", "success", 1.0, conn_id="pc")

    assert "CREATE TABLE IF NOT EXISTS dag_metrics" in fake.calls[0][0]


def test_record_failure_metric_from_context(monkeypatch):
    fake = FakePostgresHook()
    monkeypatch.setattr(metrics, "PostgresHook", lambda **kw: fake)
    _fake_config(monkeypatch)
    start = datetime(2026, 9, 5, 18, 0, tzinfo=UTC)
    end = datetime(2026, 9, 5, 18, 5, tzinfo=UTC)
    context = {
        "dag_run": SimpleNamespace(
            dag_id="daily_sales_report",
            run_id="run-f",
            start_date=start,
            end_date=end,
            logical_date=None,
        )
    }

    metrics.record_failure_metric(context)

    sql, params = _insert_call(fake)
    assert params[2] == "failed"
    assert params[3] == 300.0  # 5 минут


def test_record_failure_metric_no_dag_run_is_noop(monkeypatch):
    fake = FakePostgresHook()
    monkeypatch.setattr(metrics, "PostgresHook", lambda **kw: fake)

    metrics.record_failure_metric({})

    assert fake.calls == []


def test_on_failure_callback_combines_alert_and_metric(monkeypatch, caplog):
    fake = FakePostgresHook()
    monkeypatch.setattr(metrics, "PostgresHook", lambda **kw: fake)
    _fake_config(monkeypatch)
    start = datetime(2026, 9, 5, 18, 0, tzinfo=UTC)
    end = datetime(2026, 9, 5, 18, 5, tzinfo=UTC)
    context = {
        "dag_run": SimpleNamespace(
            dag_id="d", run_id="r", start_date=start, end_date=end, logical_date=None
        ),
        "ti": SimpleNamespace(dag_id="d", task_id="validate_data"),
        "dag": SimpleNamespace(dag_id="d"),
        "exception": ValueError("boom"),
    }

    with caplog.at_level("ERROR", logger="shared.alerts"):
        metrics.on_failure_callback(context)

    assert any("AIRFLOW ALERT" in r.message for r in caplog.records)
    sql, params = _insert_call(fake)
    assert params[2] == "failed"


def test_record_metrics_task_writes_success(monkeypatch):
    fake = FakePostgresHook()
    monkeypatch.setattr(metrics, "PostgresHook", lambda **kw: fake)
    _fake_config(monkeypatch)
    task = metrics.make_record_metrics_task()
    dag_run = SimpleNamespace(
        dag_id="daily_sales_report",
        run_id="run-ok",
        start_date=datetime(2026, 9, 5, 18, 0, tzinfo=UTC),
        logical_date=None,
    )

    msg = task.function(
        [
            ["sale_date", "product_id", "revenue", "orders"],
            ["2026-09-04", "p1", "10", "1"],
        ],
        dag_run=dag_run,
    )

    assert msg == "metrics recorded: 1 rows"
    sql, params = _insert_call(fake)
    assert params[2] == "success"
    assert params[4] == 1

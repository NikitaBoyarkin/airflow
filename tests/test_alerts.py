"""Юнит-тесты алертинга (REQ-004): формат и callback."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from shared.alerts import format_alert, on_failure_callback  # noqa: E402


class FakeTI:
    dag_id = "daily_sales_report"
    task_id = "fetch_sales"


def make_context(**extra):
    ctx = {
        "ti": FakeTI(),
        "logical_date": "2026-09-04T00:00:00+00:00",
        "exception": ValueError("boom"),
    }
    ctx.update(extra)
    return ctx


def test_format_alert_contains_core_fields():
    msg = format_alert(make_context())
    assert "AIRFLOW ALERT" in msg
    assert "daily_sales_report" in msg
    assert "fetch_sales" in msg
    assert "2026-09-04" in msg
    assert "boom" in msg


def test_format_alert_missing_context_not_crash():
    msg = format_alert({})
    assert "AIRFLOW ALERT" in msg


def test_on_failure_callback_logs_without_raise(caplog):
    import logging

    with caplog.at_level(logging.ERROR):
        on_failure_callback(make_context())
    assert any("AIRFLOW ALERT" in r.message for r in caplog.records)

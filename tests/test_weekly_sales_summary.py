"""Импорт DAG и структура графа (REQ-009).

Проверяем: weekly_sales_summary загружается (airflow.sdk API), четыре задачи
выстроены цепочкой fetch_weekly -> validate_weekly -> write_weekly ->
record_metrics, переиспользует shared-модули (config, dq, metrics, output,
windows), catchup=False (REQ-008), а приёмник выбирается по output
("sheets" -> GSheetsHook, "file" -> CSV).
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

import weekly_sales_summary  # noqa: E402

WEEKLY_HEADER = ["category", "revenue", "orders"]
WEEKLY_DATA = [WEEKLY_HEADER, ["electronics", "1250.5", "7"]]


class FakeSheetsHook:
    def __init__(self, **kwargs):
        self.init_kwargs = kwargs
        self.calls = []

    def update_values(self, **kwargs):
        self.calls.append(kwargs)


def _no_hook(**kwargs):
    raise AssertionError("CSV-режим не должен создавать GSheetsHook")


def _no_csv(*args, **kwargs):
    raise AssertionError("output=sheets не должен писать CSV")


def dag():
    return weekly_sales_summary.dag


def test_dag_id():
    assert dag().dag_id == "weekly_sales_summary"


def test_dag_schedule():
    assert dag().schedule == "@weekly"


def test_dag_tags_include_weekly():
    assert "weekly" in (dag().tags or [])


def test_dag_catchup_disabled():
    assert dag().catchup is False


def test_on_failure_callback_wired():
    assert dag().on_failure_callback is not None


def test_dag_has_four_tasks():
    ids = {t.task_id for t in dag().tasks}
    assert ids == {"fetch_weekly", "validate_weekly", "write_weekly", "record_metrics"}


def test_dependency_chain():
    metrics_task = dag().get_task("record_metrics")
    write = dag().get_task("write_weekly")
    validate = dag().get_task("validate_weekly")
    assert "write_weekly" in {t.task_id for t in metrics_task.upstream_list}
    assert "validate_weekly" in {t.task_id for t in write.upstream_list}
    assert "fetch_weekly" in {t.task_id for t in validate.upstream_list}


def test_reuses_shared_modules():
    import shared.dq
    import shared.metrics
    import shared.output
    import shared.windows

    assert weekly_sales_summary.validate_or_raise is shared.dq.validate_or_raise
    assert weekly_sales_summary.write_csv is shared.output.write_csv
    assert (
        weekly_sales_summary.make_record_metrics_task
        is shared.metrics.make_record_metrics_task
    )
    assert (
        weekly_sales_summary.on_failure_callback is shared.metrics.on_failure_callback
    )
    assert weekly_sales_summary.weekly_window is shared.windows.weekly_window


# --- Приёмник: output="sheets" | "file" ---


def test_write_weekly_rows_file_mode_writes_csv(monkeypatch, tmp_path):
    """output="file" — CSV через write_weekly_file, Sheets не создаётся."""
    monkeypatch.setattr(weekly_sales_summary, "GSheetsHook", _no_hook)

    result = weekly_sales_summary.write_weekly_rows(
        spreadsheet_id="",
        range_name="Сводка!A1",
        gcp_conn_id="gcp",
        data=WEEKLY_DATA,
        week_start="2026-08-30",
        output="file",
        out_dir=tmp_path,
    )

    assert result is WEEKLY_DATA  # pass-through: record_metrics считает строки
    content = (tmp_path / "weekly_sales_summary_2026-08-30.csv").read_text(
        encoding="utf-8"
    )
    assert "category,revenue,orders" in content
    assert "electronics,1250.5,7" in content


def test_write_weekly_rows_sheets_mode_updates_sheet(monkeypatch, tmp_path):
    """output="sheets" — один вызов update_values, CSV не пишется."""
    fake = FakeSheetsHook()
    monkeypatch.setattr(weekly_sales_summary, "GSheetsHook", lambda **kw: fake)
    monkeypatch.setattr(weekly_sales_summary, "write_csv", _no_csv)

    result = weekly_sales_summary.write_weekly_rows(
        spreadsheet_id="spread-123",
        range_name="Сводка!A1",
        gcp_conn_id="gcp",
        data=WEEKLY_DATA,
        week_start="2026-08-30",
        output="sheets",
        out_dir=tmp_path,
    )

    assert result is WEEKLY_DATA  # pass-through
    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call["spreadsheet_id"] == "spread-123"
    assert call["range_"] == "Сводка!A1"
    assert call["values"] == WEEKLY_DATA
    assert list(tmp_path.iterdir()) == []


def test_write_weekly_task_dispatches_from_config(monkeypatch):
    """Таск write_weekly берёт output/range/spreadsheet из weekly_summary()."""
    fake = FakeSheetsHook()
    monkeypatch.setattr(weekly_sales_summary, "GSheetsHook", lambda **kw: fake)
    monkeypatch.setattr(
        weekly_sales_summary,
        "DAGConfig",
        lambda: SimpleNamespace(
            weekly_summary=lambda: {
                "output": "sheets",
                "spreadsheet_id": "spread-123",
                "range_name": "Сводка!A1",
                "postgres_conn_id": "pc",
                "google_conn_id": "gcp",
            }
        ),
    )

    class FakeDagRun:
        data_interval_start = datetime(2026, 8, 30, tzinfo=UTC)
        run_after = datetime(2026, 9, 6, tzinfo=UTC)

    result = weekly_sales_summary.write_weekly.function(
        WEEKLY_DATA, dag_run=FakeDagRun()
    )

    assert result is WEEKLY_DATA
    assert fake.calls[0]["spreadsheet_id"] == "spread-123"
    assert fake.calls[0]["range_"] == "Сводка!A1"
    assert fake.calls[0]["values"] == WEEKLY_DATA

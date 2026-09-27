"""Импорт DAG и структура графа (REQ-001/REQ-005/REQ-006/REQ-007).

Проверяем: DAG загружается (airflow.sdk API), четыре задачи выстроены цепочкой
fetch_sales -> validate_data -> write_to_sheets -> record_metrics, алерты
подключены, хардкод spreadsheet_id убран (конфиг через переменные, REQ-002).
"""

from __future__ import annotations

import inspect
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

import daily_sales_report  # noqa: E402


def dag():
    return daily_sales_report.dag


def test_dag_id():
    assert dag().dag_id == "daily_sales_report"


def test_dag_schedule():
    assert dag().schedule == "@daily"


def test_dag_tags_include_sales():
    assert "sales" in (dag().tags or [])


def test_on_failure_callback_wired():
    assert dag().on_failure_callback is not None


def test_dag_has_four_tasks():
    ids = {t.task_id for t in dag().tasks}
    assert ids == {"fetch_sales", "validate_data", "write_to_sheets", "record_metrics"}


def test_dependency_chain():
    metrics_task = dag().get_task("record_metrics")
    write = dag().get_task("write_to_sheets")
    validate = dag().get_task("validate_data")
    assert "write_to_sheets" in {t.task_id for t in metrics_task.upstream_list}
    assert "validate_data" in {t.task_id for t in write.upstream_list}
    assert "fetch_sales" in {t.task_id for t in validate.upstream_list}


def test_no_hardcoded_sheet_props():
    src = inspect.getsource(daily_sales_report)
    assert "your-spreadsheet-id" not in src
    assert "SPREADSHEET_ID" not in src
    assert "RANGE_NAME =" not in src

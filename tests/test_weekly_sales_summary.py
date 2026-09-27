"""Импорт DAG и структура графа (REQ-009).

Проверяем: weekly_sales_summary загружается (airflow.sdk API), четыре задачи
выстроены цепочкой fetch_weekly -> validate_weekly -> write_weekly ->
record_metrics, переиспользует shared-модули (config, dq, metrics, output,
windows), catchup=False (REQ-008).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

import weekly_sales_summary  # noqa: E402


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

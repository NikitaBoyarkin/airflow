"""Импорт DAG и структура графа для delivery_control_tower.

Проверяем: DAG загружается, расписание ручное, DQ-гейт стоит между staging и
витринами, пять аналитических задач зависят от build_marts и друг от друга — нет.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

import delivery_control_tower  # noqa: E402


def dag():
    return delivery_control_tower.dag


def test_dag_id():
    assert dag().dag_id == "delivery_control_tower"


def test_manual_schedule():
    assert dag().schedule is None


def test_task_set():
    assert {t.task_id for t in dag().tasks} == {
        "stage",
        "quality_gate",
        "build_marts",
        "run_sql",
        "bottlenecks",
        "run_hypothesis",
        "forecast",
        "dashboard",
    }


def test_quality_gate_between_staging_and_marts():
    assert dag().get_task("quality_gate").upstream_task_ids == {"stage"}
    assert dag().get_task("build_marts").upstream_task_ids == {"quality_gate"}


def test_analysis_tasks_fan_out_from_marts():
    for task_id in ("run_sql", "bottlenecks", "run_hypothesis", "forecast", "dashboard"):
        assert dag().get_task(task_id).upstream_task_ids == {"build_marts"}

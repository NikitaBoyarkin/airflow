"""Observability: метрики запусков DAG (REQ-007).

Каждый успешный запуск пишет строку в `dag_metrics` (Postgres) через финальный
таск `record_metrics`; фейл пишется из DAG-level `on_failure_callback`.
Запросы: `scripts/metrics_report.py` (success rate, duration p95).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import task

from shared.alerts import on_failure_callback as _alert_on_failure
from shared.config import DAGConfig

logger = logging.getLogger(__name__)

METRICS_DDL = """
CREATE TABLE IF NOT EXISTS dag_metrics (
    id           bigserial PRIMARY KEY,
    dag_id       text NOT NULL,
    run_id       text NOT NULL,
    status       text NOT NULL,          -- success | failed
    duration_s   numeric(10, 2),
    rows_written int,
    logical_date date,
    recorded_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dag_metrics_recorded_at ON dag_metrics (recorded_at);
"""


def _conn_id() -> str:
    return DAGConfig().get("metrics_postgres_conn_id", default="postgres_default")


def record_metric(
    dag_id: str,
    run_id: str,
    status: str,
    duration_s: float | None,
    rows_written: int | None = None,
    logical_date: Any = None,
    *,
    conn_id: str | None = None,
) -> None:
    """Пишет строку-метрику в dag_metrics. conn_id — для тестов (без конфига)."""
    hook = PostgresHook(postgres_conn_id=conn_id or _conn_id())
    hook.run(METRICS_DDL)
    hook.run(
        "INSERT INTO dag_metrics (dag_id, run_id, status, duration_s, rows_written, logical_date) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        parameters=(dag_id, run_id, status, duration_s, rows_written, logical_date),
    )


def record_failure_metric(context: dict[str, Any]) -> None:
    """Из DAG-level on_failure_callback: пишет failed-метрику. Best-effort."""
    dag_run = context.get("dag_run")
    if dag_run is None:
        return
    duration_s = None
    if dag_run.start_date and dag_run.end_date:
        duration_s = round((dag_run.end_date - dag_run.start_date).total_seconds(), 2)
    try:
        record_metric(
            dag_id=dag_run.dag_id,
            run_id=dag_run.run_id,
            status="failed",
            duration_s=duration_s,
            logical_date=dag_run.logical_date,
        )
    except Exception:  # noqa: BLE001 — метрика не должна маскировать алерт
        logger.warning(
            "failed to record failure metric for %s", dag_run.run_id, exc_info=True
        )


def on_failure_callback(context: dict[str, Any]) -> None:
    """DAG-level callback: алерт (REQ-004) + failed-метрика (REQ-007)."""
    _alert_on_failure(context)
    record_failure_metric(context)


def make_record_metrics_task():
    """Фабрика финального таска: success-метрика после записи данных.

    Вызывается на уровне модуля каждого DAG; task_id = "record_metrics".
    """

    @task
    def record_metrics(data: list[list[str]], **context) -> str:
        dag_run = context["dag_run"]
        duration_s = None
        if dag_run.start_date is not None:
            duration_s = round(
                (datetime.now(UTC) - dag_run.start_date).total_seconds(), 2
            )
        record_metric(
            dag_id=dag_run.dag_id,
            run_id=dag_run.run_id,
            status="success",
            duration_s=duration_s,
            rows_written=len(data) - 1,
            logical_date=dag_run.logical_date,
        )
        return f"metrics recorded: {len(data) - 1} rows"

    return record_metrics

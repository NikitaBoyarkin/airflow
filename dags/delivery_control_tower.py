"""
DAG: delivery_control_tower
Пайплайн проекта delivery-control-tower: CSV -> staging -> витрины -> DQ -> аналитика.

Каждая задача запускает модуль проекта в его собственном окружении
(`uv run python -m delivery.<module>`): у платформы Airflow нет pandas/duckdb,
у проекта нет Airflow. Один процесс на задачу — пять анализов идут параллельно
после сборки витрин.

Источник данных статический (агрегат за 2022-01-01..2022-06-30), поэтому
schedule=None: запуск ручной. Появится живой источник — заменить на cron-строку
и включить catchup.

Метрики запусков из shared.metrics не используются: они пишут в Postgres
(`dag_metrics`), которого у проекта на DuckDB нет.
"""

from __future__ import annotations

import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG, task

PROJECT_DIR = Path.home() / "Desktop/00 ide/00 portfolio/delivery-control-tower"

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def run_module(module: str, timeout: int = 1800) -> str:
    """Прогнать `python -m <module>` в окружении проекта; вернуть хвост stdout.

    Ненулевой код возврата модуля -> исключение -> задача падает и ретраится.
    """
    proc = subprocess.run(
        ["uv", "run", "python", "-m", module],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"{module}: exit {proc.returncode}\n{proc.stderr[-4000:]}")
    return proc.stdout[-4000:]


@task
def stage() -> str:
    """data/raw/*.csv -> data/staging/*.parquet."""
    return run_module("delivery.etl")


@task
def quality_gate(staged: str) -> str:
    """12 именованных проверок; провал -> exit 1 -> витрины не строятся."""
    return run_module("delivery.quality")


@task
def build_marts(checked: str) -> str:
    """staging -> звёздная схема и витрины в DuckDB (файл пересоздаётся целиком)."""
    return run_module("delivery.marts")


@task
def run_sql(marts: str) -> str:
    """14 оконных запросов из sql/windows.sql + бенчмарк sql/optimization.sql."""
    return run_module("delivery.sql_layer")


@task
def bottlenecks(marts: str) -> str:
    """Разбор узких мест по каналу, категории и городу."""
    return run_module("delivery.bottlenecks")


@task
def run_hypothesis(marts: str) -> str:
    """Две предзарегистрированные гипотезы, критерий Манна — Уитни."""
    return run_module("delivery.hypothesis")


@task
def forecast(marts: str) -> str:
    """Бэктест со скользящим началом, WAPE и MASE на горизонтах 1/7/14 дней."""
    return run_module("delivery.forecast")


@task
def dashboard(marts: str) -> str:
    """Самодостаточный reports/dashboard.html."""
    return run_module("delivery.dashboard")


with DAG(
    dag_id="delivery_control_tower",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args=DEFAULT_ARGS,
    catchup=False,
    description="Delivery control tower: CSV -> DuckDB marts -> DQ -> аналитика",
    tags=["delivery", "etl", "duckdb"],
) as dag:
    marts = build_marts(quality_gate(stage()))
    run_sql(marts)
    bottlenecks(marts)
    run_hypothesis(marts)
    forecast(marts)
    dashboard(marts)

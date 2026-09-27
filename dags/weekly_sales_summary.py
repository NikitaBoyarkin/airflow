"""
DAG: weekly_sales_summary
Еженедельная сводка продаж по категориям: Postgres -> CSV (dev-фолбэк).

Второй DAG платформы (REQ-009): проверяет шаблон — переиспользует
shared-модули (config, dq, alerts, metrics, output, windows), не дублируя код.

Требуемые провайдеры:
    apache-airflow-providers-postgres
Подключения (Admin -> Connections):
    postgres_default      -- Postgres с таблицей sales (колонка category)
Конфигурация (Admin -> Variables, префикс AIRFLOW_CFG_):

    AIRFLOW_CFG_weekly_summary_output           -- "file" (дефолт) | "sheets"
    AIRFLOW_CFG_weekly_summary_postgres_conn_id -- по умолчанию postgres_default
    AIRFLOW_CFG_weekly_summary_spreadsheet_id   -- при output=sheets

Окно недели (REQ-009): [logical_date - 7 дней, logical_date) — 7 суток перед
logical_date. catchup=False (REQ-008): пропущенные недели не догоняются
автоматически; backfill — явно через `airflow dags backfill`.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG, task
from shared.config import DAGConfig
from shared.dq import validate_or_raise
from shared.metrics import make_record_metrics_task, on_failure_callback
from shared.output import write_csv
from shared.windows import weekly_window

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Сводка по категориям за неделю [start, end); параметры — ISO-даты.
WEEKLY_QUERY = """
SELECT
    category,
    SUM(amount) AS revenue,
    COUNT(*) AS orders
FROM sales
WHERE created_at >= %(start)s AND created_at < %(end)s
GROUP BY 1
ORDER BY 1;
"""

WEEKLY_HEADER = ("category", "revenue", "orders")


# --- Чистая логика (тестируема без Airflow runtime) ---


def fetch_weekly_rows(conn_id: str, start: str, end: str) -> list[list[str]]:
    """Сводка по категориям за [start, end) из Postgres; первая строка — header."""
    hook = PostgresHook(postgres_conn_id=conn_id)
    rows = hook.get_records(sql=WEEKLY_QUERY, parameters={"start": start, "end": end})
    return [list(WEEKLY_HEADER)] + [list(map(str, row)) for row in rows]


def write_weekly_file(
    data: list[list[str]], week_start: str, out_dir: str | Path | None = None
) -> list[list[str]]:
    """CSV в data/out/ (dev-фолбэк). Идемпотентно — перезапись по week_start.

    Возвращает data (pass-through): финальный record_metrics считает строки по данным.
    """
    msg = write_csv(data, f"weekly_sales_summary_{week_start}.csv", out_dir)
    logger.info(msg)
    return data


# --- Адаптеры Airflow: конфиг + контекст, затем чистые функции ---


@task
def fetch_weekly(**context) -> list[list[str]]:
    cfg = DAGConfig().weekly_summary()
    dag_run = context["dag_run"]
    # Airflow 3: manual-раны не имеют logical_date/data_interval — только run_after.
    anchor = dag_run.data_interval_start or dag_run.run_after
    start, end = weekly_window(anchor)
    return fetch_weekly_rows(
        conn_id=cfg["postgres_conn_id"],
        start=start.isoformat(),
        end=end.isoformat(),
    )


@task
def validate_weekly(data: list[list[str]]) -> list[list[str]]:
    return validate_or_raise(data, header=WEEKLY_HEADER)


@task
def write_weekly(data: list[list[str]], **context) -> list[list[str]]:
    dag_run = context["dag_run"]
    anchor = dag_run.data_interval_start or dag_run.run_after
    week_start, _ = weekly_window(anchor)
    return write_weekly_file(data, week_start.isoformat())


record_metrics = make_record_metrics_task()


with DAG(
    dag_id="weekly_sales_summary",
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    default_args=DEFAULT_ARGS,
    catchup=False,
    description="Еженедельная сводка продаж по категориям: Postgres -> CSV",
    tags=["sales", "report", "weekly"],
    # Airflow типизирует callback как Callable[[Context], None], а @task на этапе
    # сборки DAG отдаёт XComArg — оба расходятся с прикладными аннотациями.
    on_failure_callback=on_failure_callback,  # type: ignore[arg-type]
) as dag:
    record_metrics(write_weekly(validate_weekly(fetch_weekly())))  # type: ignore[arg-type]

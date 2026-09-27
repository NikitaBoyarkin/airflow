"""
DAG: daily_sales_report
Ежедневный отчёт по продажам: Postgres -> Google Sheets (dev-фолбэк: CSV).

Требуемые провайдеры:
    apache-airflow-providers-postgres apache-airflow-providers-google
Подключения (Admin -> Connections):
    postgres_default      -- Postgres с таблицей sales
    google_cloud_default  -- сервисный аккаунт с доступом к Google Sheets API
Конфигурация (Admin -> Variables, префикс AIRFLOW_CFG_):

    AIRFLOW_CFG_sales_report_spreadsheet_id   -- id документа (обязателен при output=sheets)
    AIRFLOW_CFG_sales_report_range_name       -- диапазон, по умолчанию "Отчёт!A1"
    AIRFLOW_CFG_sales_report_postgres_conn_id -- по умолчанию postgres_default
    AIRFLOW_CFG_sales_report_google_conn_id   -- по умолчанию google_cloud_default
    AIRFLOW_CFG_sales_report_output           -- "sheets" | "file" (dev-фолбэк CSV)

Политика catchup (REQ-008): catchup=False — пропущенные дни не догоняются
автоматически; точечный перезапуск/backfill делается явно через
`airflow dags backfill` (идемпотентно: окно из logical_date, перезапись CSV).
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG, task
from shared.config import DAGConfig
from shared.dq import DEFAULT_HEADER, validate_or_raise
from shared.metrics import make_record_metrics_task, on_failure_callback
from shared.output import write_csv
from shared.windows import daily_window

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Агрегат продаж за сутки [start, end); параметры — ISO-даты.
SALES_QUERY = """
SELECT
    date_trunc('day', created_at)::date AS sale_date,
    product_id,
    SUM(amount) AS revenue,
    COUNT(*) AS orders
FROM sales
WHERE created_at >= %(start)s AND created_at < %(end)s
GROUP BY 1, 2
ORDER BY 1, 2;
"""


# --- Чистая логика (тестируема без Airflow runtime) ---


def fetch_sales_rows(conn_id: str, start: str, end: str) -> list[list[str]]:
    """Агрегат продаж за [start, end) из Postgres; первая строка — header."""
    hook = PostgresHook(postgres_conn_id=conn_id)
    rows = hook.get_records(sql=SALES_QUERY, parameters={"start": start, "end": end})
    return [list(DEFAULT_HEADER)] + [list(map(str, row)) for row in rows]


def write_sales_file(
    data: list[list[str]], range_name: str, out_dir: str | Path | None = None
) -> list[list[str]]:
    """Dev-фолбэк (Q2): CSV в data/out/ вместо Google Sheets. Идемпотентно — перезапись.

    Возвращает data (pass-through): финальный record_metrics считает строки по данным.
    """
    sale_date = data[1][0] if len(data) > 1 else "no-data"
    msg = write_csv(data, f"sales_report_{sale_date}.csv", out_dir)
    logger.info(msg)
    return data


def write_sales_rows(
    spreadsheet_id: str,
    range_name: str,
    gcp_conn_id: str,
    data: list[list[str]],
    output: str = "sheets",
) -> list[list[str]]:
    """Запись результата. output="sheets" — Google Sheets; "file" — CSV (dev-фолбэк).

    Возвращает data (pass-through) — record_metrics считает строки по данным.
    """
    if output == "file":
        return write_sales_file(data, range_name)
    hook = GSheetsHook(gcp_conn_id=gcp_conn_id)
    hook.update_values(
        spreadsheet_id=spreadsheet_id,
        range_=range_name,
        values=data,
    )
    logger.info("written %d rows -> %s", len(data) - 1, range_name)
    return data


# --- Адаптеры Airflow: конфиг + контекст, затем чистые функции ---


@task
def fetch_sales(**context) -> list[list[str]]:
    cfg = DAGConfig().sales_report()
    dag_run = context["dag_run"]
    # Airflow 3: manual-раны не имеют logical_date/data_interval — только run_after.
    anchor = dag_run.data_interval_start or dag_run.run_after
    start, end = daily_window(anchor)
    return fetch_sales_rows(
        conn_id=cfg["postgres_conn_id"],
        start=start.isoformat(),
        end=end.isoformat(),
    )


@task
def validate_data(data: list[list[str]]) -> list[list[str]]:
    return validate_or_raise(data)


@task
def write_to_sheets(data: list[list[str]]) -> list[list[str]]:
    cfg = DAGConfig().sales_report()
    return write_sales_rows(
        spreadsheet_id=cfg["spreadsheet_id"],
        range_name=cfg["range_name"],
        gcp_conn_id=cfg["google_conn_id"],
        data=data,
        output=cfg["output"],
    )


record_metrics = make_record_metrics_task()


with DAG(
    dag_id="daily_sales_report",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    default_args=DEFAULT_ARGS,
    catchup=False,
    description="Ежедневный отчёт по продажам: Postgres -> Google Sheets",
    tags=["sales", "report"],
    # Airflow типизирует callback как Callable[[Context], None], а @task на этапе
    # сборки DAG отдаёт XComArg — оба расходятся с прикладными аннотациями.
    on_failure_callback=on_failure_callback,  # type: ignore[arg-type]
) as dag:
    record_metrics(write_to_sheets(validate_data(fetch_sales())))  # type: ignore[arg-type]

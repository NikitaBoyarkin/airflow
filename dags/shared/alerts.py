"""Алертинг DAG: callbacks для Airflow и форматирование сообщений (REQ-004).

Phase 1: формат + лог, точка подключения канала доставки.
Phase 2: email; REQ-010: Slack/Telegram.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class NoDataError(RuntimeError):
    """Данных нет — штатный исход, о котором оператор должен узнать."""


def format_alert(context: dict[str, Any]) -> str:
    """Формирует сообщение алерта из Airflow Context."""
    ti = context.get("ti")
    dag_id = getattr(ti, "dag_id", None) or getattr(context.get("dag"), "dag_id", "?")
    task_id = getattr(ti, "task_id", "?")
    logical_date = context.get("logical_date") or context.get("execution_date") or "?"
    exception = context.get("exception")
    return (
        "[AIRFLOW ALERT] "
        f"dag={dag_id} task={task_id} logical_date={logical_date}\n"
        f"exception={exception!r}"
    )


def on_failure_callback(context: dict[str, Any]) -> None:
    """DAG-level `on_failure_callback`: форматирует и доставляет алерт.

    Phase 1: лог в stderr. Канал доставки (email/Slack) подключается
    в Phase 2 через конфиг — см. REQ-004/REQ-010.
    """
    logger.error(format_alert(context))

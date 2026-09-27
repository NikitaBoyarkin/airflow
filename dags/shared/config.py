"""Конфигурация DAG-ов через Airflow Variables (REQ-002).

Паттерн: переменные с префиксом `AIRFLOW_CFG_<dag>_<key>`.
Значения читаются в рантайме DAG; хардкод параметров в коде запрещён.

Example:
    >>> DAGConfig().sales_report()  # в теле DAG
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

PREFIX = "AIRFLOW_CFG"

Getter = Callable[[str], str | None]


class ConfigError(Exception):
    """Обязательная Airflow Variable отсутствует или пуста."""


def _default_getter(key: str) -> str | None:
    from airflow.models import Variable  # локальный импорт: тесты без airflow

    # default_var=None: в рантайме таска Variable.get бросает AirflowRuntimeError
    # (VARIABLE_NOT_FOUND), а не KeyError — default_var обходит оба пути.
    return Variable.get(key, default_var=None)


class DAGConfig:
    """Читает конфиг из Airflow Variables: дефолты, required-валидация."""

    def __init__(self, prefix: str = PREFIX, getter: Getter | None = None) -> None:
        self._prefix = prefix
        self._getter = getter or _default_getter

    def get(self, key: str, *, default: str = "", required: bool = False) -> str:
        """Значение переменной; required=True и пусто -> ConfigError."""
        full = f"{self._prefix}_{key}"
        try:
            raw: Any = self._getter(full)
        except KeyError:
            raw = None
        value = str(raw or "").strip() or default
        if required and not value:
            raise ConfigError(f"Airflow Variable '{full}' обязательна, но отсутствует")
        return value

    def sales_report(self) -> dict[str, str]:
        """Параметры DAG daily_sales_report.

        output="sheets" — запись в Google Sheets (нужен spreadsheet_id);
        output="file"   — dev-фолбэк (Q2): CSV в data/out/, spreadsheet_id не нужен.
        """
        output = self.get("sales_report_output", default="sheets")
        if output not in ("sheets", "file"):
            raise ConfigError(
                f"sales_report_output должен быть 'sheets' или 'file', получено {output!r}"
            )
        params: dict[str, str] = {
            "output": output,
            "range_name": self.get("sales_report_range_name", default="Отчёт!A1"),
            "postgres_conn_id": self.get(
                "sales_report_postgres_conn_id", default="postgres_default"
            ),
            "google_conn_id": self.get(
                "sales_report_google_conn_id", default="google_cloud_default"
            ),
        }
        if output == "sheets":
            params["spreadsheet_id"] = self.get(
                "sales_report_spreadsheet_id", required=True
            )
        else:
            params["spreadsheet_id"] = ""
        return params

    def weekly_summary(self) -> dict[str, str]:
        """Параметры DAG weekly_sales_summary (REQ-009).

        output="file" — дефолт: dev-фолбэк CSV (Sheets ещё не настроен);
        output="sheets" — запись в Google Sheets (нужен spreadsheet_id).
        """
        output = self.get("weekly_summary_output", default="file")
        if output not in ("sheets", "file"):
            raise ConfigError(
                f"weekly_summary_output должен быть 'sheets' или 'file', получено {output!r}"
            )
        params: dict[str, str] = {
            "output": output,
            "range_name": self.get("weekly_summary_range_name", default="Сводка!A1"),
            "postgres_conn_id": self.get(
                "weekly_summary_postgres_conn_id", default="postgres_default"
            ),
            "google_conn_id": self.get(
                "weekly_summary_google_conn_id", default="google_cloud_default"
            ),
        }
        if output == "sheets":
            params["spreadsheet_id"] = self.get(
                "weekly_summary_spreadsheet_id", required=True
            )
        else:
            params["spreadsheet_id"] = ""
        return params

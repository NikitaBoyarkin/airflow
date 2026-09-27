"""Окна дат для планирования (REQ-005).

Отчёт считается за сутки ПЕРЕД logical_date: старт 05 числа -> данные за 04.
logical_date приходит из Airflow как tz-aware pendulum datetime.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta


def daily_window(logical_date: date | datetime) -> tuple[date, date]:
    """Полуинтервал [start, end) — сутки, предшествующие logical_date."""
    day = logical_date.date() if isinstance(logical_date, datetime) else logical_date
    start = day - timedelta(days=1)
    return start, day


def weekly_window(logical_date: date | datetime) -> tuple[date, date]:
    """Полуинтервал [start, end) — 7 суток, предшествующих logical_date (REQ-009)."""
    day = logical_date.date() if isinstance(logical_date, datetime) else logical_date
    start = day - timedelta(days=7)
    return start, day

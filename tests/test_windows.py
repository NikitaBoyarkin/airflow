"""Юнит-тесты окон дат (REQ-005/REQ-009): сутки/неделя перед logical_date."""

from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from shared.windows import daily_window, weekly_window  # noqa: E402


def test_normal_day():
    assert daily_window(date(2026, 9, 5)) == (date(2026, 9, 4), date(2026, 9, 5))


def test_month_boundary():
    assert daily_window(date(2026, 3, 1)) == (date(2026, 2, 28), date(2026, 3, 1))


def test_leap_year_boundary():
    assert daily_window(date(2024, 3, 1)) == (date(2024, 2, 29), date(2024, 3, 1))


def test_year_boundary():
    assert daily_window(date(2026, 1, 1)) == (date(2025, 12, 31), date(2026, 1, 1))


def test_accepts_datetime_like_airflow_passes():
    assert daily_window(datetime(2026, 9, 5, 12, 30)) == (
        date(2026, 9, 4),
        date(2026, 9, 5),
    )


def test_weekly_window_seven_days():
    assert weekly_window(date(2026, 9, 6)) == (date(2026, 8, 30), date(2026, 9, 6))


def test_weekly_window_month_boundary():
    assert weekly_window(date(2026, 3, 2)) == (date(2026, 2, 23), date(2026, 3, 2))


def test_weekly_window_accepts_datetime():
    assert weekly_window(datetime(2026, 9, 6, 0, 30)) == (
        date(2026, 8, 30),
        date(2026, 9, 6),
    )

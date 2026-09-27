"""Pipeline-шаги через мокнутые hook'и (REQ-006).

Зовём чистые функции DAG'а напрямую, подменяя PostgresHook/GSheetsHook
на уровне модуля. Мок-контур повторяет реальную цепочку fetch -> validate -> write.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

import daily_sales_report as dsr  # noqa: E402
import weekly_sales_summary as wss  # noqa: E402
from shared.alerts import NoDataError  # noqa: E402
from shared.dq import validate_or_raise  # noqa: E402

# Реалистичные типы из psycopg2: ISO-date, text, Decimal, int.
RAW_ROWS = [
    ("2026-09-04", "p1", Decimal("1250.5"), 7),
    ("2026-09-04", "p2", Decimal("300.0"), 2),
]

HEADER = ["sale_date", "product_id", "revenue", "orders"]


class FakePostgresHook:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def get_records(self, sql, parameters=None):
        self.calls.append((sql, parameters))
        return self.rows


class FakeSheetsHook:
    def __init__(self):
        self.calls = []

    def update_values(self, **kwargs):
        self.calls.append(kwargs)


def test_fetch_sales_rows_uses_window_params(monkeypatch):
    fake = FakePostgresHook(RAW_ROWS)
    monkeypatch.setattr(dsr, "PostgresHook", lambda **kw: fake)

    data = dsr.fetch_sales_rows("pc", "2026-09-04", "2026-09-05")

    assert data[0] == HEADER
    assert data[1] == ["2026-09-04", "p1", "1250.5", "7"]
    sql, params = fake.calls[0]
    assert params == {"start": "2026-09-04", "end": "2026-09-05"}
    assert "%(start)s" in sql and "%(end)s" in sql


def test_validate_or_raise_ok_returns_same_object():
    data = [HEADER, ["2026-09-04", "p1", "10", "1"]]
    assert validate_or_raise(data) is data


def test_validate_or_raise_no_data_raises_NoDataError():
    with pytest.raises(NoDataError):
        validate_or_raise([HEADER])


def test_validate_or_raise_dq_violation_raises_ValueError():
    data = [HEADER, ["2026-09-04", "p1", "-3", "1"]]
    with pytest.raises(ValueError, match="revenue < 0"):
        validate_or_raise(data)


def test_write_sales_rows_updates_sheets(monkeypatch):
    fake = FakeSheetsHook()
    monkeypatch.setattr(dsr, "GSheetsHook", lambda **kw: fake)
    data = [HEADER, ["2026-09-04", "p1", "10", "1"]]

    result = dsr.write_sales_rows("spread-123", "Отчёт!A1", "gcp", data)

    assert result is data  # pass-through: record_metrics считает строки по данным
    call = fake.calls[0]
    assert call["spreadsheet_id"] == "spread-123"
    assert call["range_"] == "Отчёт!A1"
    assert call["values"] == data


def test_write_sales_file_writes_csv(tmp_path):
    data = [HEADER, ["2026-09-04", "p1", "10", "1"]]

    result = dsr.write_sales_file(data, "Отчёт!A1", out_dir=tmp_path)

    assert result is data  # pass-through
    content = (tmp_path / "sales_report_2026-09-04.csv").read_text(encoding="utf-8")
    assert "sale_date,product_id,revenue,orders" in content
    assert "2026-09-04,p1,10,1" in content


def test_write_sales_rows_file_mode_routes(monkeypatch):
    monkeypatch.setattr(
        dsr, "write_sales_file", lambda data, range_name, out_dir=None: data
    )
    data = [HEADER, ["2026-09-04", "p1", "10", "1"]]

    result = dsr.write_sales_rows("", "Отчёт!A1", "gcp", data, output="file")

    assert result is data


def test_fetch_sales_manual_run_falls_back_to_run_after(monkeypatch):
    """Airflow 3: manual-раны без logical_date/data_interval — якорь окна = run_after."""
    captured = {}

    def fake_fetch(conn_id, start, end):
        captured.update(conn_id=conn_id, start=start, end=end)
        return [HEADER]

    class FakeDagRun:
        data_interval_start = None
        run_after = datetime(2026, 9, 5, 18, 18, 25, tzinfo=UTC)

    monkeypatch.setattr(dsr, "fetch_sales_rows", fake_fetch)
    monkeypatch.setattr(
        dsr,
        "DAGConfig",
        lambda: SimpleNamespace(
            sales_report=lambda: {
                "output": "file",
                "spreadsheet_id": "",
                "range_name": "Отчёт!A1",
                "postgres_conn_id": "pc",
                "google_conn_id": "gcp",
            }
        ),
    )

    dsr.fetch_sales.function(dag_run=FakeDagRun())

    assert captured == {"conn_id": "pc", "start": "2026-09-04", "end": "2026-09-05"}


def test_pipeline_chain_end_to_end(monkeypatch):
    """fetch -> validate -> write: что попало в Sheets — то, что вернул Postgres."""
    fake_pg = FakePostgresHook(RAW_ROWS)
    monkeypatch.setattr(dsr, "PostgresHook", lambda **kw: fake_pg)
    fake_sheets = FakeSheetsHook()
    monkeypatch.setattr(dsr, "GSheetsHook", lambda **kw: fake_sheets)

    data = dsr.fetch_sales_rows("pc", "2026-09-04", "2026-09-05")
    validated = validate_or_raise(data)
    result = dsr.write_sales_rows("spread-123", "Отчёт!A1", "gcp", validated)

    assert result is validated  # pass-through
    assert fake_sheets.calls[0]["values"][1:] == [
        ["2026-09-04", "p1", "1250.5", "7"],
        ["2026-09-04", "p2", "300.0", "2"],
    ]


# --- weekly_sales_summary (REQ-009) ---

WEEKLY_HEADER = ["category", "revenue", "orders"]
WEEKLY_ROWS = [
    ("electronics", Decimal("1250.5"), 7),
    ("home", Decimal("300.0"), 2),
]


def test_fetch_weekly_rows_uses_window_params(monkeypatch):
    fake = FakePostgresHook(WEEKLY_ROWS)
    monkeypatch.setattr(wss, "PostgresHook", lambda **kw: fake)

    data = wss.fetch_weekly_rows("pc", "2026-08-30", "2026-09-06")

    assert data[0] == WEEKLY_HEADER
    assert data[1] == ["electronics", "1250.5", "7"]
    sql, params = fake.calls[0]
    assert params == {"start": "2026-08-30", "end": "2026-09-06"}
    assert "%(start)s" in sql and "%(end)s" in sql


def test_validate_weekly_ok_returns_same_object():
    data = [WEEKLY_HEADER, ["electronics", "10", "1"]]
    assert validate_or_raise(data, header=WEEKLY_HEADER) is data


def test_validate_weekly_no_data_raises_NoDataError():
    with pytest.raises(NoDataError):
        validate_or_raise([WEEKLY_HEADER], header=WEEKLY_HEADER)


def test_write_weekly_file_writes_csv(tmp_path):
    data = [WEEKLY_HEADER, ["electronics", "10", "1"]]

    result = wss.write_weekly_file(data, "2026-08-30", out_dir=tmp_path)

    assert result is data  # pass-through
    content = (tmp_path / "weekly_sales_summary_2026-08-30.csv").read_text(
        encoding="utf-8"
    )
    assert "category,revenue,orders" in content
    assert "electronics,10,1" in content


def test_fetch_weekly_manual_run_falls_back_to_run_after(monkeypatch):
    """Airflow 3: manual-ран без data_interval — якорь недели = run_after."""
    captured = {}

    def fake_fetch(conn_id, start, end):
        captured.update(conn_id=conn_id, start=start, end=end)
        return [WEEKLY_HEADER]

    class FakeDagRun:
        data_interval_start = None
        run_after = datetime(2026, 9, 6, 10, 0, 0, tzinfo=UTC)

    monkeypatch.setattr(wss, "fetch_weekly_rows", fake_fetch)
    monkeypatch.setattr(
        wss,
        "DAGConfig",
        lambda: SimpleNamespace(
            weekly_summary=lambda: {
                "output": "file",
                "postgres_conn_id": "pc",
                "google_conn_id": "gcp",
                "spreadsheet_id": "",
                "range_name": "Сводка!A1",
            }
        ),
    )

    wss.fetch_weekly.function(dag_run=FakeDagRun())

    assert captured == {"conn_id": "pc", "start": "2026-08-30", "end": "2026-09-06"}

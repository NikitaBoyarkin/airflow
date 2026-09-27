"""Юнит-тесты DAGConfig (REQ-002): дефолты, required, префикс."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from shared.config import ConfigError, DAGConfig  # noqa: E402


def make_config(vars_: dict[str, str] | None = None) -> DAGConfig:
    """DAGConfig с dict-getter вместо Airflow Variables."""
    store = vars_ or {}

    def getter(key: str) -> str:
        if key in store:
            return store[key]
        raise KeyError(key)

    return DAGConfig(getter=getter)


class TestGet:
    def test_default_used_when_missing(self):
        cfg = make_config()
        assert cfg.get("sales_report_range_name", default="Отчёт!A1") == "Отчёт!A1"

    def test_explicit_value_wins(self):
        cfg = make_config({"AIRFLOW_CFG_sales_report_range_name": "Sheet!B2"})
        assert cfg.get("sales_report_range_name", default="Отчёт!A1") == "Sheet!B2"

    def test_required_missing_raises(self):
        cfg = make_config()
        with pytest.raises(ConfigError):
            cfg.get("sales_report_spreadsheet_id", required=True)

    def test_required_present_ok(self):
        cfg = make_config({"AIRFLOW_CFG_sales_report_spreadsheet_id": "abc123"})
        assert cfg.get("sales_report_spreadsheet_id", required=True) == "abc123"

    def test_custom_prefix(self):
        cfg = DAGConfig(prefix="MY_PREFIX", getter=lambda k: {"MY_PREFIX_x": "v"}[k])
        assert cfg.get("x") == "v"


class TestSalesReport:
    def test_defaults(self):
        cfg = make_config({"AIRFLOW_CFG_sales_report_spreadsheet_id": "s1"})
        params = cfg.sales_report()
        assert params["output"] == "sheets"
        assert params["spreadsheet_id"] == "s1"
        assert params["range_name"] == "Отчёт!A1"
        assert params["postgres_conn_id"] == "postgres_default"
        assert params["google_conn_id"] == "google_cloud_default"

    def test_missing_spreadsheet_id_raises(self):
        cfg = make_config()
        with pytest.raises(ConfigError):
            cfg.sales_report()

    def test_file_mode_does_not_require_spreadsheet_id(self):
        cfg = make_config({"AIRFLOW_CFG_sales_report_output": "file"})
        params = cfg.sales_report()
        assert params["output"] == "file"
        assert params["spreadsheet_id"] == ""

    def test_invalid_output_raises(self):
        cfg = make_config({"AIRFLOW_CFG_sales_report_output": "email"})
        with pytest.raises(ConfigError):
            cfg.sales_report()

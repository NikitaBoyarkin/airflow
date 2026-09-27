"""Юнит-тесты DQ-валидатора (REQ-003)."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from shared.dq import DEFAULT_HEADER, validate

HEADER = list(DEFAULT_HEADER)
VALID_ROW = ["2026-09-04", "p1", "1250.5", "7"]


def test_empty_input_fails():
    issues = validate([])
    assert "empty" in issues[0]


def test_header_only_is_no_data():
    issues = validate([HEADER])
    assert any("no data rows" in i for i in issues)


def test_valid_dataset_has_no_issues():
    data = [HEADER, VALID_ROW, ["2026-09-04", "p2", "0", "1"]]
    assert validate(data) == []


def test_header_mismatch_detected():
    issues = validate([["date", "product", "rev", "cnt"], VALID_ROW])
    assert any("header mismatch" in i for i in issues)


def test_negative_revenue_detected():
    issues = validate([HEADER, ["2026-09-04", "p1", "-5", "2"]])
    assert any("revenue < 0" in i for i in issues)


def test_negative_orders_detected():
    issues = validate([HEADER, ["2026-09-04", "p1", "10", "-1"]])
    assert any("orders < 0" in i for i in issues)


def test_non_numeric_revenue_detected():
    issues = validate([HEADER, ["2026-09-04", "p1", "abc", "2"]])
    assert any("revenue не число" in i for i in issues)


def test_wrong_column_count_detected():
    issues = validate([HEADER, ["2026-09-04", "p1"]])
    assert any("колонок" in i for i in issues)


def test_empty_sale_date_detected():
    issues = validate([HEADER, ["", "p1", "10", "2"]])
    assert any("пустой sale_date" in i for i in issues)

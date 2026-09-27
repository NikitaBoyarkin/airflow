"""Генератор синтетических продаж для демо (Q3).

Создаёт таблицу sales в Postgres и заполняет ~N строк/день за последние D суток.
Идемпотентен: строки в окне перезаписываются (DELETE + INSERT).

Таймстампы — UTC, чтобы окно DAG [logical_date-1, logical_date) совпадало с данными.

Пример:
    uv run python scripts/generate_sales_data.py --days 7 --rows-per-day 10000
"""

from __future__ import annotations

import argparse
import random
from datetime import UTC, datetime, time, timedelta

import psycopg2

DDL = """
CREATE TABLE IF NOT EXISTS sales (
    id          bigserial PRIMARY KEY,
    created_at  timestamptz NOT NULL,
    product_id  text NOT NULL,
    category    text NOT NULL,
    amount      numeric(12, 2) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sales_created_at ON sales (created_at);
-- Миграция для существующей таблицы: демо-данные перезаписываются целиком.
ALTER TABLE sales ADD COLUMN IF NOT EXISTS category text;
"""

# Таблица метрик запусков (REQ-007): пишется DAG-ами, читается metrics_report.py.
METRICS_DDL = """
CREATE TABLE IF NOT EXISTS dag_metrics (
    id           bigserial PRIMARY KEY,
    dag_id       text NOT NULL,
    run_id       text NOT NULL,
    status       text NOT NULL,          -- success | failed
    duration_s   numeric(10, 2),
    rows_written int,
    logical_date date,
    recorded_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dag_metrics_recorded_at ON dag_metrics (recorded_at);
"""

PRODUCTS = [f"p{i}" for i in range(1, 21)]

# Категория продукта (REQ-009: weekly_sales_summary группирует по category).
CATEGORY = {
    **{f"p{i}": "electronics" for i in range(1, 6)},
    **{f"p{i}": "home" for i in range(6, 11)},
    **{f"p{i}": "sports" for i in range(11, 16)},
    **{f"p{i}": "beauty" for i in range(16, 21)},
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", default=5434, type=int)
    p.add_argument("--user", default="nikitaboarkin")
    p.add_argument("--db", default="airflow_demo")
    p.add_argument("--days", default=7, type=int, help="сколько суток назад заполнять")
    p.add_argument("--rows-per-day", default=10000, type=int)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    conn = psycopg2.connect(
        host=args.host, port=args.port, user=args.user, dbname=args.db
    )
    conn.autocommit = True
    end = datetime.now(UTC).date()
    start = end - timedelta(days=args.days)

    with conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute(METRICS_DDL)
        cur.execute(
            "DELETE FROM sales WHERE created_at >= %s AND created_at < %s", (start, end)
        )
        rng = random.Random(42)
        rows = []
        for day_offset in range(args.days):
            day = start + timedelta(days=day_offset)
            for _ in range(args.rows_per_day):
                ts = datetime.combine(
                    day,
                    time(
                        hour=rng.randrange(0, 24),
                        minute=rng.randrange(0, 60),
                        second=rng.randrange(0, 60),
                    ),
                    tzinfo=UTC,
                )
                product = rng.choice(PRODUCTS)
                rows.append(
                    (ts, product, CATEGORY[product], round(rng.uniform(100, 5000), 2))
                )
        cur.executemany(
            "INSERT INTO sales (created_at, product_id, category, amount) VALUES (%s, %s, %s, %s)",
            rows,
        )
        cur.execute(
            "SELECT count(*), min(created_at)::date, max(created_at)::date FROM sales"
        )
        total, lo, hi = cur.fetchone()
    print(f"sales: {total} строк, окно {lo}..{hi}")


if __name__ == "__main__":
    main()

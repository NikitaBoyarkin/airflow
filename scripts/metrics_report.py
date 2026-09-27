"""Отчёт по метрикам запусков DAG (REQ-007).

Читает `dag_metrics` (Postgres) и печатает success rate и duration p95
за последние N дней. Таблица создаётся генератором и DAG-ами автоматически.

Пример:
    uv run python scripts/metrics_report.py --days 7
"""

from __future__ import annotations

import argparse

import psycopg2


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", default=5434, type=int)
    p.add_argument("--user", default="nikitaboarkin")
    p.add_argument("--db", default="airflow_demo")
    p.add_argument("--days", default=7, type=int, help="за сколько суток смотреть")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    conn = psycopg2.connect(
        host=args.host, port=args.port, user=args.user, dbname=args.db
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                dag_id,
                count(*) AS total,
                count(*) FILTER (WHERE status = 'success') AS ok,
                round(
                    count(*) FILTER (WHERE status = 'success')::numeric
                    / NULLIF(count(*), 0) * 100, 1
                ) AS success_rate_pct,
                round(percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_s)::numeric, 2) AS p95_s
            FROM dag_metrics
            WHERE recorded_at >= now() - %s::interval
            GROUP BY dag_id
            ORDER BY dag_id
            """,
            (f"{args.days} days",),
        )
        rows = cur.fetchall()
    if not rows:
        print(f"Нет метрик за последние {args.days} дн. (таблица dag_metrics пуста)")
        return
    print(f"Метрики за последние {args.days} дн.:")
    print(f"{'dag_id':<24} {'total':>6} {'ok':>5} {'rate%':>7} {'p95_s':>8}")
    for dag_id, total, ok, rate, p95 in rows:
        print(f"{dag_id:<24} {total:>6} {ok:>5} {rate:>7} {p95:>8}")
    total_all = sum(r[1] for r in rows)
    ok_all = sum(r[2] for r in rows)
    print(f"\nИтого: {ok_all}/{total_all} успешных ({ok_all / total_all * 100:.1f}%)")


if __name__ == "__main__":
    main()

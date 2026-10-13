"""Derived-surface EDA checks for the Airflow DAG outputs (data/out/*.csv).

Checks output shape / validity / date continuity / cross-DAG reconciliation.
Prints a machine-readable summary; no files are written except the combined tables
(created by prepare_data.py in the scratch dir).

Run:
    python analyze_airflow.py [--scratch DIR]
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

import prepare_data as prep


def continuity(dates: list[date]) -> dict:
    ds = sorted(dates)
    span = (ds[-1] - ds[0]).days + 1
    present = set(ds)
    missing = [
        str(ds[0] + timedelta(days=i))
        for i in range(span)
        if (ds[0] + timedelta(days=i)) not in present
    ]
    return {
        "first": str(ds[0]),
        "last": str(ds[-1]),
        "span_days": span,
        "n_present": len(ds),
        "missing_dates": missing,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scratch", default=None)
    args = ap.parse_args()
    _ = prep._scratch(args.scratch)  # ensure dir exists

    daily = prep.build_daily()
    weekly = prep.build_weekly()

    out: dict = {}

    # ---- Daily: grain = sale_date x product_id -------------------------------
    out["daily_shape"] = list(daily.shape)
    out["daily_dupe_full_rows"] = int(daily.duplicated().sum())
    out["daily_dupe_key"] = int(
        daily.duplicated(subset=["sale_date", "product_id"]).sum()
    )
    out["daily_n_dates"] = daily["sale_date"].nunique()
    out["daily_n_products"] = daily["product_id"].nunique()
    out["daily_continuity"] = continuity(list(daily["sale_date"].unique()))

    per_day = (
        daily.groupby("sale_date")
        .agg(
            revenue=("revenue", "sum"),
            orders=("orders", "sum"),
            products=("product_id", "nunique"),
        )
        .reset_index()
    )
    per_day["avg_price"] = per_day["revenue"] / per_day["orders"]
    per_day["revenue_pct_prev"] = per_day["revenue"].pct_change()
    out["daily_per_day"] = [
        {
            "sale_date": str(r.sale_date),
            "revenue": round(float(r.revenue), 2),
            "orders": int(r.orders),
            "products": int(r.products),
            "avg_price": round(float(r.avg_price), 2),
            "revenue_pct_prev": None
            if pd.isna(r.revenue_pct_prev)
            else round(float(r.revenue_pct_prev), 4),
        }
        for r in per_day.itertuples()
    ]
    out["daily_total_revenue"] = round(float(daily["revenue"].sum()), 2)
    out["daily_total_orders"] = int(daily["orders"].sum())

    # validity ranges
    out["validity"] = {
        "revenue_min": round(float(daily["revenue"].min()), 2),
        "revenue_max": round(float(daily["revenue"].max()), 2),
        "revenue_negatives": int((daily["revenue"] < 0).sum()),
        "orders_min": int(daily["orders"].min()),
        "orders_max": int(daily["orders"].max()),
        "orders_negatives": int((daily["orders"] < 0).sum()),
        "products": sorted(daily["product_id"].unique().tolist()),
        "date_min": str(daily["sale_date"].min()),
        "date_max": str(daily["sale_date"].max()),
        "date_max_gt_today": bool(daily["sale_date"].max() > date(2026, 10, 13)),
    }
    out["daily_rev_orders_corr"] = round(
        float(daily["revenue"].corr(daily["orders"])), 4
    )

    # product-level spread (segment)
    prod = (
        daily.groupby("product_id")["revenue"]
        .agg(["mean", "sum"])
        .sort_values("mean", ascending=False)
    )
    out["product_gap_ratio"] = round(float(prod["mean"].max() / prod["mean"].min()), 4)
    out["product_mean_min"] = round(float(prod["mean"].min()), 2)
    out["product_mean_max"] = round(float(prod["mean"].max()), 2)

    # is the product effect a composition artefact? products appear every day equally
    out["product_days_present"] = sorted(
        daily.groupby("product_id")["sale_date"].nunique().unique().tolist()
    )

    # ---- Weekly: grain = week_start x category -------------------------------
    out["weekly_shape"] = list(weekly.shape)
    out["weekly_dupe_full_rows"] = int(weekly.duplicated().sum())
    out["weekly_dupe_key"] = int(
        weekly.duplicated(subset=["week_start", "category"]).sum()
    )
    wk = (
        weekly.groupby("week_start")
        .agg(
            revenue=("revenue", "sum"),
            orders=("orders", "sum"),
            categories=("category", "nunique"),
        )
        .reset_index()
    )
    wk["avg_price"] = wk["revenue"] / wk["orders"]
    wk["implied_daily_revenue"] = wk["revenue"] / 7
    out["weekly_per_week"] = [
        {
            "week_start": str(r.week_start),
            "window": f"[{r.week_start}, {(r.week_start + timedelta(days=7))})",
            "revenue": round(float(r.revenue), 2),
            "orders": int(r.orders),
            "categories": int(r.categories),
            "avg_price": round(float(r.avg_price), 2),
            "implied_daily_revenue": round(float(r.implied_daily_revenue), 2),
        }
        for r in wk.itertuples()
    ]
    out["weekly_week_gap_ratio"] = round(
        float(wk["revenue"].max() / wk["revenue"].min()), 4
    )
    # week_start continuity / overlap
    wks = sorted(weekly["week_start"].unique())
    out["weekly_week_starts"] = [str(w) for w in wks]
    out["weekly_overlapping_windows"] = [
        [str(a), str(b)] for a, b in zip(wks, wks[1:]) if (b - a).days < 7
    ]

    # ---- Cross-DAG reconciliation -------------------------------------------
    daily_avg = daily.groupby("sale_date")["revenue"].sum().mean()
    wk_implied = wk["implied_daily_revenue"].mean()
    out["reconciliation"] = {
        "daily_avg_revenue_per_day": round(float(daily_avg), 2),
        "weekly_implied_revenue_per_day": round(float(wk_implied), 2),
        "ratio_weekly_over_daily": round(float(wk_implied / daily_avg), 4),
    }

    print(json.dumps(out, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

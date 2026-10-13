"""Figures for the Airflow DAG-output EDA (derived surface).

Reads the combined tables built by prepare_data.py and writes 5 PNGs to
../figures/. OO-style matplotlib, light theme, English labels.

Run (from this scripts/ dir):
    python plot_airflow.py
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import prepare_data as prep  # noqa: E402

FIG_DIR = Path(__file__).resolve().parents[1] / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

plt.rcParams.update(
    {
        "figure.dpi": 150,
        "savefig.dpi": 300,
        "font.size": 11,
        "axes.titlesize": 13,
        "axes.grid": True,
        "grid.alpha": 0.3,
        "axes.spines.top": False,
        "axes.spines.right": False,
    }
)

C_MAIN = "#1f77b4"
C_PARTIAL = "#d62728"
C_MISSING = "#bbbbbb"


def save(fig, name: str) -> None:
    fig.savefig(FIG_DIR / name, dpi=300, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def fig_daily_revenue(daily: pd.DataFrame) -> None:
    per_day = daily.groupby("sale_date")["revenue"].sum().reset_index()
    per_day["sale_date"] = pd.to_datetime(per_day["sale_date"])
    full = per_day[per_day["revenue"] > 1e7]
    partial = per_day[per_day["revenue"] <= 1e7]

    # build a continuous calendar so the missing day is visible
    all_days = pd.date_range(
        per_day["sale_date"].min(), per_day["sale_date"].max(), freq="D"
    )
    present = set(per_day["sale_date"])
    missing = [d for d in all_days if d not in present]

    fig, ax = plt.subplots(figsize=(10, 5.5), layout="constrained")
    ax.bar(all_days, [0] * len(all_days), color="none")
    ax.bar(
        full["sale_date"],
        full["revenue"] / 1e6,
        width=0.7,
        color=C_MAIN,
        edgecolor="black",
        linewidth=0.6,
        label="full day",
    )
    ax.bar(
        partial["sale_date"],
        partial["revenue"] / 1e6,
        width=0.7,
        color=C_PARTIAL,
        edgecolor="black",
        linewidth=0.6,
        label="partial day (incomplete window)",
    )
    for d in missing:
        ax.bar([d], [0], width=0.7, color=C_MISSING, edgecolor="black", linewidth=0.6)
        ax.annotate(
            "missing\n(2026-09-03)",
            (d, 0.4),
            ha="center",
            va="bottom",
            fontsize=9,
            color="#555555",
        )

    ax.annotate(
        "-83.9% vs prior day\n(1,612 vs ~10,000 orders)",
        xy=(partial["sale_date"].iloc[0], partial["revenue"].iloc[0] / 1e6),
        xytext=(partial["sale_date"].iloc[0] - timedelta(days=1.6), 16),
        fontsize=9,
        color=C_PARTIAL,
        arrowprops={"arrowstyle": "->", "color": C_PARTIAL, "lw": 1.2},
    )

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax.tick_params(axis="x", rotation=30)
    ax.set(
        xlabel="sale_date",
        ylabel="Revenue, mln",
        ylim=(0, 32),
        title="Daily report revenue by sale_date — one gap and one partial day",
    )
    ax.legend(frameon=False, loc="upper left")
    save(fig, "01_daily_revenue_by_date.png")


def fig_distribution(daily: pd.DataFrame) -> None:
    fig, axs = plt.subplots(1, 2, figsize=(12, 5), layout="constrained")
    rev = daily["revenue"]
    axs[0].hist(rev, bins=25, color=C_MAIN, edgecolor="black", linewidth=0.5)
    axs[0].set(
        xlabel="revenue per product-day",
        ylabel="count",
        title="Distribution of revenue (linear)",
    )
    axs[1].hist(rev, bins=25, color=C_MAIN, edgecolor="black", linewidth=0.5)
    axs[1].set_yscale("log")
    axs[1].set(
        xlabel="revenue per product-day",
        ylabel="count (log)",
        title="Same, log y — bimodal: full days vs partial day",
    )
    axs[1].annotate(
        "20 rows from\n2026-09-05 (partial)",
        xy=(2.5e5, 5),
        xytext=(6.0e5, 12),
        fontsize=9,
        color=C_PARTIAL,
        arrowprops={"arrowstyle": "->", "color": C_PARTIAL, "lw": 1.2},
    )
    fig.suptitle("revenue per product-day (n=100) is bimodal, not skewed")
    save(fig, "02_revenue_distribution.png")


def fig_product(daily: pd.DataFrame) -> None:
    prod = daily.groupby("product_id")["revenue"].mean().sort_values(ascending=False)
    fig, ax = plt.subplots(figsize=(11, 5.5), layout="constrained")
    ax.bar(
        prod.index, prod.values / 1e6, color=C_MAIN, edgecolor="black", linewidth=0.5
    )
    ax.axhline(
        daily["revenue"].mean() / 1e6,
        color=C_PARTIAL,
        ls="--",
        lw=1.3,
        label=f"overall mean = {daily['revenue'].mean() / 1e6:.2f} mln",
    )
    ax.set(
        xlabel="product_id",
        ylabel="Mean revenue, mln",
        title="Mean revenue by product_id — max/min gap only x1.12 (no segment signal)",
    )
    ax.tick_params(axis="x", rotation=60)
    ax.legend(frameon=False)
    save(fig, "03_revenue_by_product.png")


def fig_weekly(weekly: pd.DataFrame) -> None:
    wk = weekly.groupby("week_start").agg(revenue=("revenue", "sum")).reset_index()
    wk["week_start"] = pd.to_datetime(wk["week_start"])
    labels = [
        f"{d.date()}\n[{d.date()}, {(d + timedelta(days=7)).date()})"
        for d in wk["week_start"]
    ]
    fig, ax = plt.subplots(figsize=(10, 5.5), layout="constrained")
    bars = ax.bar(
        range(len(wk)),
        wk["revenue"] / 1e6,
        color=C_MAIN,
        edgecolor="black",
        linewidth=0.6,
    )
    for i, b in enumerate(bars):
        ax.annotate(
            f"{wk['revenue'].iloc[i] / 1e6:.1f}",
            (b.get_x() + b.get_width() / 2, b.get_height()),
            ha="center",
            va="bottom",
            fontsize=10,
        )
    ax.set_xticks(range(len(wk)))
    ax.set_xticklabels(labels, fontsize=9)
    ax.set(
        xlabel="week_start (window start)",
        ylabel="Weekly revenue, mln",
        title="Weekly summary by week_start — overlapping 7-day windows",
    )
    ax.annotate(
        "week starts are 6 and 1 days apart:\nwindows overlap, summing them double-counts",
        xy=(0.5, 0.55),
        xycoords="axes fraction",
        fontsize=9,
        color="#555555",
    )
    save(fig, "04_weekly_revenue_by_week.png")


def fig_orders_vs_revenue(daily: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(9, 6), layout="constrained")
    partial_date = date(2026, 9, 5)
    full = daily[daily["sale_date"] != partial_date]
    part = daily[daily["sale_date"] == partial_date]
    ax.scatter(
        full["orders"],
        full["revenue"] / 1e6,
        s=28,
        color=C_MAIN,
        edgecolor="black",
        linewidth=0.3,
        label="full day",
    )
    ax.scatter(
        part["orders"],
        part["revenue"] / 1e6,
        s=28,
        color=C_PARTIAL,
        edgecolor="black",
        linewidth=0.3,
        label="partial day (2026-09-05)",
    )
    r = daily["revenue"].corr(daily["orders"])
    slope = np.polyfit(daily["orders"], daily["revenue"] / 1e6, 1)
    xs = np.linspace(daily["orders"].min(), daily["orders"].max(), 50)
    ax.plot(
        xs,
        np.polyval(slope, xs),
        color="#333333",
        ls="--",
        lw=1.2,
        label=f"OLS fit (r={r:.3f})",
    )
    ax.set(
        xlabel="orders",
        ylabel="Revenue, mln",
        title="revenue ~ orders: r=0.997 — revenue is orders x ~2,550 (structural, not signal)",
    )
    ax.legend(frameon=False)
    save(fig, "05_orders_vs_revenue.png")


def main() -> None:
    daily = prep.build_daily()
    weekly = prep.build_weekly()
    fig_daily_revenue(daily)
    fig_distribution(daily)
    fig_product(daily)
    fig_weekly(weekly)
    fig_orders_vs_revenue(daily)
    print("figures written to", FIG_DIR)


if __name__ == "__main__":
    main()

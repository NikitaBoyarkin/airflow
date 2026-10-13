"""Combine the Airflow DAG output CSVs (data/out/*.csv) into single profiling tables.

These files are pipeline OUTPUTS (derived surface): each daily file is one run of
`daily_sales_report` (grain = sale_date x product_id), each weekly file is one run of
`weekly_sales_summary` (grain = week_start x category).

Combined tables are written to a scratch dir OUTSIDE the project so the project tree
stays clean; only the report artefacts live under reports/eda/.

Run:
    python prepare_data.py [--scratch DIR]
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
OUT_DIR = PROJECT / "data" / "out"


def _scratch(cli: str | None) -> Path:
    base = Path(cli) if cli else Path(os.environ.get("TMPDIR", "/tmp")) / "eda-airflow"
    base.mkdir(parents=True, exist_ok=True)
    return base


def build_daily() -> pd.DataFrame:
    frames = []
    for path in sorted(OUT_DIR.glob("sales_report_*.csv")):
        df = pd.read_csv(path)
        df["source_file"] = path.name
        frames.append(df)
    daily = pd.concat(frames, ignore_index=True)
    daily["sale_date"] = pd.to_datetime(daily["sale_date"]).dt.date
    return daily


def build_weekly() -> pd.DataFrame:
    frames = []
    for path in sorted(OUT_DIR.glob("weekly_sales_summary_*.csv")):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", path.name)
        df = pd.read_csv(path)
        df["week_start"] = pd.to_datetime(m.group(1)).date()
        df["source_file"] = path.name
        frames.append(df)
    weekly = pd.concat(frames, ignore_index=True)
    return weekly


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scratch", default=None, help="scratch dir for combined CSVs")
    args = ap.parse_args()
    scratch = _scratch(args.scratch)

    daily = build_daily()
    weekly = build_weekly()

    dpath = scratch / "daily_combined.csv"
    wpath = scratch / "weekly_combined.csv"
    daily.to_csv(dpath, index=False)
    weekly.to_csv(wpath, index=False)

    print(f"daily : {daily.shape} -> {dpath}")
    print(f"weekly: {weekly.shape} -> {wpath}")
    print("daily dates :", sorted({str(d) for d in daily["sale_date"].unique()}))
    print("weekly weeks:", sorted({str(d) for d in weekly["week_start"].unique()}))


if __name__ == "__main__":
    main()

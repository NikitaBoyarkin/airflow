"""Запись результата DAG во внешний приёмник (REQ-002, dev-фолбэк Q2).

`write_csv()` — общий CSV-райтер для dev-режима `output="file"`:
идемпотентная перезапись файла в `data/out/`. Google Sheets остаётся
основным каналом (GSheetsHook в DAG-файлах); CSV — фолбэк без внешних API.
"""

from __future__ import annotations

import csv
from pathlib import Path


def write_csv(
    data: list[list[str]], filename: str, out_dir: str | Path | None = None
) -> str:
    """Пишет data (первая строка — header) в `out_dir/filename`. Идемпотентно.

    Возвращает сообщение вида "written N rows -> <filename>".
    """
    out_dir = (
        Path(out_dir)
        if out_dir
        else Path(__file__).resolve().parents[2] / "data" / "out"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / filename
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerows(data)
    return f"written {len(data) - 1} rows -> {path.name}"

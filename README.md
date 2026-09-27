# Airflow DAG Platform

Портфельный Data Engineering проект: платформа отчётов на Apache Airflow 3, где каждый отчёт — версионируемый DAG. Заменяет ручную выгрузку агрегатов из Postgres и перенос в Google Sheets воспроизводимым, идемпотентным пайплайном с DQ-валидацией, алертами и метриками запусков.

## Что решает

Ручной отчёт по продажам (SQL-клиент → копирование в Sheets) давал задержки, ошибки копирования и отсутствие воспроизводимости. Платформа доводит данные до приёмника по расписанию без ручного вмешательства, падения видны сразу через алерты, новый отчёт добавляется по общему шаблону переиспользования shared-модулей.

## DAG-и

| DAG | Расписание | Что делает | Приёмник |
|-----|-----------|------------|----------|
| `daily_sales_report` | `@daily` | Агрегат продаж за сутки (`sale_date`, `product_id`, `revenue`, `orders`) | Google Sheets (или CSV-фолбэк) |
| `weekly_sales_summary` | `@weekly` | Сводка продаж за 7 суток по `category` (`category`, `revenue`, `orders`) | CSV ( Sheets опционально) |

Каждый DAG строится по шаблону `fetch → validate → write → record_metrics` (`@task`-декораторы, Airflow 3 SDK). Окно — полуинтервал `[logical_date − N, logical_date)`; `catchup=False` — пропущенные периоды не догоняются, backfill явно через `airflow dags backfill`.

## Структура

```
dags/
  daily_sales_report.py     # DAG: Postgres -> Google Sheets / CSV
  weekly_sales_summary.py   # DAG: Postgres -> CSV (сводка по категориям)
  shared/                   # переиспользуемые модули платформы (REQ-001)
    config.py               # конфиг из Airflow Variables, без хардкода
    dq.py                   # DQ-валидация перед записью
    alerts.py               # формат и callback алертов при падении
    metrics.py              # observability: запись в dag_metrics
    output.py               # CSV-райтер (dev-фолбэк), идемпотентная перезапись
    windows.py              # daily/weekly окна по logical_date
scripts/
  generate_sales_data.py    # синтетика в Postgres для демо
  metrics_report.py         # success rate / duration p95 из dag_metrics
tests/                      # pytest: DAG, shared-модули, интеграционные таски
docs/prd.md                 # PRD проекта
data/out/                   # выходные CSV (dev-режим output="file")
```

## Стек

- **Оркестрация:** Apache Airflow 3.3 (SDK, `@task`-декораторы), локальный standalone-режим
- **Провайдеры:** `apache-airflow-providers-postgres`, `apache-airflow-providers-google`
- **Источник:** Postgres 16 (демо: `127.0.0.1:5434`, БД `airflow_demo`)
- **Приёмник:** Google Sheets API (через `GSheetsHook`); dev-фолбэк — CSV в `data/out/`
- **Python:** 3.14, менеджер пакетов `uv`, тесты `pytest`

## Запуск

```bash
uv sync                                   # зависимости
uv run pytest                             # тесты (без Airflow runtime)

# Демо-данные в Postgres (синтетика, ~N строк/день):
uv run python scripts/generate_sales_data.py --days 7 --rows-per-day 10000

# Airflow standalone (UI http://localhost:8080, admin/admin):
export AIRFLOW_HOME=$(pwd)
uv run airflow standalone

# Отчёт по метрикам запусков:
uv run python scripts/metrics_report.py --days 7
```

CLI-команды Airflow требуют `export AIRFLOW_HOME=$(pwd)` — иначе они смотрят в `~/airflow` и падают с «Database migration required».

## Качество кода

CI (`.github/workflows/ci.yml`) на каждый push и PR гоняет четыре гейта:

```bash
uv run ruff check .          # линт: E,W,F,I,B,C4,UP (line-length игнорируется)
uv run ruff format --check . # формат; применить — uv run ruff format .
uv run mypy                  # типы: dags/, scripts/, tests/
uv run pytest                # 64 теста, без Airflow-сервисов и БД (хуки замоканы)
```

Конфиг — в `pyproject.toml` (`[tool.ruff]`, `[tool.mypy]`). `target-version = "py313"` выбран намеренно: на `py314` `ruff format` переписывает `except (TypeError, ValueError):` в PEP 758 `except TypeError, ValueError:` — валидно для 3.14, но ломает прогон на 3.13.

## Конфигурация (Airflow Variables, префикс `AIRFLOW_CFG_`)

| Переменная | Назначение |
|-----------|-----------|
| `AIRFLOW_CFG_sales_report_output` | `sheets` (по умолчанию) \| `file` (CSV-фолбэк) |
| `AIRFLOW_CFG_sales_report_spreadsheet_id` | id документа (обязателен при `output=sheets`) |
| `AIRFLOW_CFG_sales_report_range_name` | диапазон, по умолчанию `Отчёт!A1` |
| `AIRFLOW_CFG_weekly_summary_output` | `file` (по умолчанию) \| `sheets` |
| `AIRFLOW_CFG_*_postgres_conn_id` | по умолчанию `postgres_default` |
| `AIRFLOW_CFG_*_google_conn_id` | по умолчанию `google_cloud_default` |

Подключения: `postgres_default` (Postgres с таблицей `sales`), `google_cloud_default` (сервисный аккаунт Google Sheets API) — заводятся в Admin → Connections.

## Поток данных

```
Postgres.sales
   │  daily_window / weekly_window  [logical_date-N, logical_date)
   ▼
fetch_sales / fetch_weekly   ── SQL-агрегат ──►  rows
   ▼
validate_data / validate_weekly   ── DQ: header, revenue/orders >= 0, no-data ──► rows | NoDataError
   ▼
write_to_sheets / write_weekly   ──►  Google Sheets  |  CSV (data/out/)
   ▼
record_metrics   ──►  dag_metrics (success rate, p95 duration)
   падение ──► on_failure_callback ──► alert (лог) + failed-метрика
```

## Observability

Каждый успешный запуск пишет строку в таблицу `dag_metrics` (Postgres): `dag_id`, `run_id`, `status`, `duration_s`, `rows_written`, `logical_date`. Провал фиксируется в DAG-level `on_failure_callback` (алерт + `failed`-метрика). `scripts/metrics_report.py` агрегирует success rate и p95 duration за N дней. Таблицы `sales` и `dag_metrics` создаются скриптом-генератором и DAG-ами автоматически (`CREATE TABLE IF NOT EXISTS`).

## Документация

Полный PRD (проблема, требования REQ-001…REQ-011, дорожная карта, риски) — `docs/prd.md`.

## Статус

Портфельный проект; источник данных — синтетический (`scripts/generate_sales_data.py`). Google Sheets как приёмник требует реального сервисного аккаунта; dev-режим `output="file"` работает без внешних API.

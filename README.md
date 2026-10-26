# Airflow DAG Platform

[![CI](https://github.com/NikitaBoyarkin/airflow/actions/workflows/ci.yml/badge.svg)](https://github.com/NikitaBoyarkin/airflow/actions/workflows/ci.yml)

> Каждый отчёт — версионируемый DAG: 3 DAG-а и 72 теста заменяют ручную выгрузку идемпотентным пайплайном с DQ-валидацией.

## Navigation
- 🌐 [Портфолио](https://nikitaboyarkin.github.io)
- 🏦 [Флагман: Volta neobank](https://github.com/NikitaBoyarkin/volta-banking)
- 🧪 Связанные: [supabase-product-analytics](https://github.com/NikitaBoyarkin/supabase-product-analytics) · [streamlit-app](https://github.com/NikitaBoyarkin/streamlit-app) · [airflow](https://github.com/NikitaBoyarkin/airflow)

**▶ [Диаграмма: Airflow DAG-платформа: идемпотентный поток отчёта](docs/diagrams/airflow.html)** — открывается в браузере, без установки и сборки

**На этой странице:** [Ситуация](#ситуация) · [Задача](#задача) · [Действия](#действия) · [Результат](#результат) · [DAG-и](#dag-и) · [Структура](#структура) · [Стек](#стек) · [Запуск](#запуск) · [Качество кода](#качество-кода) · [Поток данных](#поток-данных) · [Observability](#observability)

## Ситуация

Ручной отчёт по продажам (SQL-клиент → копирование в Sheets) давал задержки, ошибки копирования и отсутствие воспроизводимости.

## Задача

Заменить ручную выгрузку агрегатов из Postgres воспроизводимым, идемпотентным пайплайном с DQ-валидацией, алертами и метриками запусков.

## Действия

- Оформил каждый отчёт как версионируемый DAG в Airflow 3 по общему шаблону `fetch → validate → write → record_metrics` (`@task`-декораторы, Airflow 3 SDK).
- Вынес платформенные куски в `shared/`: `config.py` (Airflow Variables), `dq.py`, `alerts.py`, `metrics.py`, `output.py`, `windows.py`.
- Задал полуинтервальное окно `[logical_date − N, logical_date)` и `catchup=False`; backfill — явно через `airflow dags backfill`.
- Сделал приёмником Google Sheets (или CSV-фолбэк `output="file"`).
- Добавил observability: каждый успешный запуск пишет строку в `dag_metrics`, падение ловит `on_failure_callback`.
- Собрал CI из четырёх гейтов: `ruff check`, `ruff format --check`, `mypy`, `pytest`.

## Результат

- 3 DAG-а (`daily_sales_report`, `weekly_sales_summary`, `delivery_control_tower`) и 72 теста без Airflow-сервисов и БД; новый отчёт добавляется по общему шаблону.
- `scripts/metrics_report.py` агрегирует success rate и p95 duration за N дней.

Портфельный Data Engineering проект: платформа отчётов на Apache Airflow 3, где каждый отчёт — версионируемый DAG. Заменяет ручную выгрузку агрегатов из Postgres и перенос в Google Sheets воспроизводимым, идемпотентным пайплайном с DQ-валидацией, алертами и метриками запусков.

## Что решает

Ручной отчёт по продажам (SQL-клиент → копирование в Sheets) давал задержки, ошибки копирования и отсутствие воспроизводимости. Платформа доводит данные до приёмника по расписанию без ручного вмешательства, падения видны сразу через алерты, новый отчёт добавляется по общему шаблону переиспользования shared-модулей.

## DAG-и

| DAG | Расписание | Что делает | Приёмник |
|-----|-----------|------------|----------|
| `daily_sales_report` | `@daily` | Агрегат продаж за сутки (`sale_date`, `product_id`, `revenue`, `orders`) | Google Sheets (или CSV-фолбэк) |
| `weekly_sales_summary` | `@weekly` | Сводка продаж за 7 суток по `category` (`category`, `revenue`, `orders`) | CSV (по умолчанию) или Google Sheets |
| `delivery_control_tower` | вручную (`schedule=None`) | 8 задач: CSV → staging → DQ-гейт → витрины DuckDB → 5 анализов параллельно | файлы проекта `delivery-control-tower` |

Каждый DAG строится по шаблону `fetch → validate → write → record_metrics` (`@task`-декораторы, Airflow 3 SDK). Окно — полуинтервал `[logical_date − N, logical_date)`; `catchup=False` — пропущенные периоды не догоняются, backfill явно через `airflow dags backfill`.

`delivery_control_tower` — пример оркестрации **чужого** проекта: у платформы нет pandas/duckdb, у `delivery-control-tower` нет Airflow, поэтому каждая задача запускает `uv run python -m delivery.<module>` в окружении того проекта через `subprocess`. Источник статический (агрегат за 2022-01-01…2022-06-30), отсюда `schedule=None`. Замер прогона 2026-10-25: 440 с, из них 413 с — задача `forecast`; параллельная ветка после витрин экономит ~30 с, то есть ценность оркестратора здесь в DQ-гейте и видимости падений, а не в скорости.

## Структура

```
dags/
  daily_sales_report.py     # DAG: Postgres -> Google Sheets / CSV
  weekly_sales_summary.py   # DAG: Postgres -> CSV (сводка по категориям)
  delivery_control_tower.py # DAG: внешний проект delivery-control-tower (DuckDB)
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
airflow_home/               # AIRFLOW_HOME: cfg, sqlite-метастор, логи (gitignored)
```

## Стек

- **Оркестрация:** Apache Airflow 3.3 (SDK, `@task`-декораторы), локальный standalone-режим
- **Провайдеры:** `apache-airflow-providers-postgres`, `apache-airflow-providers-google`
- **Источник:** Postgres 16 (демо: `127.0.0.1:5434`, БД `airflow_demo`)
- **Приёмник:** Google Sheets API (через `GSheetsHook`); dev-фолбэк — CSV в `data/out/`
- **Python:** 3.14, менеджер пакетов `uv`, тесты `pytest`

## Запуск

```bash
uv sync                                   # зависимости для DAG-ов и тестов
uv sync --group eda                       # + pandas/matplotlib для reports/eda/
uv run pytest                             # тесты (без Airflow runtime)

# Демо-данные в Postgres (синтетика, 10 000 строк/день; замер 2026-10-25 — 70 000 за 7 суток):
uv run python scripts/generate_sales_data.py --days 7 --rows-per-day 10000

# Airflow standalone (UI http://localhost:8082):
export AIRFLOW_HOME=$(pwd)/airflow_home
uv run airflow standalone

# Отчёт по метрикам запусков:
uv run python scripts/metrics_report.py --days 7
```

CLI-команды Airflow требуют `AIRFLOW_HOME`, иначе они смотрят в `~/airflow` и падают с «Database migration required».

### Свежий клон: три переменные до первого запуска

`airflow_home/` в `.gitignore` — это конфиг машины, а не репозитория, поэтому на новом клоне файла `airflow_home/airflow.cfg` не существует. Порт занят (8080 — Trino, 8081 — личный инстанс из `~/airflow`), и выставить один только порт API недостаточно:

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
export AIRFLOW__API__PORT=8082
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://localhost:8082/execution/
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
uv run airflow standalone
```

**Почему третий ключ обязателен.** `airflow standalone` поднимает api-server на `[api] port`, но LocalExecutor отправляет задачи на `[core] execution_api_server_url`, который по умолчанию остаётся `http://localhost:8080/execution/` и за портом API не следует. На этой машине на 8080 отвечает Trino: 404, после чего Airflow 3.3.2 падает в цикле с `TypeError: HTTPStatusError.__init__() missing 2 required keyword-only arguments: 'request' and 'response'` (баг обработки ошибки httpx). Симптом: dag_run висит в `running`, первая задача — вечно `queued`, при этом UI открывается нормально.

Проверка после старта:

```bash
uv run airflow config get-value core execution_api_server_url   # ожидаем http://localhost:8082/execution/
```

Логин `admin`, пароль генерируется при первом запуске и лежит в `airflow_home/simple_auth_manager_passwords.json.generated`.

### Docker Compose (запуск с нуля без локального Airflow)

```bash
cp .env.example .env
echo "AIRFLOW_UID=$(id -u)" >> .env        # чтобы контейнер писал CSV в ./data
docker compose up -d --build

# Демо-данные в контейнерный Postgres (сервис `postgres`, порт внутри 5432):
docker compose run --rm -e PGPASSWORD=postgres airflow \
    python scripts/generate_sales_data.py \
    --host postgres --port 5432 --user postgres --db airflow_demo --days 7
```

UI — http://localhost:8082, логин `admin`, пароль печатается в `docker compose logs airflow` при первом старте. DAG-и создаются сразу включёнными (`DAGS_ARE_PAUSED_AT_CREATION=false`); если метастор остался с прошлого запуска — `docker compose exec airflow airflow dags unpause daily_sales_report`, иначе trigger висит в `queued`.

| Сервис | Host | Контейнер |
|---|---|---|
| Airflow UI | `8082` (`AIRFLOW_PORT`) | `8080` |
| Postgres | `5434` (`POSTGRES_PORT`) | `5432` |

Внутри контейнера `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` указывает на внутренний `8080` — смена наружного `AIRFLOW_PORT` не требует правки этого ключа (в отличие от локального сценария выше). DAG-и стартуют в dev-режиме `output="file"` (`AIRFLOW_VAR_AIRFLOW_CFG_*_OUTPUT=file`), поэтому Google-креды не нужны, а CSV появляются в `data/out/`. `delivery_control_tower` в контейнере не запускается: ему нужен sibling-репозиторий и `uv`, которых в образе нет — этот DAG остаётся host-only. Образ собран на Python 3.12 (официальные образы Airflow не собираются под 3.14 из `pyproject.toml`), локальная разработка остаётся на 3.14 через `uv`.

## Качество кода

CI (`.github/workflows/ci.yml`) на каждый push и PR гоняет четыре гейта:

```bash
uv run ruff check .          # линт: E,W,F,I,B,C4,UP (line-length игнорируется)
uv run ruff format --check . # формат; применить — uv run ruff format .
uv run mypy                  # типы: dags/, scripts/, tests/
uv run pytest                # 72 теста, без Airflow-сервисов и БД (хуки замоканы)
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

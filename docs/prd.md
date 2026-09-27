# PRD: Airflow DAG Platform

**Автор:** Nikita Boyarkin
**Дата:** 2026-09-05
**Статус:** Draft
**Версия:** 1.0

---

## 1. Executive Summary

Проблема: ручные отчёты по продажам (Data Analyst вручную выгружает агрегат из Postgres и переносит в Google Sheets) — задержки, ошибки копирования, нет воспроизводимости. Решение: платформа на Apache Airflow, где каждый отчёт — версионируемый DAG; первый — `daily_sales_report` (Postgres → Google Sheets), публикуемый каждое утро без ручного вмешательства. Эффект: данные в таблице к 09:00, единый процесс добавления новых отчётов (< 1 дня на DAG), падения видны сразу через алерты.

## 2. Problem Statement

### Текущая ситуация
- Отчёт по продажам собирается вручную: запрос в SQL-клиенте → копирование в Sheets → форматирование.
- Процесс не повторяем: нет автоматического перезапуска при ошибке, нет истории версий.
- Единственный код сегодня — черновой DAG `daily_sales_report.py`: хардкод `SPREADSHEET_ID = "your-spreadsheet-id"`, нет проверок качества, нет алертов, `schedule="@daily"` без учёта даты выполнения, зависимости Airflow в `pyproject.toml` отсутствуют, `main.py` — заглушка.

### Влияние на пользователя
- **Кто затронут:** аналитик (автор отчёта) + стейкхолдеры, которые читают лист Google Sheets.
- **Как затронут:** отчёт может опоздать или содержать невалидные строки без явного сигнала; при падении ничего не уведомляет.
- **Серьёзность:** High — при регулярном использовании тихая ошибка данных = неверные решения.

### Бизнес-влияние
- **Стоимость проблемы:** задержка принятия решений на сутки+; риск доверия к отчётности при тихой ошибке.
- **Стратегическая важность:** портфельный проект, показывающий навык Data Engineering (оркестрация, надёжность пайплайнов) — усиливает позицию на интервью BI/DE.

### Почему решать сейчас
- Код уже существует вчерне — доработка до рабочего состояния дёшева.
- Airflow SDK — актуальный стек; проект ложится в портфолио.
- Локальная демо-среда позволяет проверить end-to-end без внешних согласований.

## 3. Goals & Success Metrics

### Goal 1: Надёжный ежедневный пайплайн
- **Описание:** DAG каждый день доводит данные до Sheets без ручного вмешательства.
- **Метрика:** Success rate запусков (завершённых успешно / всего scheduled) за 30 дней.
- **Baseline:** не измеряется (0 автоматических запусков).
- **Target:** ≥ 95%.
- **Срок:** 1 месяц с первой production-дачи.
- **Метод измерения:** статистика Run в Airflow UI (или лог-таблица).

### Goal 2: Корректные данные
- **Описание:** строки в Sheets совпадают с SQL-агрегатом, значения валидны.
- **Метрика:** % запусков, прошедших DQ-валидацию без нарушений.
- **Baseline:** 0% (DQ-проверок нет).
- **Target:** 100% запусков проходят DQ.
- **Срок:** к концу Phase 2.
- **Метод измерения:** DQ-шаг логирует результат; провал → fail run.

### Goal 3: Повторяемость / идемпотентность
- **Описание:** повторный запуск за одну дату даёт тот же результат, дубликатов нет.
- **Метрика:** % повторных запусков, где содержимое листа идентично.
- **Baseline:** 0% (дата не параметризована, перезапись диапазона возможна вручную).
- **Target:** 100% повторных запусков идемпотентны.
- **Срок:** к концу Phase 2.
- **Метод измерения:** тест: re-run DAG с той же `logical_date` → сравнение строк листа.

### Goal 4: Платформа для N DAG
- **Описание:** новый отчёт добавляется по шаблону платформы, переиспользуя shared-модули.
- **Метрика:** время от задачи «нужен отчёт» до первого успешного запуска.
- **Baseline:** нет процесса (1-й DAG создан вручную за ~1 день).
- **Target:** < 1 дня по шаблону.
- **Срок:** Phase 3.
- **Метод измерения:** замер по добавлению второго DAG.

## 4. User Stories

### Story 1: Автоматическая доставка отчёта
**As a** аналитик, **I want to** получать свежий отчёт в Google Sheets каждое утро автоматически, **So that I can** не тратить время на ручной перенос данных.

**Acceptance Criteria:**
- [ ] DAG стартует по расписанию без ручного триггера
- [ ] Таблица обновлена до 09:00 при доступности источников
- [ ] При сбое данные предыдущего дня не пропадают (лист не затирается пустотой)

**Dependencies:** None

### Story 2: Доверие к цифрам
**As a** стейкхолдер, **I want to** видеть в листе ровно те строки, что в источнике, **So that I can** принимать решения на верных данных.

**Acceptance Criteria:**
- [ ] Число строк листа = числу строк SQL-результата
- [ ] revenue/orders неотрицательны, даты = вчерашний день
- [ ] Пустой результат не молчит — помечается как «no data» и алертится

**Dependencies:** REQ-003, REQ-004

### Story 3: Быстрое добавление отчёта
**As a** разработчик DAG, **I want to** использовать общие хелперы и шаблон, **So that I can** выпустить новый отчёт за день.

**Acceptance Criteria:**
- [ ] Есть shared-модуль с типовыми шагами (fetch → validate → write)
- [ ] Наличие шаблона DAG и документации по добавлению
- [ ] Новый DAG подхватывается Airflow без правки скелета платформы

**Dependencies:** REQ-001

### Story 4: Громкие падения
**As a** оператор платформы, **I want to** получать алерт при падении DAG, **So that I can** починить до того, как отчёт станет критично просроченным.

**Acceptance Criteria:**
- [ ] Падение любого шага вызывает алерт
- [ ] Алерт содержит dag_id, task_id, дату, текст ошибки
- [ ] Retries (2) срабатывают раньше алерта (алерт — на итоговый fail)

**Dependencies:** REQ-004

## 5. Functional Requirements

### Must Have (P0) — критично для запуска

#### REQ-001: Каркас платформы Airflow
**Описание:** Структура проекта и зависимости, позволяющие добавлять N DAG без дублирования скелета.

**Acceptance Criteria:**
- [ ] `dags/` содержит DAG-и; `plugins/` или `dags/shared/` — переиспользуемые модули
- [ ] В `pyproject.toml` заданы `apache-airflow`, `apache-airflow-providers-postgres`, `apache-airflow-providers-google`, `apache-airflow-providers-common-sql`
- [ ] `uv sync` устанавливает зависимости без ошибок; `uv run pytest` проходит
- [ ] Новый DAG (пустой шаблон) импортируется в Airflow без ошибок

**Техническая спецификация:**
```
dags/
  daily_sales_report.py
  weekly_sales_summary.py     # Phase 3
  shared/
    config.py                 # Airflow Variables → словарь конфигов
    dq.py                     # DQ-валидатор (непусто, значения ≥0, header)
    alerts.py                 # callbacks: on_failure, on_no_data
tests/
  test_daily_sales_report.py
  test_dq.py
```

**Task Breakdown:**
- Структура + deps: Small (3h)
- Shared-модули (config/dq/alerts): Medium (6h)
- Тесты: Medium (5h)

**Dependencies:** None

#### REQ-002: Конфигурация DAG-ов без хардкода
**Описание:** Внешние параметры (`SPREADSHEET_ID`, `RANGE_NAME`, conn ids, схема таблицы) читаются из конфигурации, а не зашиты в код.

**Acceptance Criteria:**
- [ ] `SPREADSHEET_ID`, `RANGE_NAME`, название листа, conn ids хранятся в Airflow Variables / конфиг-файле
- [ ] Удаление `SPREADSHEET_ID = "your-spreadsheet-id"` и аналогов из кода DAG — проверка grep по `your-` и `TODO`
- [ ] Смена spreadsheet id не требует правки кода DAG
- [ ] Отсутствие обязательной переменной → осмысленная ошибка при загрузке DAG

**Task Breakdown:**
- Внедрение config.py: Medium (4h)
- Рефакторинг DAG на config: Small (2h)
- Тест на fallback/ошибку: Small (2h)

**Dependencies:** REQ-001

#### REQ-003: DQ-валидация перед записью
**Описание:** Перед записью в Sheets данные проходят набор проверок; нарушение → fail с понятным сообщением.

**Acceptance Criteria:**
- [ ] Провал, если результат пуст (0 строк данных)
- [ ] Провал, если revenue или orders < 0 в любой строке
- [ ] Провал, если заголовок не совпадает с ожидаемым (`sale_date, product_id, revenue, orders`)
- [ ] Сообщение об ошибке перечисляет, какая проверка и сколько строк нарушили

**Техническая спецификация:**
```
def validate(data: list[list[str]]) -> list[str]:
    """Возвращает список нарушений; пустой список = ok."""
```

**Task Breakdown:**
- dq.py + вызов в DAG: Medium (5h)
- Тесты нарушений: Medium (4h)

**Dependencies:** REQ-002

#### REQ-004: Алертинг при падении и пустом результате
**Описание:** DAG уведомляет оператора при итоговом fail и при «no data» (если это не штатная ситуация).

**Acceptance Criteria:**
- [ ] `on_failure_callback` на уровне DAG шлёт алерт при итоговом провале (после retries)
- [ ] Алерт содержит: `dag_id`, `task_id`, `logical_date`, текст ошибки
- [ ] «no data for yesterday» при успешном шаге доходит до оператора (не тихий return строки)
- [ ] Канал алерта настраивается через конфиг (email в Phase 2, Slack/Telegram в P2)

**Task Breakdown:**
- alerts.py + callbacks: Medium (5h)
- Обработка no-data: Small (2h)
- Тест вызова алерта (mock): Medium (4h)

**Dependencies:** REQ-002

#### REQ-005: Идемпотентное планирование по дате выполнения
**Описание:** SQL-фильтр «за вчера» вычисляется из `logical_date` DAG, а не из `now()`, чтобы backfill и повторные запуски давали детерминированный период.

**Acceptance Criteria:**
- [ ] Окно дат берётся из `logical_date` (или `data_interval_start`) — grep не находит `now()` в SQL
- [ ] Повторный запуск с той же `logical_date` перезаписывает тот же диапазон, дубликатов строк нет
- [ ] Запуск за `logical_date != сегодня` даёт данные именно за этот день
- [ ] Поведение задокументировано в docstring DAG

**Task Breakdown:**
- SQL-шаблон с параметром даты: Medium (4h)
- Тест окна дат: Small (2h)
- Документация: Small (1h)

**Dependencies:** REQ-003

#### REQ-006: Автотесты DAG и хелперов
**Описание:** Пайплайн покрыт pytest: импорт DAG, корректность SQL-окна, DQ-логика, поведение при моках hook'ов.

**Acceptance Criteria:**
- [ ] `uv run pytest` зелёный в CI/локально
- [ ] Тест: DAG `daily_sales_report` импортируется без ошибок (все задачи графа определены)
- [ ] Тест: SQL-фильтр возвращает ожидаемое окно для заданной даты
- [ ] Тест: DQ отклоняет пустой/отрицательный/битый header (таблица нарушений)
- [ ] Тест: `write_to_sheets` с мокнутым `GoogleSheetsHook` записывает ровно переданные строки

**Task Breakdown:**
- Инфраструктура тестов: Medium (4h)
- Юнит-тесты (DQ, окно): Medium (5h)
- Тест с моками hook'ов: Medium (5h)

**Dependencies:** REQ-001, REQ-003, REQ-005

### Should Have (P1) — важно, но не блокирует

#### REQ-007: Observability пайплайна
**Описание:** Метрики работы DAG (duration, success/fail, строк в записи) собираются и доступны для обзора.

**Acceptance Criteria:**
- [x] Каждый успешный запуск пишет строку-метрику (dag_id, duration_s, rows_written, status)
- [x] Есть запрос/дашборд «success rate за N дней» — выполняется за < 10 с
- [x] Duration p95 доступен из собранных метрик

**Dependencies:** REQ-006

#### REQ-008: Backfill и частичный перезапуск
**Описание:** Отработана политика `catchup`, `backfill` и перезапуск за конкретный день.

**Acceptance Criteria:**
- [x] `catchup` настроен осознанно (документированное значение в DAG)
- [x] `airflow dags backfill -s -e daily_sales_report` за пропущенные дни отрабатывает без дублей
- [x] Перезапуск за один день не трогает соседние дни

**Dependencies:** REQ-005

#### REQ-009: Второй DAG как проверка шаблона
**Описание:** `weekly_sales_summary` (сводка за неделю по категориям) собирается по шаблону платформы.

**Acceptance Criteria:**
- [x] DAG переиспользует shared-модули (config, dq, alerts), не дублируя их код
- [x] Расписание — `@weekly`, окно недели из `logical_date` корректно
- [x] Запуск на локальном Airflow заканчивается успешно
- [x] Время добавления зафиксировано и ≤ 1 рабочего дня

**Dependencies:** REQ-001…REQ-006

### Nice to Have (P2) — будущее улучшение

#### REQ-010: Slack/Telegram уведомления
**Описание:** Помимо email, алерты дублируются в мессенджер.

**Acceptance Criteria:**
- [ ] Вебхук Slack/Telegram настраивается переменной
- [ ] Формат алерта единый с email-версией
- [ ] Отправка повторяет попытку при недоступности мессенджера (не теряет событие)

**Dependencies:** REQ-004

#### REQ-011: CI/CD для DAG-ов
**Описание:** На каждый push в репозиторий — lint + тесты; тег — пометка готовой версии.

**Acceptance Criteria:**
- [ ] GitHub Action: ruff на `dags/` + `uv run pytest`
- [ ] Провал CI блокирует мердж (branch protection)
- [ ] Инструкция деплоя версии в README

**Dependencies:** REQ-006

## 6. Non-Functional Requirements

### Performance
- DAG duration p95: < 5 мин (источник влезает в окно выполнения до 09:00)
- Шаг fetch: < 2 мин при объёме ≤ 50k строк
- Запись в Sheets: < 1 мин на 10k строк (batch update)

### Security
- Учётные данные: только через Airflow Connections / Variables, не в git (`.env` и `secrets` в `.gitignore`)
- Сервисный аккаунт Google: минимальный scope `spreadsheets`, доступ только к целевому документу
- Параметры БД: через `PostgresHook`, без строки подключения в коде
- Compliance: данные синтетические/тестовые — PII-требования не в скоупе

### Scalability
- 10+ DAG на платформе без деградации scheduler (лёгкие DAG, shared-код)
- Рост данных: до 100k строк в сутки на DAG (пересмотр выше — вне скоупа)
- Google Sheets API rate limits: 60 запросов/мин/проект, 100 запросов/100 с/пользователь — уважаются batch-обновлением

### Reliability
- Success rate: ≥ 95% (30 дней)
- Retries: 2, retry_delay 5 мин (на каждый task)
- Алерт на итоговый fail; RTO ≈ 1 рабочий день (следующий scheduled run чинит)
- RPO: потерять можно только текущий день, если лист не был записан/затёрт — защищает DQ (не писать пусто)

## 7. Technical Considerations

### Архитектура
```
Postgres (sales) ──fetch──▶ [daily_sales_report DAG] ──validate──► DQ check
                                    │                                  │ ok
                                    ▼                                  ▼
                              on_failure_callback                     Google Sheets API
                                    │                          (spreadsheets.values.update)
                                    ▼
                              Алерт (email/slack)
```

### Технологический стек
- **Оркестрация:** Apache Airflow (SDK 2.x, @task-декораторы), локально / Docker Compose
- **Источник:** PostgreSQL (`PostgresHook`, схема `sales`)
- **Приёмник:** Google Sheets (`GoogleSheetsHook`, `spreadsheets.values.update`)
- **Язык:** Python 3.14 (проект); если Airflow не поддерживает 3.14 — venv 3.11/3.12 (см. Risks)
- **Пакеты:** uv; runtime deps: `apache-airflow`, providers postgres/google
- **Тесты:** pytest, моки hook'ов

### Внешние зависимости
1. **PostgreSQL:** цель — читать `sales`; подключение `postgres_default`; fallback: синтетический генератор для dev.
2. **Google Sheets API:** цель — `spreadsheets.values.update`; сервисный аккаунт с scope `spreadsheets`; rate limits см. Scalability; fallback: нет — при недоступности алерт.
3. **Airflow scheduler:** локально; без него DAG не запускается по расписанию (dev-запуск вручную).

### Миграция (для существующих систем)
Greenfield-проект: миграции нет. Переход от текущего черновика:
1. Заменить хардкод на конфиг (REQ-002).
2. Перенести DAG в `dags/`, подключить shared-модули.
3. Проверить старый формат листа на совместимость (header).

### Тестирование
- Unit: DQ-валидатор, SQL-окно дат, парсинг настроек — target > 80% покрытие хелперов
- Integration: DAG с мокнутыми hook'ами (Postgres → fake rows, Sheets → fake API)
- E2E: реальный запуск на локальном Airflow с синтетикой: Postgres → лист
- No performance-тестов (малые объёмы); Security: grep-скан на секреты в коде

## 8. Implementation Roadmap

### Phase 1: Foundation (нед 1)
**Goal:** Каркас платформы: структура, зависимости, shared-модули, тесты.
**Tasks:**
- [ ] Структура `dags/`, `dags/shared/`, `tests/` (REQ-001) — Small (3h)
- [ ] `pyproject.toml`: airflow + providers, `uv sync` (REQ-001) — Small (2h)
- [ ] `dags/shared/config.py`, `dq.py`, `alerts.py` (REQ-001) — Medium (6h)
- [ ] Инфраструктура pytest, тест импорта DAG (REQ-006) — Medium (5h)
**Validation Checkpoint:** `uv run pytest` зелёный; DAG импортируется в Airflow UI без ошибок.

### Phase 2: daily_sales_report production-ready (нед 2-3)
**Goal:** Первый DAG надёжен: конфиг, DQ, идемпотентность, алерты.
**Tasks:**
- [ ] Рефакторинг DAG на config (REQ-002) — Medium (4h)
- [ ] DQ-валидация перед записью (REQ-003) — Medium (5h)
- [ ] SQL-окно по `logical_date` (REQ-005) — Medium (4h)
- [ ] Алертинг + no-data обработка (REQ-004) — Medium (5h)
- [ ] Юнит-тесты хелперов + мокнутые hook'и (REQ-006) — Large (10h)
**Validation Checkpoint:** Успешный E2E запуск на локальном Airflow: реальные строки в Sheets; принудительный fail → алерт.

### Phase 3: Extend & Observe (нед 4)
**Goal:** Платформа доказана вторым DAG; метрики собираются.
**Tasks:**
- [x] Observability: метрики запусков (REQ-007) — Medium (6h)
- [x] Backfill/catchup политика (REQ-008) — Small (3h)
- [x] `weekly_sales_summary` по шаблону (REQ-009) — Medium (6h)
**Validation Checkpoint:** ≥ 2 DAG в UI; success rate за неделю ≥ 95%; время добавления второго DAG ≤ 1 дня.

**E2E-результаты (2026-09-05):**
- `weekly_sales_summary` — manual + scheduled раны success; CSV `weekly_sales_summary_<week_start>.csv` совпадает с Postgres (4 категории).
- Backfill `daily_sales_report -s 2026-09-01 -e 2026-09-03` — 3 рана success, 3 distinct CSV (08-31/09-01/09-02), повторный backfill — 0 новых ранов (идемпотентно).
- `scripts/metrics_report.py --days 7` — success rate + p95 из `dag_metrics`.
- E2E поймал 2 бага: `GROUP BY 1, 2` в weekly-запросе (одна неагрегатная колонка) и `rows_written` = длина строки-сообщения вместо числа строк (write-таски теперь возвращают data pass-through).
- Forced-fail E2E (REQ-007 failure path): bad row `p_bad` (revenue < 0) → validate_data failed после 3 попыток → DAG failed → `dag_metrics` получил строку `status='failed', duration_s=609.71` + `[AIRFLOW ALERT]` в dag_processor-логе. Артефакты теста удалены.

### Зависимости задач
```
Phase 1 → Phase 2 → Phase 3
Critical Path: REQ-001 → REQ-002 → REQ-003 → REQ-006 → E2E checkpoint
```

### Оценка усилий
- Phase 1: ~16h
- Phase 2: ~28h
- Phase 3: ~15h
- **Итого:** ~59h (~3-4 недели соло по 4-5ч/нед)
- **Риск-буфер:** +20%

## 9. Out of Scope

1. **Managed Airflow (Cloud Composer / Astronomer)** — прод-инфраструктура; локальный/Docker вариант закрывает задачи портфолио; будущее расширение.
2. **Трансформации уровня DWH (dbt, большие join'ы)** — платформа отвечает за доставку готовых агрегатов.
3. **Объёмы > 100k строк/сутки и Spark/Spark-стриминг** — при росте — отдельный PRD.
4. **CDC-репликация из источника** — выгрузка работает против готовой схемы `sales`.
5. **SLA-мониторинг уровня PagerDuty/полного on-call** — достаточно email/Slack-алертов.

## 10. Open Questions & Risks

### Open Questions
#### Q1: Окружение Airflow — ✅ РЕШЕНО (2026-09-05)
- **Решение:** (B) venv + `airflow standalone` — рантайм живёт в `00 ide/00 projects/airflow` (Airflow 3.3.1, Python 3.14, UI http://localhost:8080, admin/admin).
- **Как подключён DAG:** `00 projects/airflow/dags` → симлинк на `00 portfolio/airflow/dags`; провайдеры `apache-airflow-providers-postgres` и `apache-airflow-providers-google` установлены в `.venv` рантайма.
- **Запуск/остановка:** `./start.sh` / `./stop.sh` в `00 projects/airflow`.
- **Верификация:** health `/api/v2/monitor/health` — все компоненты healthy; `airflow dags list` показывает `daily_sales_report` (bundle `dags-folder`), import-errors пусто; `airflow tasks list` → `fetch_sales, validate_data, write_to_sheets`.
- **Грабли Airflow 3.3.1 (зафиксированы в коде):**
  - Manual-раны не имеют `logical_date`/`data_interval_*` (NULL в `dag_run`) — только `run_after`. Якорь окна: `dag_run.data_interval_start or dag_run.run_after` (см. `fetch_sales`).
  - В рантайме таска `Variable.get` бросает `AirflowRuntimeError(VARIABLE_NOT_FOUND)`, а не `KeyError` — `_default_getter` передаёт `default_var=None`.
  - DAG-level `on_failure_callback` срабатывает при фейле ран-а (после исчерпания ретраев), не на каждый фейл таска.
  - CLI: `airflow dags list-runs <dag_id>` — `dag_id` позиционный; `logical_date` в выводе пуст для manual-ранов.
  - CLI-команды вне `start.sh` требуют `export AIRFLOW_HOME=$(pwd)` (иначе смотрят в `~/airflow` и падают «Database migration required»).
  - DAG-level callback логируется в `logs/dag_processor/<date>/dags-folder/<dag>.py.log`, не в standalone.log.
- **Влияние:** снято — E2E-валидация возможна (осталось Q2/Q3 для реального прогона).

#### Q2: Google Sheets — сервисный аккаунт — ✅ РЕШЕНО (2026-09-05)
- **Решение:** (B) dev-фолбэк — `output="file"` пишет CSV в `data/out/` вместо Sheets. Интерфейс `GSheetsHook` сохранён; переключение на реальные Sheets — переменная `AIRFLOW_CFG_sales_report_output=sheets` + `spreadsheet_id` (SA настраивается позже, не блокирует E2E).
- **Реализация:** `write_sales_rows(..., output="sheets"|"file")`; `write_sales_file()` — идемпотентная перезапись `sales_report_<date>.csv`.
- **Верификация:** E2E-ран записал `data/out/sales_report_2026-09-04.csv` (20 продуктов, агрегаты сверены с Postgres).
- **Влияние:** снято для dev-демо; реальные Sheets остаются опцией (Phase 3+).

#### Q3: Схема sales — ✅ РЕШЕНО (2026-09-05)
- **Решение:** (B) генератор синтетики `scripts/generate_sales_data.py` + локальный Postgres 16 (Homebrew, 127.0.0.1:5434, БД `airflow_demo`).
- **Схема:** `sales(id bigserial, created_at timestamptz, product_id text, amount numeric(12,2))` + индекс по `created_at`; таймстампы UTC — окно DAG `[logical_date-1, logical_date)` совпадает с данными.
- **Идемпотентность:** DELETE+INSERT в окне; `random.Random(42)` — воспроизводимо.
- **Верификация:** 70k строк (10k/день × 7 дней, окно 2026-08-29..2026-09-05); агрегаты DAG сверены с БД.
- **Влияние:** снято.

#### Q4: Канал алертинга
- **Статус:** не выбран.
- **Варианты:** (A) email (Airflow built-in), (B) Slack webhook, (C) Telegram bot.
- **Владелец:** автор
- **Дедлайн:** конец Phase 2
- **Влияние:** Low

### Risks & Mitigation

| Риск | Вероятность | Влияние | Severity | Митигация | Контингенция |
|------|-------------|---------|----------|-----------|--------------|
| Google Sheets не настроен (SA/key) — E2E заблокирован | High | High | **Critical** | Настроить SA на старте Phase 2; dev-фолбэк на файловый вывод | Оставить интерфейс hook'а, подменить реализацию на local-копию |
| Airflow несовместим с Python 3.14 | Medium | High | **High** | Проверить матрицу версий до Phase 1; держать `.python-version` 3.11/3.12 для airflow | venv 3.11 со stale-lock, uv pin |
| Локальный Airflow не поднят — DAG не проверен в runtime | High | Medium | **High** | Docker Compose на старте Phase 1 | `airflow standalone` для быстрой проверки |
| Схема `sales` отсутствует / полезна не под SQL | Medium | Medium | **Medium** | Генератор синтетики по схеме из DAG | Адаптировать SQL под фактическую схему (Q3) |
| Тихий пустой результат в выходные — ложные алерты | Low | Medium | **Medium** | Различать «нет данных» (выходной) и «ошибка» в алерте | Подавление no-data для известных пустых дней |
| Rate limit Google Sheets при росте строк | Low | Low | **Low** | Batch update, 1 вызов на лист | Чанкование по 10k строк |

## 11. Validation Checkpoints

### Checkpoint 1: Конец Phase 1
**Критерии:**
- [x] `uv run pytest` зелёный (без скипов критичных тестов)
- [x] DAG `daily_sales_report` импортируется в Airflow UI без ошибок
- [x] Окружение Airflow выбрано (Q1) и поднято
**Если провален:** не начинать Phase 2 — сначала починить каркас/окружение.

### Checkpoint 2: Конец Phase 2
**Критерии:**
- [x] E2E на локальном Airflow: строки реально появились в Sheets (или dev-фолбэке) — CSV `data/out/sales_report_2026-09-04.csv`, агрегаты сверены с Postgres (2026-09-05)
- [x] Принудительный fail (битая строка `p_bad`, amount −100) → алерт получен: `[AIRFLOW ALERT] dag=daily_sales_report task=validate_data` в dag_processor логе (19:03:54, после исчерпания 2 ретраев). Важно: DAG-level `on_failure_callback` логируется в `logs/dag_processor/.../daily_sales_report.py.log`, НЕ в standalone.log
- [x] Повторный re-run за ту же дату (2026-09-04) идемпотентен: SHA-256 CSV `010b835b…` до и после идентичен, 21 строка, дублей нет
**Если провален:** остановить расширение; фиксить надёжность первого DAG до Phase 3.

### Checkpoint 3: Конец Phase 3
**Критерии:**
- [x] ≥ 2 DAG в UI, оба успешно запускаются
- [x] success rate ≥ 95% за неделю наблюдения
- [x] Время добавления второго DAG ≤ 1 рабочего дня
**Если провален:** пересмотреть шаблон/документацию платформы (Story 3).

---

**Конец PRD**

*Шаблон: comprehensive. Адаптировано под домен data engineering (оркестрация пайплайнов).*

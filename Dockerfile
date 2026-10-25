ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Python 3.12, не 3.14 из pyproject.toml. Официальные образы Airflow не собираются
# под 3.14, а `uv sync` здесь запускать нельзя: pyproject.toml:6 требует
# requires-python >=3.14, и 3.12-окружение он отклонит. Поэтому Airflow и
# провайдеры ставятся pip-ом с constraints релиза. Локальный dev через uv на 3.14
# остаётся без изменений; расхождение касается только контейнера.
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

# psycopg2-binary: нужен scripts/generate_sales_data.py (импорт psycopg2),
# чтобы демо-данные можно было засеять командой `docker compose run`.
USER airflow
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    apache-airflow-providers-postgres \
    apache-airflow-providers-google \
    psycopg2-binary

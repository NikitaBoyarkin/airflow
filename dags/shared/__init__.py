"""Shared-модули платформы Airflow (REQ-001).

Импортируются DAG-ами как `from shared.config import DAGConfig`.
Airflow добавляет папку dags/ в sys.path, поэтому `shared` доступен
из любого DAG без копирования кода.
"""

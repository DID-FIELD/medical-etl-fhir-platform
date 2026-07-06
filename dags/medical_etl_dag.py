import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.pipeline import run_full_etl  # noqa: E402

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "depends_on_past": False,
}

with DAG(
    dag_id="medical_etl_full_pipeline",
    default_args=default_args,
    description="Pandas/PostgreSQL medical ETL baseline with quality checks and FHIR export.",
    start_date=datetime(2026, 7, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["medical", "etl", "fhir"],
) as dag:
    PythonOperator(task_id="run_full_etl_pipeline", python_callable=run_full_etl)

"""Manual existing-snapshot validation. No generation or production publication.

Airflow 2.11 on Linux: configure SYNTHEA_SNAPSHOT, SYNTHEA_VALIDATION_OUTPUT, SYNTHEA_WORKER_PYTHON.
Mount the existing snapshot read-only. Task XCom contains only manifest paths.
"""
import os
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator


def validate_source():
    from src.snapshot_workflow import verify_snapshot
    return verify_snapshot(os.environ['SYNTHEA_SNAPSHOT'])


def validate_component(component, **context):
    from src.airflow_tasks import execute_component
    return execute_component(component, context['run_id'], context['ti'].try_number)


with DAG(dag_id='synthea_snapshot_validation', schedule=None, catchup=False,
         start_date=datetime(2026, 9, 21, tzinfo=timezone.utc), max_active_runs=1,
         default_args={'retries': 1, 'retry_delay': timedelta(minutes=2)},
         tags=['synthea', 'snapshot', 'validation']) as dag:
    source = PythonOperator(task_id='verify_snapshot', python_callable=validate_source)
    fhir = PythonOperator(task_id='export_fhir', python_callable=validate_component,
                          op_kwargs={'component': 'fhir'})
    spark = PythonOperator(task_id='compare_spark', python_callable=validate_component,
                           op_kwargs={'component': 'spark'})
    source >> fhir >> spark

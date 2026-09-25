"""Manual full CSV Spark ETL with baseline comparison and retry reuse."""
from datetime import datetime, timedelta, timezone
import os
from airflow import DAG
from airflow.operators.python import PythonOperator


def validate_source():
    from src.spark_full_workflow import verify_inputs
    archive_hash, baseline_hash = verify_inputs(os.environ['SYNTHEA_FULL_ARCHIVE'],
        os.environ['SYNTHEA_FULL_BASELINE'], os.environ.get('SYNTHEA_BIRTH_DATE_OFFSET', '+08:00'))
    return dict(archive_sha256=archive_hash, baseline_manifest_sha256=baseline_hash)


def execute_etl(**context):
    from src.airflow_tasks import execute_full_spark
    return execute_full_spark(context['run_id'], context['ti'].try_number)


def consume(component, **context):
    from src.airflow_tasks import execute_consumer
    manifest = context['ti'].xcom_pull(task_ids='full_spark_etl')
    if not manifest:
        raise ValueError('Missing upstream Spark manifest')
    return execute_consumer(component, manifest, context['run_id'], context['ti'].try_number)


with DAG('synthea_full_spark', schedule=None, catchup=False,
         start_date=datetime(2026, 9, 21, tzinfo=timezone.utc), max_active_runs=1,
         default_args={'retries': 1, 'retry_delay': timedelta(minutes=2)},
         tags=['synthea', 'spark-full', 'validation']) as dag:
    source = PythonOperator(task_id='verify_inputs', python_callable=validate_source)
    etl = PythonOperator(task_id='full_spark_etl', python_callable=execute_etl)
    fhir = PythonOperator(task_id='export_fhir', python_callable=consume, op_kwargs={'component':'fhir'})
    database = PythonOperator(task_id='load_database', python_callable=consume, op_kwargs={'component':'database'})
    source >> etl >> [fhir, database]

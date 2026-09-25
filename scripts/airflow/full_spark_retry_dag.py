"""Acceptance-only failure after a full Spark result has been verified."""
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.spark_full_workflow import attempt_directory, verify_inputs
from src.stream_io import file_hash


def validate_source():
    return verify_inputs(os.environ['SYNTHEA_FULL_ARCHIVE'], os.environ['SYNTHEA_FULL_BASELINE'],
                         os.environ.get('SYNTHEA_BIRTH_DATE_OFFSET', '+08:00'))


def run_then_fail_once(**context):
    from src.airflow_tasks import execute_full_spark
    number = context['ti'].try_number
    result = execute_full_spark(context['run_id'], number)
    directory = attempt_directory(os.environ['SYNTHEA_FULL_OUTPUT'], context['run_id'], number).parent
    receipt = dict(try_number=number, manifest=result, manifest_sha256=file_hash(result))
    with (directory / f'airflow-try-{number}.json').open('x', encoding='utf-8') as stream:
        json.dump(receipt, stream, indent=2)
    if number == 1:
        raise RuntimeError('Acceptance-only injected failure after full Spark success')
    first = json.loads((directory / 'airflow-try-1.json').read_text())
    if first['manifest'] != result or first['manifest_sha256'] != receipt['manifest_sha256']:
        raise ValueError('Retry did not reuse the original full Spark result')
    return result


with DAG('synthea_full_spark_retry_acceptance', schedule=None, catchup=False,
         start_date=datetime(2026, 9, 21, tzinfo=timezone.utc), max_active_runs=1,
         default_args={'retries': 1, 'retry_delay': timedelta(seconds=5)},
         tags=['acceptance-only']) as dag:
    source = PythonOperator(task_id='verify_inputs', python_callable=validate_source)
    etl = PythonOperator(task_id='full_spark_etl', python_callable=run_then_fail_once)
    source >> etl

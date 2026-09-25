"""Acceptance-only DAG, staged into an isolated AIRFLOW_HOME by the runner."""
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.snapshot_workflow import attempt_directory
from src.stream_io import file_hash

def validate_source():
    from src.snapshot_workflow import verify_snapshot
    return verify_snapshot(os.environ['SYNTHEA_SNAPSHOT'])


def validate_component(component, **context):
    from src.airflow_tasks import execute_component
    return execute_component(component, context['run_id'], context['ti'].try_number)


def export_then_fail_once(**context):
    result = validate_component('fhir', **context)
    number = context['ti'].try_number
    component_dir = attempt_directory(os.environ['SYNTHEA_VALIDATION_OUTPUT'], 'fhir',
                                      context['run_id'], number).parent
    receipt = dict(try_number=number, manifest=result, manifest_sha256=file_hash(result))
    with (component_dir / f'airflow-try-{number}.json').open('x', encoding='utf-8') as stream:
        json.dump(receipt, stream, indent=2)
    if number == 1:
        raise RuntimeError('Acceptance-only injected failure after successful export')
    first = json.loads((component_dir / 'airflow-try-1.json').read_text())
    if receipt['manifest'] != first['manifest'] or receipt['manifest_sha256'] != first['manifest_sha256']:
        raise ValueError('Retry did not reuse the verified successful export')
    return result


with DAG('synthea_retry_acceptance', schedule=None, catchup=False,
         start_date=datetime(2026, 9, 21, tzinfo=timezone.utc), max_active_runs=1,
         default_args={'retries': 1, 'retry_delay': timedelta(seconds=5)},
         tags=['acceptance-only']) as dag:
    source = PythonOperator(task_id='verify_snapshot', python_callable=validate_source)
    fhir = PythonOperator(task_id='export_fhir', python_callable=export_then_fail_once)
    spark = PythonOperator(task_id='compare_spark', python_callable=validate_component,
                           op_kwargs={'component': 'spark'})
    source >> fhir >> spark



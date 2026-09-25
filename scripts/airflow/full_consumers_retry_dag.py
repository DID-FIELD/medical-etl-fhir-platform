"""Acceptance DAG: fail both consumers once after successful durable work."""
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.stream_io import file_hash


def validate_source():
    from src.spark_full_workflow import verify_inputs
    return verify_inputs(os.environ['SYNTHEA_FULL_ARCHIVE'], os.environ['SYNTHEA_FULL_BASELINE'], '+08:00')


def execute_etl(**context):
    from src.airflow_tasks import execute_full_spark
    return execute_full_spark(context['run_id'], context['ti'].try_number)


def consume_then_fail_once(component, **context):
    from src.airflow_tasks import execute_consumer
    from src.consumer_workflow import attempt_directory
    number = context['ti'].try_number
    manifest = context['ti'].xcom_pull(task_ids='full_spark_etl')
    result = execute_consumer(component, manifest, context['run_id'], number)
    directory = attempt_directory(os.environ['SYNTHEA_FULL_OUTPUT'], context['run_id'], component, number).parent
    receipt = dict(try_number=number, manifest=result, manifest_sha256=file_hash(result))
    with (directory / f'airflow-try-{number}.json').open('x', encoding='utf-8') as stream:
        json.dump(receipt,stream,indent=2)
    if number == 1:
        raise RuntimeError('Acceptance-only failure after durable consumer success')
    first = json.loads((directory / 'airflow-try-1.json').read_text())
    if first['manifest'] != result or first['manifest_sha256'] != receipt['manifest_sha256']:
        raise ValueError('Consumer retry did not reuse the successful receipt')
    return result


with DAG('synthea_full_consumers_retry_acceptance', schedule=None, catchup=False,
         start_date=datetime(2026,9,22,tzinfo=timezone.utc),max_active_runs=1,
         default_args={'retries':1,'retry_delay':timedelta(seconds=5)},tags=['acceptance-only']) as dag:
    source=PythonOperator(task_id='verify_inputs',python_callable=validate_source)
    etl=PythonOperator(task_id='full_spark_etl',python_callable=execute_etl)
    fhir=PythonOperator(task_id='export_fhir',python_callable=consume_then_fail_once,op_kwargs={'component':'fhir'})
    database=PythonOperator(task_id='load_database',python_callable=consume_then_fail_once,op_kwargs={'component':'database'})
    source >> etl >> [fhir,database]

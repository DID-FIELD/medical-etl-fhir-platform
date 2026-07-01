import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.pipeline import (
    step_init_tables,
    step_data_extract,
    step_data_clean_and_anonymize,
    step_ods_layer_load,
    step_dwd_and_fhir_output
)

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "depends_on_past": False
}

with DAG(
    dag_id="medical_etl_layered_pipeline",
    default_args=default_args,
    description="医疗数仓分层ETL调度：按ODS→DWD→FHIR分层编排任务，支持失败重试与依赖管控",
    start_date=datetime(2026, 7, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["medical", "warehouse", "airflow", "layered"]
) as dag:

    # 任务节点定义
    t_init = PythonOperator(
        task_id="init_database_tables",
        python_callable=step_init_tables
    )

    t_extract = PythonOperator(
        task_id="multi_source_data_extract",
        python_callable=step_data_extract
    )

    t_clean = PythonOperator(
        task_id="data_clean_and_anonymize",
        python_callable=step_data_clean_and_anonymize
    )

    t_load_ods = PythonOperator(
        task_id="ods_layer_data_load",
        python_callable=step_ods_layer_load
    )

    t_load_dwd_fhir = PythonOperator(
        task_id="dwd_layer_and_fhir_output",
        python_callable=step_dwd_and_fhir_output
    )

    # 定义上下游依赖关系（串行分层流转，上层完成才能进入下层）
    t_init >> t_extract >> t_clean >> t_load_ods >> t_load_dwd_fhir
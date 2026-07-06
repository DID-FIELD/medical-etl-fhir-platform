import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.pipeline import (  # noqa: E402
    step_data_clean_and_anonymize,
    step_data_extract,
    step_dwd_and_fhir_output,
    step_init_tables,
    step_ods_layer_load,
    step_spark_warehouse_build,
)

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "depends_on_past": False,
}

with DAG(
    dag_id="medical_layered_warehouse_pipeline",
    default_args=default_args,
    description="Layered medical ETL with quality checks, Spark warehouse build and FHIR output.",
    start_date=datetime(2026, 7, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["medical", "spark", "warehouse", "fhir"],
) as dag:
    init_tables = PythonOperator(task_id="init_postgres_tables", python_callable=step_init_tables)

    with TaskGroup("pandas_etl") as pandas_etl:
        extract = PythonOperator(task_id="extract_raw_sources", python_callable=step_data_extract)
        clean_quality = PythonOperator(
            task_id="clean_anonymize_and_quality_check",
            python_callable=step_data_clean_and_anonymize,
        )
        load_ods = PythonOperator(task_id="load_ods_layer", python_callable=step_ods_layer_load)
        load_dwd_fhir = PythonOperator(
            task_id="load_dim_dwd_and_export_fhir",
            python_callable=step_dwd_and_fhir_output,
        )

        extract >> clean_quality >> load_ods >> load_dwd_fhir

    spark_warehouse = PythonOperator(
        task_id="build_spark_parquet_warehouse",
        python_callable=step_spark_warehouse_build,
    )

    init_tables >> pandas_etl >> spark_warehouse

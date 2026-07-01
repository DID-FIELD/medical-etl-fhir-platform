import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# 注入项目根路径，解决模块导入问题（Airflow 运行时自动生效）
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.pipeline import run_full_etl

# DAG 全局默认参数
default_args = {
    "owner": "data_engineer",
    "retries": 2,               # 任务失败自动重试 2 次
    "retry_delay": timedelta(minutes=2),  # 每次重试间隔 2 分钟
    "email_on_failure": False,
    "depends_on_past": False
}

# 定义 DAG 工作流
with DAG(
    dag_id="medical_etl_full_pipeline",
    default_args=default_args,
    description="医疗多源数据ETL全流程：数据接入 → 清洗脱敏 → 数仓分层入库 → FHIR标准转换",
    start_date=datetime(2026, 7, 1),
    schedule_interval="0 2 * * *",  # Cron 表达式：每天凌晨 2 点定时执行
    catchup=False,                  # 关闭历史补跑，避免重复执行
    tags=["medical", "etl", "dwd", "fhir"]
) as dag:

    # 全量 ETL 执行任务
    run_full_etl_task = PythonOperator(
        task_id="run_full_etl_pipeline",
        python_callable=run_full_etl
    )

    run_full_etl_task
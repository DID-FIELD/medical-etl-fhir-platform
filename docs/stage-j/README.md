# Stage J — 完整 Spark Airflow 编排

> 后续更新：Stage L 已完成千人完整 Spark DAG 的 FHIR/数据库下游接入和重试验收，见 [报告](../stage-l/README.md)。下文保留本阶段当时的范围。

2026-09-21。新链路为现有 CSV 归档 → 完整 Spark 质量治理/九表 → Python 基准 14 组对账。只读使用既有千人输入，不生成数据，不发布 current，不访问数据库。

## 实现与验证

- `src/spark_full_workflow.py`：run_id 哈希目录、try_number 隔离；FAILED 留档；成功结果核验来源/口径/代码/26 项检查/14 组对账/递归文件清单与哈希后复用。额外文件、缺失文件、缺失数据集和损坏结果拒绝复用。
- `src/airflow_tasks.py`：用既有 worker Python 子进程执行；XCom 只返回合法 manifest 路径，Airflow venv 不导入 Spark。
- `dags/synthea_full_spark.py`：手动触发 verify_inputs → full_spark_etl，max_active_runs=1。
- `scripts/airflow/full_spark_acceptance.py`：独立 AIRFLOW_HOME、SQLite 元数据库；依次验收 DagBag、dag.test、SequentialExecutor scheduler。
- `scripts/airflow/full_spark_retry_dag.py`：仅验收使用，首次 ETL 成功后注入任务失败；第二次须返回相同路径、相同 manifest 哈希且无 attempt-0002 产物。
- [Windows 专项](tests-windows-r1.txt)：21 passed / 75.92 秒，含 14 项新工作流、7 项旧工作流。新复用单测使用真实 manifest 元数据与小型替代文件验证哈希契约，不能代替实际 Spark 运行。
- 真实 Airflow 千人会话 `full-spark-airflow-p1000-r1`：SUCCESS；DagBag、dag.test、真实 SequentialExecutor scheduler 全部通过，verify_inputs try=1、full_spark_etl try=2。首次产物成功后注入任务失败，第二次返回同一 manifest 路径与哈希，无 attempt-0002 产物。正式文件指针保持 stage-c-verified。见 [验收报告](full-spark-airflow-p1000-r1/acceptance.json)。
- 千人结果：1,159 患者、70,229 就诊、2,239 检查/序列、89,354 实例；14 组双向多重集零差异、26 项内部检查通过。dag.test 和 scheduler 各运行一次完整 ETL；scheduler 重试只核验复用。未做本轮 RSS 采样，不复用 Stage I 的时间/内存作为本轮测量。

## 运行环境

使用 MedicalETL-Airflow WSL、Airflow 2.11.2、worker PySpark 4.0.1、JDK 21、local[2]。恢复项目只读 bind mount 后，在同一 WSL 会话执行：

```bash
/opt/medical-etl-airflow/runtime/airflow-venv/bin/python \
  /opt/medical-etl-airflow/project/scripts/airflow/full_spark_acceptance.py \
  --session NEW-UNUSED-SESSION
```

真实 DAG 需要环境变量：SYNTHEA_WORKER_PYTHON、SYNTHEA_FULL_ARCHIVE、SYNTHEA_FULL_BASELINE、SYNTHEA_FULL_OUTPUT；SYNTHEA_BIRTH_DATE_OFFSET 默认为 +08:00。worker 必须能从 PYTHONPATH 导入项目，PYSPARK_PYTHON 指向 worker，SPARK_LOCAL_IP=127.0.0.1。所有新会话不可覆盖，失败保留。

## 验收范围

本次使用千人归档；万人完整 Spark CLI 已在 [Stage I](../stage-i/README.md) 通过，但不等于万人新 DAG 已验收。旧 [Stage H](../stage-h/README.md) 的万人 Airflow 编排的是已存在快照、FHIR 和三张汇总表 Spark。

Stage J 验收当时未接 PostgreSQL/FHIR；后续 [Stage K](../stage-k/README.md) 已完成千人消费适配，但尚未把消费者追加到本 DAG。不同 DAG 或并发手动 worker 竞争同一结果目录尚无锁协议；当前验收是 SequentialExecutor 串行执行。不会把 Spark 约 2.52 GiB 的万人采样 RSS 混入 Python 500 MiB 指标。

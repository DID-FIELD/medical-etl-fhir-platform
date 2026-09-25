# FHIR 万人分块导出、Spark 同口径和 Airflow 推进

更新：2026-09-21。复用 `output/scale-stream-final/runs/p10000-stream-local-r1`；未重新生成或下载万人数据。正式文件及 PostgreSQL 发布仍为 `stage-c-verified`，见 [发布及任务复用核验](workflow-verification.json)。全部原有未提交修改保留，未提交 Git。

## FHIR 已完成
- SQLite 磁盘索引替代全量五表/资源缓存；逐条 R4B 模型验证、NDJSON 写出和逐条回读，分块校验源文件及产物哈希。检查主键唯一、引用存在及就诊患者一致。正常异常关闭 SQLite 并清理暂存；外部强杀仍可能留下暂存。
- 千人 UTC 快照与旧 `build_resources` 的三类资源逐条完全一致，见 [等价证据](p1000-equivalence-r1.json)。旧内存映射函数仅保留作小规模参考；默认导出入口已切换到流式实现。
- 万人输出 Patient 11,476、Encounter 677,836、ImagingStudy 19,435，总计 708,747。第二次运行 253.326 秒，进程树采样峰值 74.07 MiB，500 MiB 保护下退出 0；见 [完整性能及验收证据](p10000-fhir-r2.json)。
- 两轮三个 NDJSON 的 SHA-256 全部相同。首轮导出成功，但控制器保存测量时发生相对路径错误，性能数据丢失；[失败记录](p10000-fhir-r1-controller-failure.json)保留。已修复为绝对路径，用新目录复跑；不能将首轮称为完整性能验收。
- 输出 `output/fhir/p10000-stream-r2`。只是 FHIR R4B 摘要文件导出，缺可信 StudyInstanceUID，未补造 series/identifier；未运行官方 HL7 Validator、术语服务或 Bulk Data 服务。

## Spark 已完成的范围
- 新入口 `src.spark.synthea_compare` 从同一快照已验证的 DIM/DWD/桥表 Parquet 重算三张 DWS/ADS，使用双向 `exceptAll` 检查所有行及重复数，不仅比较总数。
- 千人三表零差异，见 [证据](p1000-spark-r3.json)。万人患者汇总 11,476 行、日×模态 14,682 行、患者画像 11,476 行均零缺失、零多余，见 [万人证据](p10000-spark-r1.json)。
- 万人 25.843 秒、采样进程树峰值 929.94 MiB。Spark JVM 内存单独计量，不属于 FHIR 或文件 ETL 的 500 MiB 验收。
- PySpark 4.0.1、Py4J 0.10.9.9、JDK 21；依赖装入 F 盘 `.venv`，JDK 在 `output/runtime/jdk21`。原 requirements.txt 中 Spark 3.5.1 不代表本次环境；使用本目录 `requirements-spark.txt`。
- 初次 Java 25 不兼容、第二次遗漏 Parquet `run_id`，失败 manifest 均保留；修复后的千人/万人成功。未更改已有示例 Spark 作业。
- 范围是 DIM/DWD→DWS/ADS 聚合对照，尚不是 Spark 全量 CSV 清洗/隔离/去重与九表 ETL 替代实现。

## Airflow 已推进及剩余验收
- 新 DAG：`dags/synthea_snapshot_validation.py`，手动触发、`catchup=False`、`max_active_runs=1`，依次核验快照→导出 FHIR→Spark 对照。没有生成数据或正式发布步骤。
- `src.snapshot_workflow` 对完整源清单核验哈希，已成功结果仅在绑定同一源 manifest 且产物哈希一致时复用；失败/不完整/不同来源输出拒绝覆盖，需使用新输出目录。XCom 仅返回 run_id 或 manifest 路径。
- 已运行真实万人成功结果的任务函数复用，已测试重复执行、产物篡改拒绝、失败目录保护；DAG 通过 Python 编译检查。
- 当前 Windows 无可用 WSL/Linux 容器；未安装或启动 Airflow，未声称 DagBag、调度器、任务实例重试或 DAG 集成验收成功。[Airflow 官方前置条件](https://airflow.apache.org/docs/apache-airflow/2.11.2/installation/prerequisites.html)要求 Windows 使用 WSL2/Linux 容器。
- 下一步在 Linux 环境使用 Airflow 2.11.2/Python 3.12、JDK 21、相同核心依赖及本次 Spark 依赖，安装项目模块并挂载现有快照为只读。Windows `.venv` 不可直接复用到 Linux。设置 `SYNTHEA_SNAPSHOT` 为该快照、`SYNTHEA_VALIDATION_OUTPUT` 为全新的独立目录，先做 DagBag 导入检查，再运行 `airflow dags test synthea_snapshot_validation 2026-09-21`，最后验证调度器及重试。环境准备属于尚未完成工作。

## 回归及边界
- [数据库隔离回归](test-results-database-r2.txt)：79 passed、2 skipped（Spark 专项另跑）、2 条既有弃用警告。
- [真实 Spark 测试](spark-test-results.txt)：2 passed；覆盖有/无影像、零检查患者及篡改汇总后失败。合计 81 项通过，分两次执行。
- 首次数据库回归因 55432 服务未监听失败，完整失败输出保留；启动项目原有 PostgreSQL 后隔离回归通过。没有重新加载万人数据库。此处连接拒绝原因明确，与先前未定位的提交后 OperationalError 不能混为一谈。
- 采样间隔 250ms，进程树 RSS 可能重复计算共享页并漏过短峰值；不是 OS 硬限额。输出、便携运行时、日志和数据库配置均仍在 Git 忽略目录，需独立备份。

## F 盘复现入口
```powershell
# 必须使用新的输出和证据名称；现有结果不会覆盖
.\.venv\Scripts\python.exe -m scripts.benchmark_fhir --snapshot output/scale-stream-final/runs/p10000-stream-local-r1 --output output/fhir/NEW-RUN --evidence docs/stage-g/NEW-RUN.json
$env:JAVA_HOME=(Get-ChildItem output/runtime/jdk21 -Directory | Select-Object -First 1).FullName
$env:SPARK_LOCAL_IP='127.0.0.1'
$env:PYSPARK_PYTHON=(Resolve-Path .venv/Scripts/python.exe).Path
.\.venv\Scripts\python.exe -m src.spark.synthea_compare --snapshot output/scale-stream-final/runs/p10000-stream-local-r1 --output output/spark/NEW-RUN
```

# 当前交接：完整 Spark → FHIR / PostgreSQL 千人调度与重试已通过

> **最新 Stage L（2026-09-22）：** 完整 DAG 已扩展为 verify_inputs → full_spark_etl → [export_fhir, load_database]，千人 DagBag、dag.test、真实 scheduler 全部通过。两个下游成功后各注入失败，try=2 复用同一 manifest/hash；数据库重试实时核对 11 组完整内容。FHIR 73,627 资源等价、132 次串行 API 通过。正式文件/数据库仍 stage-c-verified。见 [Stage L](stage-l/README.md)、[验收报告](stage-l/full-consumers-p1000-r2/acceptance.json)。下方 Stage J/K 的“尚未追加到 DAG”是历史状态，已由本轮完成。

### 当前继续入口

- 实现：src/consumer_workflow.py、src/airflow_tasks.py、dags/synthea_full_spark.py；新验收用 scripts/airflow/full_consumers_acceptance.py，旧 Stage J 两任务验收脚本不适用于当前四任务 DAG。
- Spark/FHIR 用 WSL worker，数据库调用现有 Windows worker；没有修改 PostgreSQL 网络配置。输出共享在 output/full-consumers-p1000-r2，元数据库/调度日志在 WSL 同名 runs 会话。
- schema 根据 DAG run_id 派生为 synthea_airflow_<hash>，具体两个 schema 名见验收报告。不可覆盖本轮输出或 schema；再验收需全新 session。
- 回归 r1：29 passed / 116.04 秒（新消费工作流5 + reader10 + Spark工作流14）；补充 r2：2 passed / 4.77秒；路径回归 r3：2 passed / 6.48秒。首轮 Airflow r1 因 bind mount 路径别名误判失败，修复后 r2 通过，失败证据保留。覆盖提交后回执前失败恢复、同数量内容篡改拒绝、跨run与回执/代码绑定。
- 没有新增万人四任务 DAG、内存峰值、多机集群或并发压力结论。下一步如需扩展，优先明确是否验收万人新链路；并发、多机、官方FHIR Validator/服务与独立备份仍待办。没有正在后台运行的验收调度器。


> 教材更新（2026-09-22）：[离线 HTML](html/01-overview.html) 已同步为 15 章，正文来源在 docs/html/chapters/，用 build_chapters.py 统一生成。新增完整 Spark、Airflow 重试、目录消费三章；全页链接与 1366/390 宽度检查通过。本次仅更新教学，Stage K 后的业务待办不变。

更新：2026-09-21。本轮在 Stage J 基础上，已接通完整 Spark Parquet 目录到 PostgreSQL/FHIR 消费器并完成真实千人验证。没有重跑 Spark ETL、生成数据、修改简历或正式发布。

## 最新续做：Stage K 消费链路已完成

- `src/snapshot_reader.py` 新增公共分批读取器：旧 JSON 与 Spark Parquet 目录均可读；目录文件清单/哈希校验，Parquet 每批 500 行，模型列/run_id/行数检查。ODS 使用持久化 source_row，不能按分区读取顺序重新编号。
- FHIR 沿用既有 R4B 摘要映射与 SQLite 磁盘索引；数据库共用原事务加载与 SQL 对账，新增 ODS 序号范围和发布前源文件复核。
- 实际消费 Stage I 已成功千人产物的副本：output/spark-consumers-p1000-r1/source。原 WSL 产物未变。与绑定 Python 基准分别消费，**SUCCESS**。
- FHIR 73,627 个资源（1,159 Patient / 70,229 Encounter / 2,239 ImagingStudy），双向内容差异为 0；分区顺序不同，不要求 NDJSON 字节哈希相同。
- 隔离 schema **synthea_scale_consumers_p1000_r1**：九表 + ODS + 处置账本共 11 组 SQL EXCEPT ALL 双向零差异；重复加载 ALREADY_LOADED；132 次串行 API 校验通过。
- Python/Spark 快照数据库加载分别 23.732 / 24.066 秒，包含校验；没有本轮 RSS 或万人消费性能结论。
- 测试 r1：31 项旧链路通过，新增 10 项被 fixture 缺少 warnings.json 阻断；修正 fixture 后 r2 新增 10 项全部通过（11.61 秒），含真实隔离数据库对账、源变化事务回滚。分批结果不混称为一次完整回归。
- 正式文件和 **synthea_v1.current_snapshot** 本轮均只读核对，前后都是 stage-c-verified。只发布新隔离 schema；没有改正式数据。
- [Stage K](stage-k/README.md) / [验收 JSON](stage-k/p1000-r1/acceptance.json) / [API 证据](stage-k/p1000-r1/api.json)。完整现场 output/spark-consumers-p1000-r1/acceptance，失败测试 r1 保留。报告和测试日志已归档，README/复习指南/HTML 前五章已同步。
- **还没有把这些消费者追加到完整 Spark Airflow DAG**，旧 snapshot_validation 也仍保留旧格式校验契约。下一步可做新 DAG 下游消费与重试验收，或按需要验证万人消费者规模；不要把本轮千人结果扩大为万人结论。

以下第 1 节是上一轮 Stage J 结果，当前待办以本节和第 4 节为准。

## 1. 上一轮 Stage J 结果

- WSL r5：study_metadata、series_metadata、encounter_conflict **3 passed / 42.57 秒**。完整转换参数表 15 组均有真实 Spark 执行证据，分批结果不累加为单次完整回归。见 [日志](stage-i/tests-wsl-r5.log)。
- 新工作流 `src/spark_full_workflow.py`：run_id 哈希目录、try_number 独立目录；FAILED 现场保留；成功重试核验来源、生日口径、代码哈希、26 项内部检查、14 组基准对账和完整递归产物清单/哈希。损坏 SUCCESS 明确失败，不悄悄重算。
- `src/airflow_tasks.py` 新增独立 worker adapter；`dags/synthea_full_spark.py` 手动 verify_inputs → full_spark_etl，不导入 Spark 到 Airflow venv。
- Windows 工作流回归 **21 passed / 75.92 秒**，包括新工作流 14 项与既有快照 7 项；2 条依赖弃用警告。现场 output/spark-full-workflow-tests-r1 不覆盖。新复用单测用真实 manifest 元数据和小文件测试契约，实际执行另见下面证据。
- **新 Airflow `full-spark-airflow-p1000-r1` SUCCESS**：DagBag、dag.test、真实 SequentialExecutor scheduler 均通过。verify_inputs try=1，full_spark_etl try=2；首次完整 ETL 成功后故意抛出任务异常，第二次复用相同 manifest 路径与哈希，无 attempt-0002 产物。
- 本次实际规模为千人归档：1,159 患者、70,229 就诊、2,239 检查/序列、89,354 实例；14 组双向多重集零差异、26 项检查通过。dag.test 和 scheduler 分别运行完整 ETL，scheduler 的第二次尝试只验证复用。
- **没有新增万人 DAG 验收，也没有本轮 RSS 采样**。不能把 Stage I 的时间/内存写成本轮测量。
- 证据：[Stage J](stage-j/README.md)、[验收 JSON](stage-j/full-spark-airflow-p1000-r1/acceptance.json)、[Windows 测试摘要](stage-j/tests-windows-r1.txt)。完整产物仍在 WSL，报告、manifest、重试回执及运行日志归档到 F 盘。
- README、PROJECT_REVIEW_GUIDE 和 HTML 前五章已同步当前证据。其余专项章节保留历史教学案例。正式文件 current 前后均 stage-c-verified；本轮没有操作/查询数据库发布状态，没有提交、重置或清理已有修改。

## 2. 历史规模证据和边界

| 项目 | 已验收 | 边界 |
| --- | --- | --- |
| Python streaming 万人 | 1,725,660 源行；11,476 患者、677,836 就诊、19,435 检查/序列、1,036,348 实例；392.618 秒、280.76 MiB | 500 MiB 仅对应此客户端范围 |
| PostgreSQL/API 万人 | 加载 327.112 秒；132 次串行 ASGI 接口对照通过 | 首次 OperationalError 根因未确认；无 HTTP 并发压测 |
| FHIR 万人 | 708,747 R4B 摘要资源；253.326 秒、74.07 MiB | 非官方 Validator / FHIR Server / Bulk Data 验收 |
| 完整 Spark 千人/万人 | 14 组双向零差异；万人 26 项检查通过，283.201 秒、2.52 GiB | WSL/JDK 21 local[2]；时间包含回读/对账；非 500 MiB 或多机集群 |
| 旧 Airflow Stage H r3 | 万人旧快照校验 → FHIR → 三表 Spark 汇总，重试通过 | 不包含 CSV 清洗 |
| 新 Airflow Stage J r1 | 千人 CSV → 完整 Spark 九表与基准对账，真实调度重试通过 | Stage K 已接通独立消费者，尚未追加到 DAG |

权威规模证据见 [Stage I](stage-i/README.md)。此前详细治理规则与运行记录归档为 [Stage J 之前的交接](history/HANDOFF_SPARK_FULL_ETL_before_stage_j.md)，其中“未补测试/未做 Airflow”已过时。Python 口径的权威代码仍是 src/synthea_pipeline.py 和 src/streaming_pipeline.py；本轮未改 Spark 五个核心模块。

## 3. 环境、路径和保护要求

- **只在 F:\project\medical-etl-fhir-platform 工作**，每条命令显式指定 F 盘 workdir；默认 D 盘不存在，会报 os error 267。Windows 使用 `.venv\Scripts\python.exe`。
- 保留所有已有 modified/untracked；不 reset/checkout/clean，不擅自提交，不修改 resume.tex 或简历材料。
- 复用现有 ZIP，不重新生成/下载 Synthea；新输出必须独立 run_id/目录。pytest 用全新 --basetemp，失败现场保留。
- 正式文件 `output/synthea/current.json` 为 stage-c-verified；正式数据库 schema synthea_v1。不得发布或切换正式批次；允许新建隔离 schema 验收，连接凭据不得打印。
- WSL 发行版 MedicalETL-Airflow：Ubuntu 24.04.5、Python 3.12.3、Airflow 2.11.2、JDK 21、PySpark 4.0.1。无需重装。
- `/opt/medical-etl-airflow/project` 是 `/mnt/f/project/medical-etl-fhir-platform` 的只读 bind mount；WSL 重启可能丢失。恢复挂载、只读核对和执行放同一 WSL 会话。
- WSL Python：`/opt/medical-etl-airflow/runtime/worker-venv/bin/python`；Airflow Python 在相邻 airflow-venv/bin/python。项目输出写 `/opt/medical-etl-airflow/runs/`，不能写只读 project。
- 完整 Spark 基准：`output/scale-stream-final/runs/p1000-stream-local-r1`、`p10000-stream-local-r1`；ZIP 对应 data/generated/scale-v4-p1000/source.zip 与 scale-v4-p10000/source.zip；生日偏移 +08:00，事件 UTC。
- 既有完整规模作业 `/opt/medical-etl-airflow/runs/spark-full-p1000-r1`、spark-full-p10000-r1 已成功，不为确认状态再跑。
- 新 Airflow现场 `/opt/medical-etl-airflow/runs/full-spark-airflow-p1000-r1`，含独立元数据库、日志和两个 run_id 的产物；已停止验收 scheduler。不能覆盖此会话或旧 Stage H r3。
- r5 边界测试现场 `/opt/medical-etl-airflow/runs/spark-full-tests-r5`、同名 .log。Windows/WSL 早期失败记录继续保留。

## 4. 后续工作选择

Stage J 编排与 Stage K 千人消费适配已完成。下一步可按用户方向推进：

1. 将已验证的 Spark Parquet 消费器追加到完整 Spark Airflow DAG，设计独立输出/隔离 schema 与下游重试复用，先验收千人，再按必要性验证万人。
2. 如果要求新 DAG 的万人调度证据，可复用既有万人 ZIP 和基准，在全新 session 单独验收；当前没有这项结论。
3. 并发 worker 竞争同一尝试目录的锁协议、HTTP 并发性能、官方 FHIR Validator/术语服务与增量 CDC 仍未验证。
4. 独立备份位置和完整性待审计；output/final-delivery-20260921 是同盘汇集，不能称作异盘独立灾备。

项目讲解从 [PROJECT_REVIEW_GUIDE](../PROJECT_REVIEW_GUIDE.md) 与 [HTML 总览](html/01-overview.html) 继续，不重复新建综合说明。新 Airflow 部署环境变量和运行命令见 Stage J。

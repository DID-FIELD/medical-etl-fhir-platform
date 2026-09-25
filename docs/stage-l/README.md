# Stage L — 完整 Spark DAG 接入 FHIR 与隔离 PostgreSQL

2026-09-22。新链路 `verify_inputs → full_spark_etl → [export_fhir, load_database]` 已实现，千人真实 Airflow 验收已全部通过（full-consumers-p1000-r2）。不是万人或多机集群验收。

## 输入与运行隔离

下游从同一 DAG run 的 full_spark_etl XCom 获取 manifest 路径，不读取全局 current。adapter 检查路径位于本 run 的 Spark 尝试目录，复核来源、生日口径、代码、26 项内部检查与14组对账。消费者再次检查源 run_id、目录清单与文件哈希。

FHIR 与数据库消费者按 run_id/component/try_number 分目录。数据库 schema 固定从 run_id 哈希派生为 `synthea_airflow_<32位哈希>`，不能传正式 schema。两次验收运行分别使用不同 schema；不会切换 synthea_v1。

## 恢复规则

- FHIR SUCCESS 重试核验输入绑定、消费者代码、完整产物哈希、资源数量与导出清单，复用同一路径。
- 数据库 SUCCESS 重试除回执外，还用只读 REPEATABLE READ 事务，对九表、ODS、处置账本逐行核对源快照。SQLite 磁盘索引保留重复次数，每批游标读取；同数量但内容被修改也拒绝复用。
- 若数据库已提交但 worker 在写成功回执前失败，下一次使用同一 schema/run_id 加载，返回 ALREADY_LOADED，再做内容核验。不会依赖“上次任务失败”推断数据库回滚。
- FAILED 和不完整尝试保留，新尝试独立；损坏 SUCCESS 明确报错，不隐式修复或覆盖。
- FHIR 与数据库是两个独立下游任务，不是跨系统事务；其中一个失败不表示另一个回滚。

## 运行环境

本机采用混合 worker：Spark/FHIR 在 WSL worker venv，数据库通过 WSL interop 调用现有 Windows .venv。数据库仍只监听 Windows localhost，没有调整监听地址、认证或防火墙。共享输出位于 F 盘 output，全新 session；项目源码从既有只读 bind mount 读取。

通用环境变量沿用 SYNTHEA_FULL_ARCHIVE / BASELINE / OUTPUT、SYNTHEA_WORKER_PYTHON。数据库默认可用同一个原生 worker；本机另配置 SYNTHEA_DATABASE_WORKER_PYTHON、SYNTHEA_DB_WINDOWS_WORKER=1、SYNTHEA_WINDOWS_PROJECT，边界用 wslpath 转换路径。XCom 只传 manifest 路径；不传凭据。

## 验证

- `tests/test_consumer_workflow.py`：FHIR 复用及损坏拒绝、run 绑定、失败现场、数据库内容篡改、提交后回执前失败恢复。
- r1：新消费者 5 项 + 读取器 10 项 + Spark 工作流 14 项，**29 passed / 116.04 秒**，真实数据库测试使用隔离 schema。
- r2：新增跨 run 输入与回执/代码绑定测试，**2 passed / 4.77 秒**。两个批次分别记录，均有两条既有依赖弃用警告。
- 完整验收脚本 `scripts/airflow/full_consumers_acceptance.py` 使用新元数据库，依次 DagBag、dag.test、SequentialExecutor scheduler。仅验收 DAG 在 FHIR/数据库各自成功后抛异常，两个任务第二次都必须复用相同 manifest/hash、没有第二份消费者产物。
- 同时验证数据库 11 组内容对账、FHIR 与 Stage K 参考资源等价、132 次串行 API、正式文件和数据库指针不变。结果已归档，见 [完整验收报告](full-consumers-p1000-r2/acceptance.json)。

## 文件与后续范围

生产 DAG：`dags/synthea_full_spark.py`；worker 协调：`src/airflow_tasks.py`；消费恢复：`src/consumer_workflow.py`；验收 DAG：`scripts/airflow/full_consumers_retry_dag.py`。

旧 Stage J 报告对应当时的两任务 DAG；当前完整 DAG 已扩展为四任务，应使用本阶段验收脚本，不再用旧 full_spark_acceptance.py 的两任务断言重验当前 DAG。Stage H 旧快照 DAG 保留。

尚未涵盖：万人新下游调度、并发 worker 竞争、跨系统原子提交、多机集群、HTTP 并发性能、官方 FHIR Validator、独立备份完整性。没有新增 RSS 测量。

## 首轮失败与修复（保留证据）

full-consumers-p1000-r1 的 Spark、FHIR、数据库产物成功，但生产 dag.test 的 load_database 任务在返回回执时失败，整体验收 FAILED，未进入 scheduler。只读 bind mount 存在时，wslpath 将 F 盘回执转换为 /opt/medical-etl-airflow/project/...；预期产物路径为 /mnt/f/project/...。两者指向同一文件，原字符串路径比较误判。

适配器现使用 samefile 核对返回文件是否就是本 run/尝试允许的 manifest，再返回配置中的产物路径；不按同名或相同内容放行其他文件。路径别名/错误文件/缺失文件回归与跨 run 拒绝检查通过：r3 为 2 passed、6 deselected，6.48 秒。修复后 full-consumers-p1000-r2 独立验收已通过，见下方最终结果。

失败报告、产物回执和日志见 [r1 证据](full-consumers-p1000-r1/acceptance.json)。失败现场、独立 schema 均保留，不把产物成功当作调度成功。

## 最终验收结果

- DagBag、生产四任务 DAG 的 dag.test、真实 SequentialExecutor scheduler 全部 SUCCESS。
- scheduler：verify_inputs / full_spark_etl 均 try=1；export_fhir / load_database 均 try=2。
- 两个下游首次成功后注入异常，第二次均返回原 manifest 路径和哈希，没有 attempt-0002 产物。数据库重试执行实时只读内容核验，没有重写数据。
- 完整 Spark 千人九表/ODS/账本/warnings 14 组对账与 26 项内部检查通过；数据库 11 组内容零差异。
- FHIR 1,159 Patient、70,229 Encounter、2,239 ImagingStudy，与已验证参考资源内容一致，共 73,627 个资源。
- 新隔离 schema 上 132 次串行 ASGI API 校验通过，正式文件与 synthea_v1 数据库批次前后仍 stage-c-verified。
- dag.test 与 scheduler 分别执行一轮 ETL/消费，使用独立 schema；此处未进行万人新链路或 RSS 测量。

Windows 共享现场：output/full-consumers-p1000-r2；WSL 调度日志/元数据库：/opt/medical-etl-airflow/runs/full-consumers-p1000-r2。旧失败现场和 Stage J/K 证据均保留。

## 教学与归档复核

15 章 HTML 已同步 Stage L（源码在 docs/html/chapters，统一生成）。383 个本地链接/锚点有效，重复构建字节一致；Edge 1366/390 宽度的 30 个页面视图无整页横向溢出。抽查第 14 章手机版及第 15 章桌面版。见 [教材 QA](../html/qa-stage-l-20260922.json)。

归档后独立复核 SUCCESS、四任务状态与尝试次数、两个消费者回执哈希、11 组数据库零差异、73,627 个 FHIR 资源、132 次 API 和正式批次不变；验收报告中记录的代码哈希与当前代码一致。验收进程已正常退出，scheduler 已停止。

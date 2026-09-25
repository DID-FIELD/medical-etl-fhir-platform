# 新任务交接：Stage L 已完成，下一步扩展万人完整链路

更新：2026-09-22。用户本次只要求写交接，准备自己新开任务；不要自动创建新任务。本文件是当前续做入口，旧交接和记忆文件中的待办可能已过时。

## 1. 接手后先看这里

- 实际工作目录 **F:\project\medical-etl-fhir-platform**。环境可能仍显示 D:，该路径不可用；每次 shell 调用显式指定 F: workdir。
- 千人完整链路 `verify_inputs → full_spark_etl → [export_fhir, load_database]` 已完成真实 Airflow 验收，不要重新实现或仅为确认状态重跑。
- 最新权威证据：[Stage L 说明](stage-l/README.md)、[成功报告](stage-l/full-consumers-p1000-r2/acceptance.json)、[HTML 教学](html/01-overview.html)。
- 本轮验收进程已正常退出，scheduler 已停止，没有等待中的任务或审批。
- 用户要求业务推进时同步更新 HTML 教学。没有授权修改简历、切换正式批次、清理旧现场或提交现有所有改动。

## 2. 已完成与准确边界

| 阶段 | 已完成 | 不应扩大的结论 |
| --- | --- | --- |
| Python streaming | 万人 1,725,660 源行，392.618 秒、280.76 MiB | 500 MiB 结论仅对应已测客户端范围 |
| Python 数据库/API | 万人加载 327.112 秒，132 次串行 ASGI 请求通过 | 首次提交后 OperationalError 根因未确认；非 HTTP 并发 |
| Python FHIR | 万人 708,747 个摘要资源，253.326 秒、74.07 MiB | 非官方 Validator 或完整 FHIR Server |
| Stage I 完整 Spark | 千人、万人 CSV 治理及九表；14 组双向多重集零差异，26 项内部检查；万人 283.201 秒、2.52 GiB | WSL/JDK21 local[2]，含回读与对账；非 500 MiB、多机集群 |
| Stage J 完整 Spark 调度 | 千人两任务 DAG，真实 scheduler 的 ETL try=2 复用 | 当时没有下游，旧验收脚本仍断言两任务 |
| Stage K 独立消费者 | 千人 Spark Parquet → FHIR/PostgreSQL/API，资源等价、11 组 SQL 双向零差异 | 当时未编排下游；没有万人 Spark 消费结论 |
| Stage L 四任务 DAG | 千人 DagBag、生产 dag.test、真实 scheduler；双消费者故障重试、内容对账、API 全部通过 | 没有万人四任务 DAG、本轮 RSS、多机或并发结论 |

Stage I 的 15 组转换参数案例已分批实际运行完，不能把不同批次合称一次完整测试。详细历史证据分别在 stage-i、stage-j、stage-k；不要把旧 snapshot_validation DAG 的万人结果混成完整 CSV Spark DAG 的万人结果。

## 3. Stage L 最新实测

成功 session：**full-consumers-p1000-r2**。

- 生产四任务 dag.test 成功；真实 SequentialExecutor scheduler 四任务全部 success。
- verify_inputs / full_spark_etl：try=1；export_fhir / load_database：try=2。
- 两个消费者首次持久化成功后，由验收 DAG 注入异常；第二次返回相同 manifest 路径与哈希，无消费者 attempt-0002。
- FHIR：1,159 Patient + 70,229 Encounter + 2,239 ImagingStudy = **73,627**，与已验证的 Stage K 参考资源逐条内容一致。
- 数据库：九表 + source_records + row_dispositions 共 **11 组**完整内容一致。Stage L 用 SQLite 磁盘索引保留重复次数，逐行核对；不要写成它执行了 Stage K 的 SQL EXCEPT ALL。
- 隔离 schema 上 **132 次串行 ASGI API** 检查通过。
- 正式文件 `output/synthea/current.json` 与正式数据库 `synthea_v1.current_snapshot` 前后均 **stage-c-verified**。
- 本轮没有新增内存测量。dag.test 与 scheduler 分别跑一轮独立 ETL/消费；消费者重试复用既有结果。

成功现场：

- Windows：`output/full-consumers-p1000-r2/`，含共享完整数据和验收 JSON。
- WSL：`/opt/medical-etl-airflow/runs/full-consumers-p1000-r2/`，含独立 Airflow 元数据库、任务日志；主日志为同级 `full-consumers-p1000-r2.log`。
- 归档：`docs/stage-l/full-consumers-p1000-r2/`，含报告、manifest、重试回执、日志和三批测试日志，不复制整套数据。
- dag.test schema：`synthea_airflow_98c0bae56930d09f7515687fb3c6edcc`。
- scheduler schema：`synthea_airflow_0665f29b1df26e508b98b9a58a7d1cf6`。

### 首轮失败必须保留

full-consumers-p1000-r1 的 Spark/FHIR/数据库产物成功，但 Airflow 数据库任务回执路径校验失败，整体验收 FAILED，未进入 scheduler。启用只读 bind mount 后，wslpath 将 Windows 文件返回为 `/opt/medical-etl-airflow/project/...`，预期路径为 `/mnt/f/...`；同一文件的两个挂载路径被字符串比较误拒绝。

已在 `src/airflow_tasks.py` 用 `samefile` 核对文件身份，再返回配置中的允许路径。相同内容但不同文件、缺失文件仍拒绝。修复后新 r2 全流程通过。失败日志、回执、输出和隔离 schema 均保留；见 [r1 报告](stage-l/full-consumers-p1000-r1/acceptance.json)。不要把产物成功等同于 DAG 成功。

### 测试记录

- consumer-workflow-tests-r1：29 passed / 116.04 秒（消费者5、读取器10、Spark工作流14），包含真实隔离数据库。
- r2：2 passed / 4.77 秒，跨 run 输入与回执/代码绑定。
- r3：2 passed、6 deselected / 6.48 秒，路径别名及错误文件拒绝、跨 run 拒绝。
- 分批记录，不合称一次完整回归；各批有两条既有依赖弃用警告。
- 覆盖同数量内容篡改拒绝、提交成功后回执前失败恢复、成功复用、失败现场保留。
- 归档后已独立复核结果、重试回执和代码哈希；git diff --check 通过，仅提示既有 run_etl.py 换行转换。

## 4. 代码导航与契约

| 文件 | 用途 |
| --- | --- |
| src/spark/synthea_rules.py、synthea_ingest.py、synthea_etl.py、synthea_full.py | 完整 Spark CSV 清洗、九表构建与对账 |
| src/spark_full_workflow.py | 输入/代码/递归产物校验，按 run_id 哈希和尝试目录隔离、成功复用 |
| src/snapshot_reader.py | 旧 JSON 与 Spark Parquet 目录公共读取器，每批 500 行；保留 ODS source_row |
| src/consumer_workflow.py | FHIR/数据库消费恢复，派生隔离 schema，数据库实时内容核验 |
| src/airflow_tasks.py | 独立 worker 调用、同 run 上游绑定、WSL/Windows 路径适配 |
| dags/synthea_full_spark.py | 当前生产四任务手动 DAG |
| scripts/airflow/full_consumers_acceptance.py | 当前千人完整验收入口 |
| scripts/airflow/full_consumers_retry_dag.py | 仅验收使用的双下游故障注入 DAG |
| scripts/airflow/windows_acceptance_probe.py | 正式 DB 只读核对及隔离 API 验证 |
| scripts/collect_consumer_airflow_evidence.py | WSL 完成后归档，不覆盖已有目录 |
| tests/test_consumer_workflow.py | 消费与适配器回归 |

下游只接收本 DAG run 的 full_spark_etl XCom manifest，不读全局 current。数据库 schema 固定从 run_id 哈希派生。FHIR SUCCESS 重试核验内容文件哈希；数据库 SUCCESS 重试还执行只读 REPEATABLE READ 事务，完整核对 11 张表。提交后回执前失败时，下次允许 ALREADY_LOADED，再重新对账。两个下游独立恢复，不是跨系统原子事务。

旧 `scripts/airflow/full_spark_acceptance.py` 对应 Stage J 两任务版本，不能拿它直接重验当前四任务 DAG。Stage H 旧快照 DAG 保留，不混用。

## 5. 环境与操作约束

- 保留现有大量 modified/untracked；不 reset、checkout、clean，不擅自提交。不修改 `resume.tex` 或 docs/resume。
- 复用现有 Synthea ZIP，不重新生成或下载；新验收必须新 session、新输出目录、派生隔离 schema。pytest 每批用全新 --basetemp；失败目录保留。
- Windows Python：`.venv\Scripts\python.exe`。中文日志设 `PYTHONIOENCODING=utf-8`，文件读写显式 UTF-8；部分旧脚本有 BOM，可用 utf-8-sig 读取。
- PostgreSQL 在 Windows localhost:55432，数据库 medical_etl；凭据在 `output/local-postgres/connection.json`，不要输出其内容。先只读确认服务状态，不擅自改认证、监听、防火墙或正式批次。
- WSL：`C:\Program Files\WSL\wsl.exe`，发行版 MedicalETL-Airflow，root。sandbox 可能要求 require_escalated；遵循实际审批结果。
- Airflow Python：`/opt/medical-etl-airflow/runtime/airflow-venv/bin/python`，Airflow 2.11.2。
- Spark/FHIR worker：`/opt/medical-etl-airflow/runtime/worker-venv/bin/python`，PySpark 4.0.1、JDK21、local[2]；worker 没有 psycopg2，数据库通过 WSL interop 调用现有 Windows venv。
- Windows worker 在 WSL 的路径：`/mnt/f/project/medical-etl-fhir-platform/.venv/Scripts/python.exe`，cwd 必须是 `/mnt/f/project/medical-etl-fhir-platform`；移除继承的 Linux PYTHONPATH。
- 只读源码挂载 `/opt/medical-etl-airflow/project` 来自 `/mnt/f/project/medical-etl-fhir-platform`。WSL 重启可能丢失挂载；恢复、只读核验和执行必须放同一会话。共享新产物写 `/mnt/f/.../output/<新session>`，Airflow 元数据库写 `/opt/.../runs/<新session>`，不能通过只读源码挂载写输出。

恢复挂载（WSL bash）：

```bash
mountpoint -q /opt/medical-etl-airflow/project || mount --bind /mnt/f/project/medical-etl-fhir-platform /opt/medical-etl-airflow/project
mount -o remount,bind,ro /opt/medical-etl-airflow/project
```

输入：千人/万人 ZIP 分别为 `data/generated/scale-v4-p1000/source.zip`、`scale-v4-p10000/source.zip`；Python 基准在 `output/scale-stream-final/runs/p1000-stream-local-r1`、`p10000-stream-local-r1`。生日偏移 +08:00，事件时间 UTC。Stage I 已有成功完整 Spark 作业在 WSL runs/spark-full-p1000-r1、spark-full-p10000-r1；无需为确认状态重算。

千人归档 SHA256：`6775a19d38b29f363b025bf7676c2d21268b831c4afa44e92f00a17b2259ad2d`；绑定 Python manifest：`20f048ccc48e9359ae21600d0c69a34bdacb6a8ae1d0281e80eb5de5b908e5b7`。

## 6. HTML 教学交接

15 章均已同步到 Stage L，重点看 14-airflow、15-consumers。编辑 `docs/html/chapters/*.html` 和 chapters.css，然后运行：

```powershell
.\.venv\Scripts\python.exe docs/html/build_chapters.py
```

不要只改生成后的页面。教材当前 QA：[qa-stage-l-20260922.json](html/qa-stage-l-20260922.json)，383 个本地链接/锚点有效，重复构建字节一致，Edge 在 1366/390 宽度下的 30 个视图无整页溢出。抽查第14章手机、第15章桌面截图。

QA 现场 `output/html-stage-l-qa-20260922/`，含 check_links.py、check.cjs、browser.json 和截图。Node 为 `E:\node\node.exe`；Playwright 位于 `C:/Users/DID/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright`，浏览器为安装的 Edge。下一次页面修改重新检查时使用新 QA 目录保留本次证据。

## 7. 建议下一步（尚未执行）

若用户在新任务继续推进，优先做 **万人完整 Spark → FHIR/PostgreSQL 四任务 DAG 验收**。不要把这项待办写成已完成。

1. 先将当前千人验收入口参数化，允许显式人口规模、ZIP、基准、FHIR 参考和合理超时；它现在硬编码 p1000 与 Stage K 千人参考，不能仅改 session 名就称万人。
2. 复用现有万人 ZIP/基准，核对匹配的已成功万人 FHIR 参考及哈希；如需新参考导出，单独新目录，不覆盖已有现场。
3. 用全新 session、隔离 schema 验收四任务、双消费者重试、11 组完整内容、FHIR 资源等价、串行 API、正式指针不变，归档后更新 HTML。规模应为 11,476 患者、677,836 就诊、19,435 检查，FHIR 共 708,747。
4. 若要新增性能结论，单独设计测量范围与 RSS 采样；未测不能沿用旧数值。
5. 并发 worker 竞争、跨系统部分成功恢复、HTTP 并发、官方 FHIR Validator/术语服务、独立备份完整性均是后续独立任务。本次没有承诺生产并发或多机能力。

用户明确的新指令优先于上述建议。若只要求继续，不需要为读取、常规修复、隔离验收和教材同步重复索要确认；任何正式发布或破坏性操作仍不在本交接范围。

## 8. 历史入口

旧交接完整保存在 [Stage L 交接整理前快照](history/HANDOFF_SPARK_FULL_ETL_before_stage_l_handoff_20260922.md)。docs/TASK_MEMORY.md、PROGRESS.md、HANDOFF_MEMORY_500M.md 是累计记录，当前事实以本文件及 Stage L SUCCESS 证据为准。

> **2026-09-22 Stage L 已完成：** 完整 Spark DAG 已串起 FHIR 和隔离 PostgreSQL，千人真实调度与两个下游重试复用通过，73,627 个 FHIR 资源等价、数据库11组内容一致、132次串行API通过。正式发布不变。见 [Stage L](stage-l/README.md)；下方旧待办为历史记录。

> **2026-09-21 最新 Stage K：** Spark 目录产物已接通 PostgreSQL/FHIR；千人 73,627 个 FHIR 资源等价、数据库 11 组双向零差异、132 次串行 API 校验通过，正式文件/数据库发布仍 stage-c-verified。待将下游消费加入完整 Spark DAG，万人消费者规模未验收。见 [当前交接](HANDOFF_SPARK_FULL_ETL.md)、[Stage K](stage-k/README.md)。下文为历史记录。

> **2026-09-21 本轮已完成：** 3 个 Spark 边界用例通过，独立完整 Spark Airflow 千人 DagBag、dag.test、真实 scheduler 及失败后成功复用全部通过（full-spark-airflow-p1000-r1）。工作流回归 21 passed；正式文件发布仍 stage-c-verified，未操作数据库或简历。见 [当前交接](HANDOFF_SPARK_FULL_ETL.md)、[Stage J 证据](stage-j/README.md)。以下为历史记录，旧“待新 Airflow/仅交接”等描述不再表示当前状态。

> **当前续做入口（2026-09-21）：[完整 Spark ETL 交接](HANDOFF_SPARK_FULL_ETL.md)、[Stage I](stage-i/README.md)。** 完整 Spark 千人/万人九表、ODS、账本、warnings 已双向零差异；万人约 283 秒（含对账）、2.52 GiB 采样 RSS，不属于 Python 500 MiB 验收。待补跑 3 个新边界用例和新 Airflow 编排。以下是历史交接，不再作为最新待办。

> 最终交接更新（2026-09-21）：Airflow 已完成真实 Linux/WSL2 验收。运行时为 Ubuntu 24.04.5 / WSL2、Python 3.12.3、Airflow 2.11.2、JDK 21；源万人快照只读校验通过，source_manifest_sha256=69a9f6652041c7475bca33fa81ece23d5430f8a912721b5d08853ce266b0a11c。正式 DAG DagBag 和 dag.test 通过；真实 SequentialExecutor scheduler run `manual__p10000-airflow-r3` 成功，verify_snapshot try=1、export_fhir try=2、compare_spark try=1 全部成功。FHIR 首次任务故障后成功产物被哈希核验并复用，FHIR 结果与 Windows 已验证结果一致；正式文件发布仍 `stage-c-verified`。完整证据：[Airflow验收](stage-h/acceptance-p10000-airflow-r3.json)、[安装状态](stage-h/installation.json)。
>
> r2 失败原因已定位并保留：SequentialExecutor 子进程找不到 `airflow`，验收脚本未把 airflow-venv/bin 放入 PATH；r3 已修复。localhost proxy 提示不是错误。WSL 项目挂载为每次命令临时只读 bind mount，重启 WSL 后需重新挂载；未改变正式发布。
>
> 当前项目状态：FHIR、Spark 同口径、Airflow 均已完成万人快照验证；所有用户未提交修改保留。后续可更新简历/项目材料，或在明确需求后扩展完整 Spark CSV ETL、FHIR Validator/术语服务、并发 API 压测和增量 CDC；这些均不是本轮已完成证据。
> 最新交接补充（2026-09-21，Airflow后续）：已补齐 scripts/airflow/bootstrap_linux.sh、acceptance.py、retry_acceptance_dag.py。生产DAG调用独立worker Python；src/snapshot_workflow.py按run_id/try_number隔离尝试，保留失败目录，重试核验并复用已成功结果。7项针对性测试通过，完整隔离DB回归84 passed/2 Spark skipped。正式文件及DB仍stage-c-verified，万人快照和已有结果未覆盖。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
>
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
> 当前硬阻塞：无可用WSL/Linux/Docker。WSL安装命令被自动审批拒绝，因为是持久OS变更并可能要求重启，需要用户明确批准“安装WSL2+Ubuntu24.04，发行版存储在F盘，不自动重启”。安装命令未执行，不得绕过拒绝。已完成可独立推进的代码/脚本/测试，接下来获取该明确授权或用户提供Linux环境。Airflow运行时、DagBag、dag.test、调度器及任务重试仍未实测，不能标完成。详情docs/stage-h/README.md；下方是历史交接。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
> 最新交接（2026-09-21，本轮已完成 FHIR 并推进 Spark/Airflow）：FHIR 默认入口已切换到 SQLite 暂存逐条导出，复用万人快照导出 708,747 资源；完整采样 253.326 秒、74.07 MiB，千人逐条旧版等价、两次万人产物哈希一致。首轮导出成功但监控结果保存失败，记录保留，完整性能证据为 docs/stage-g/p10000-fhir-r2.json。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
>
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
> Spark：F 盘 .venv 已安装 PySpark 4.0.1/Py4J 0.10.9.9，便携 JDK 21 在 output/runtime/jdk21。新 src/spark/synthea_compare.py 从同一快照 DIM/DWD/桥表重算三张 DWS/ADS；千人、万人全量双向 exceptAll 均无差异。万人25.843秒/929.94 MiB，非500 MiB目标，也不是完整Spark CSV清洗替代。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
>
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
> Airflow：dags/synthea_snapshot_validation.py 和 src/snapshot_workflow.py 已完成；手动快照核验→FHIR→Spark，重试复用成功产物并校验哈希，失败目录拒绝覆盖。任务函数已对万人成功产物验证，DAG仅编译检查。Windows 无可用 WSL/容器，Airflow运行时、DagBag、dags test及调度器验收尚未完成；下一步准备Linux环境后继续，不能宣称已运行Airflow。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
>
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
> 回归：79 passed/2 Spark skipped，另跑真实 Spark 2 passed，合计81项通过。首次DB回归连接拒绝是本地服务未启动，失败保留；启动项目原有 PostgreSQL 后复验通过。正式文件及DB仍 stage-c-verified，万人DB没有重新加载。未生成数据、未改简历、未提交或清除任何用户修改。详细报告及复现入口见 docs/stage-g/README.md；下面为历史交接。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
> 最新交接（2026-09-21）：用户准备新开会话。下一步仍是FHIR规模导出，随后Spark同口径和Airflow；尚未开始修改FHIR导出器，本轮只是读取代码后切换到简历任务。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
>
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
> 简历：用户已明确授权修改resume.tex，覆盖早期“不要改resume.tex”的约束。已更新医疗项目为已验证版（172.6万源行、文件ETL峰值280.76 MiB、数据库加载327秒、73项测试、FHIR R4B摘要），日期为2026.04--至今；备份在output/resume-check/resume-before-20260921.tex。docs/resume/experience-verified.tex为已验证片段，experience-target.tex为含待验收Spark/Airflow/FHIR分块内容的目标稿，不能当作已完成证据。尚未生成PDF；本地缺resume.cls等模板。用户偏好直接返回与原文长度相近的简历文字，不要额外版本说明或冗长内容。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
>
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
> 新发现的年度口径：万人批次就诊时间实际跨1915--2026年，2025年就诊48,972、检查1,534；生成配置虽写十年，不能据此声称仅十年历史。年度窗口、CDC、水位、分区发布及增量汇总均未实现。现在不插入年度扩展任务，先完成现有FHIR/Spark/Airflow计划。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
>
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
> 关键限制：万人数据库加载已提交，首次后续API OperationalError根因未确认；复验132次串行ASGI请求通过，非HTTP并发压测。正式文件/数据库发布仍stage-c-verified，规模数据库schema为synthea_scale_db_20260920。所有未提交改动需保留，不reset、不重生万人数据、不输出数据库密码。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
# 交接：万人文件ETL、数据库分块加载与API复验完成
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
更新：2026-09-20。用户已要求继续，本轮按原交接的测试→千人等价→时区→万人受控实测→文档顺序完成。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
## 结果与证据
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 万人归档复用；完整处理1,725,660源行，输出11,476患者、677,836就诊、19,435检查、1,036,348实例。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 进程树250ms采样RSS峰值280.76 MiB，墙钟392.618秒；退出0，所有对账与Parquet回读通过。一次成功测量，不是多次稳定性验收。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 千人UTC与旧版九张模型表、处置账本、源清单、质量和数量全部一致。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 显式+08:00生日口径修复千人45条误隔离，保留事件UTC；默认仍+00:00。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 最新完整回归73 passed，2条依赖弃用警告，包含数据库隔离schema；新增11项流式/监控及17项数据库测试。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 十二章HTML及1366/390宽度检查通过，所有章节导航可到十二章。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
详见[实测报告](stage-e/STREAMING.md)、[万人JSON](stage-e/p10000-stream-local-r1.json)、[当前进度](PROGRESS.md)。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
## 代码入口
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
src/streaming_pipeline.py：SQLite磁盘暂存，关联组分批复用transform，输出九表JSON/Parquet、ODS、账本、清单。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
src/stream_io.py：分块哈希和JSON数组输出。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
scripts/benchmark_streaming.py：500 MiB监控、唯一批次、非正常退出补FAILED、千人旧版等价比较。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
scripts/document_streaming_results.py：从保存证据生成STREAMING.md和第十一章。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
tests/test_streaming_pipeline.py / tests/test_streaming_monitor.py：新增覆盖。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
原型每批1万行在千人真实数据失败：单患者最多29,846行。现在上限40,000行/32 MiB JSON，仍整组处理；超大组主动失败。外部保护始终500 MiB。内存并非任意规模恒定：患者连通索引会随患者数增长。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
## 最新数据库交付
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- src/json_stream.py分块解析；src/database/streaming_load.py分批入库，保留单事务提交和SQL对账；原store.load_run已切换至新实现。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 每批1000条/2 MiB，加入API查询索引及UTC会话设置。新增17项测试；完整73 passed。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 复用万人快照，隔离schema synthea_scale_db_20260920加载327.112秒；客户端采样峰值128.75 MiB，服务端独立采样948.36 MiB，不是整个服务低于500 MiB。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 首次提交后API OperationalError根因未确认；第二次ALREADY_LOADED，132次串行ASGI请求对照全部通过。初次失败保留，不重复加载冒充首次通过。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 加载期间API仍可见旧千人快照，正式synthea_v1仍stage-c-verified。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- scripts/benchmark_database.py、scripts/verify_database_scale.py、tests/test_streaming_database.py；证据docs/stage-f，报告第十二章。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
## 下一步
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
FHIR规模导出（先检查全量读取并改分块），然后Spark同口径、Airflow。后续若OperationalError重现，应保留安全诊断信息定位，不能将一次复验通过称为长期稳定。完整FHIR影像series缺可信StudyInstanceUID，暂不能补造。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
外部强杀可能留暂存文件；控制器补FAILED，但没有硬断电、多发布者并发验收。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
## 环境与约束
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
F:\project\medical-etl-fhir-platform，显式指定workdir；.venv\Scripts\python.exe。D盘旧路径不可用。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
万人ZIP：data/generated/scale-v4-p10000/source.zip，不重复下载/生成。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
新产物：output/scale-stream-final/runs/p10000-stream-local-r1。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
正式文件与PostgreSQL发布仍stage-c-verified，均已只读核对；未覆盖正式批次。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
PostgreSQL 127.0.0.1:55432 / medical_etl / synthea_v1；凭据不打印不提交。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
不要改resume.tex；工作区未提交，不reset。原始数据/运行输出被Git忽略，需独立备份。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
流式未接主run_etl.py，使用受控benchmark_streaming入口；新run_id不可覆盖历史。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
验证脚本已补安全phase/sqlstate/error_category记录；此诊断增强在实测后添加，未重新执行万人加载。错误分类仅记录固定标签，不输出连接凭据或源行。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`


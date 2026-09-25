> 新任务入口（2026-09-22）：已整理无历史待办混淆的 [当前交接](HANDOFF_SPARK_FULL_ETL.md)，包含 Stage L 结果、失败修复、环境、教材与万人后续步骤。用户准备自行新开任务；本轮未启动新验收。

> **2026-09-22 Stage L 已完成：** 完整 Spark DAG 已串起 FHIR 和隔离 PostgreSQL，千人真实调度与两个下游重试复用通过，73,627 个 FHIR 资源等价、数据库11组内容一致、132次串行API通过。正式发布不变。见 [Stage L](stage-l/README.md)；下方旧待办为历史记录。

> **2026-09-21 最新 Stage K：** Spark 目录产物已接通 PostgreSQL/FHIR；千人 73,627 个 FHIR 资源等价、数据库 11 组双向零差异、132 次串行 API 校验通过，正式文件/数据库发布仍 stage-c-verified。待将下游消费加入完整 Spark DAG，万人消费者规模未验收。见 [当前交接](HANDOFF_SPARK_FULL_ETL.md)、[Stage K](stage-k/README.md)。下文为历史记录。

> **2026-09-21 本轮已完成：** 3 个 Spark 边界用例通过，独立完整 Spark Airflow 千人 DagBag、dag.test、真实 scheduler 及失败后成功复用全部通过（full-spark-airflow-p1000-r1）。工作流回归 21 passed；正式文件发布仍 stage-c-verified，未操作数据库或简历。见 [当前交接](HANDOFF_SPARK_FULL_ETL.md)、[Stage J 证据](stage-j/README.md)。以下为历史记录，旧“待新 Airflow/仅交接”等描述不再表示当前状态。

> **当前续做入口（2026-09-21）：[完整 Spark ETL 交接](HANDOFF_SPARK_FULL_ETL.md)、[Stage I](stage-i/README.md)。** 完整 Spark 三源 CSV → 质量治理 → 九表已实现，千人/万人 14 组（九表、ODS、账本、warnings）双向零差异；万人 SUCCESS，283.201 秒（含回读/对账），采样 RSS 约 2.52 GiB。待补跑 3 个新边界用例并实现独立新 Airflow 链路。下方为历史记录，Stage H r3 仅编排旧快照/FHIR/三表 Spark。用户要求晚点续做，本次只完成交接；未改简历或正式发布。

# 最终进度：2026-09-21

- Airflow 实际验收完成：WSL2 Ubuntu 24.04.5、Airflow 2.11.2、Python 3.12.3、JDK 21；DagBag、dag.test、真实 scheduler 均通过。
- `export_fhir` 首次尝试后故障注入重试成功，try=2 复用 try=1 的成功 manifest；FHIR 结果与 Windows 验证结果一致。
- `verify_snapshot`、`export_fhir`、`compare_spark` 全部成功；正式文件发布保持 `stage-c-verified`。
- 完整证据：[Airflow验收](stage-h/acceptance-p10000-airflow-r3.json) · [阶段H报告](stage-h/README.md)。
- r2 scheduler 失败因 PATH 缺少 Airflow bin，已修复并由 r3 复验；localhost proxy 提示可忽略。
# Airflow 继续推进：2026-09-21
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
独立worker、按运行/尝试隔离输出、成功复用和失败保护已完成；Linux部署及真实调度器验收脚本已备好。7项专项通过；完整隔离回归84 passed、2 Spark专项跳过。系统缺少Linux运行环境，WSL安装因系统级变更被自动审批拒绝，等待用户明确安装授权。Airflow实际运行尚未完成。[详细进度及安装范围](stage-h/README.md)。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
# 最新进度补充：2026-09-21
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- FHIR 万人分块导出完成：708,747资源，253.326秒/74.07 MiB；千人逐条等价、万人两次哈希一致。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- Spark 三张DWS/ADS从同一DIM/DWD重算，千人/万人全量零差异；万人25.843秒/929.94 MiB。不是完整Spark清洗替代。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- Airflow DAG与任务重试/复用逻辑完成，实际Linux运行时/调度验收待完成。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 79项隔离数据库回归通过，另2项真实Spark通过；正式文件/数据库仍stage-c-verified；未重生万人数据，全部未提交修改保留。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- [本轮报告、失败记录及完整证据](stage-g/README.md)。以下为之前阶段记录，原“待验证”描述以此补充为准。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
# 项目进度
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
更新：2026-09-20。万人文件ETL及数据库分块加载已完成，API抽样经复验通过。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
## 当前阶段
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
| 阶段 | 状态 |
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
| --- | --- |
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
| A/B 复习、数据剖析与建模 | 完成；三表56字段及粒度契约 |
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
| C 小样本闭环 | CSV→分层文件→PostgreSQL→API；FHIR R4B摘要导出已验证 |
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
| D 可靠性 | 本轮完整回归73项通过，含隔离schema数据库测试；2条依赖弃用警告 |
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
| E 文件规模 | 万人完整ETL成功；采样峰值280.76 MiB，392.618秒；万人当前仅一次成功实测 |
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
| F 组件与复盘 | 万人数据库加载、API抽样通过；FHIR规模、Spark同口径、Airflow待验证 |
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
## 本轮可信结果
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 复用万人ZIP：11,476患者、677,836就诊、1,036,348影像实例，共1,725,660条输入。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 输出：19,435检查、19,435序列、1,036,348实例；患者DIM/DWS/ADS各11,476；日×模态14,682组。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 九张模型表JSON/Parquet、三张ODS JSON、处置账本、警告、哈希、对账和Parquet回读全部完成，临时SQLite已关闭并清理。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 千人默认UTC口径：九张表、完整账本、源清单、数量及质量与旧版一致；238.21 MiB、23.975秒。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 生日显式+08:00口径：千人45条误隔离恢复；事件时间、日模态统计仍为UTC。万人全部输入有效。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 250ms采样整个ETL进程树RSS，共享页可能重复计数，短暂峰值可能漏采；500 MiB不是操作系统硬限额。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 原型每批10,000行不足以容纳单患者29,846行，改为每批最多40,000行/32 MiB原始JSON；超大关联组主动失败，外部500 MiB保护未提高。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
## 环境与发布
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
实际项目根目录：F:\project\medical-etl-fhir-platform；使用.venv。D盘旧路径不可用。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
正式文件及数据库发布均仍为stage-c-verified；本轮已只读核对。PostgreSQL为127.0.0.1:55432 / medical_etl / synthea_v1。连接配置已Git忽略，不打印密码。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
新产物：output/scale-stream-final/runs/p10000-stream-local-r1。没有发布到正式数据库。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
代码与文档未提交；resume.tex未修改。原始归档、输出和凭据不由Git备份。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
## 继续任务
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
1. FHIR导出仍需规模验证：先检查其全量读取与资源缓存，再分块导出并复用万人快照验收；不要重复生成数据。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
2. 需要更稳健性能结论时再安排独立万人复跑，比较semantic_sha256；当前不宣称已完成多次万人稳定性测试。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
3. FHIR完整series仍需可信StudyInstanceUID；官方Validator/术语服务/FHIR服务未验证。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
4. Spark同口径对照、Airflow编排，最后完善面试材料。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
5. 外部强制终止仍可能留下磁盘暂存目录；控制器会补FAILED，但硬断电和多发布者并发未完成验证。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
## 本轮数据库/API结果
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 隔离schema：synthea_scale_db_20260920；当前p10000-stream-local-r1，保留千人批次。正式synthea_v1未切换。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 万人加载327.112秒；客户端进程树峰值128.75 MiB；独立PostgreSQL进程树采样峰值948.36 MiB（共享页可能重复计数）。不能宣称整套数据库低于500 MiB。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 首次加载SQL对账全部通过，之后API阶段OperationalError，根因未确认。复用已提交数据复验成功，返回ALREADY_LOADED；132次串行ASGI请求通过，并非HTTP并发压测。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 修复API输出会话时区，显式UTC；单批1000条/2 MiB输入，一个事务统一提交。新增17项解析/回滚/对账测试。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- 万人加载期间，另一API请求仍可见千人旧快照；所有章节1366/390宽度检查通过。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
- [数据库验收报告](stage-f/README.md) · [第十二章](html/12-database-scale.html) · [73项测试输出](stage-f/test-results.txt)。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
## 证据入口
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
[流式验收报告](stage-e/STREAMING.md) · [第十一章](html/11-streaming.html) · [万人证据](stage-e/p10000-stream-local-r1.json) · [测试记录](stage-e/streaming-test-results.txt) · [交接](HANDOFF_MEMORY_500M.md)
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
小样本历史：108患者、5,571就诊、413检查、478实例；17项数据库/API核验，FHIR共6,092资源、9项导出核验。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`
旧版万人内存失败和早期原型失败均保留在第十章及stage-e证据，不能混作本轮结果。
`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`

`以下内容为历史交接记录；若与最上方最终更新冲突，以最终更新和 stage-h/acceptance-p10000-airflow-r3.json 为准。`


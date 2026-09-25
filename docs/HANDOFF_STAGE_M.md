# Stage M 交接：万人完整链路已通过

更新：2026-09-25。r6 于 2026-09-24 完成验收，今日使用标准收集器归档并独立验据通过。**Stage M 已完成，无需重复运行全量验收。**

## 已核实结果

独立 session `full-consumers-p10000-r6` 使用既有万人 ZIP、Python 基准与 FHIR 参考，完成 DagBag、生产四任务 dag.test、真实 SequentialExecutor scheduler。生产 DAG 于 18:35 成功，scheduler 于 19:23 成功；时区 Asia/Shanghai。

- 两次 Spark 均首次成功，分别 375.373 秒和 373.898 秒；各有 26 项检查、14 组基准对账通过。完整产物清单和哈希已独立复核。
- scheduler 输入/Spark 为 try=1，FHIR/数据库为 try=2。两消费者首次持久化成功后注入故障，重试复用同一 manifest 路径和哈希，无第二份消费者产物。
- FHIR：11,476 Patient、677,836 Encounter、19,435 ImagingStudy，共 708,747 资源，与绑定的 Python 参考逐条等价。
- 数据库：九表、source_records、row_dispositions 共 11 组完整内容零差异；采用保留重复次数的 SQLite 磁盘索引核验。132 次串行 ASGI API 检查通过。
- 验收前后正式文件与数据库指针均为 `stage-c-verified`，未切换发布。2026-09-25 再次只读查看正式文件指针同值；今日未重新连接数据库。
- 状态读取遇到 8 次 SQLite 锁冲突，有限重试后继续完成，没有放宽总超时或消费者 1800 秒上限。

[完整报告](stage-m/full-consumers-p10000-r6/acceptance.json) · [独立验据](stage-m/full-consumers-p10000-r6/independent-verification.json) · [阶段说明](stage-m/README.md)。

## 修复与历史

r2 首次 FHIR 明确超时，SQLite 暂存改用 worker 系统临时目录后独立万人消费验证成功。r3 scheduler Spark 堆不足；后续显式配置 2g。r4 数据库首次超时且未提交，之后主机关机。r5 生产成功，但验收脚本查询 Airflow SQLite 报 database is locked，finally 清理终止 scheduler，整轮 FAILED；原始回执不改写。

r6 修复仅对 SQLite BUSY/LOCKED 读取错误重试，每次重新建立会话，并检查 scheduler 存活与原总截止时间；其他错误仍失败。真实 Airflow 环境 13 项回归通过，Windows 核心环境因无 SQLAlchemy 为 8 passed、5 skipped。详细证据见 [锁冲突诊断](stage-m/scheduler-lock-diagnosis-20260924-r1/README.md) 和 [FHIR 诊断](stage-m/fhir-timeout-diagnosis-20260923-r1/README.md)。

## 环境与接手

实际项目目录为 `F:\project\medical-etl-fhir-platform`，D: 旧路径无效，shell 显式使用 F: workdir。Windows Python 为 `.venv/Scripts/python.exe`；WSL 为 MedicalETL-Airflow，runtime 在 `/opt/medical-etl-airflow/runtime/`。WSL 重启后只读 bind mount 需在同一 shell 恢复。

验收入口：`scripts/airflow/full_consumers_acceptance.py`。独立验据入口：`scripts/verify_stage_m_evidence.py`，依赖保留的 `output/full-consumers-p10000-r6` 产物；归档入口：`scripts/collect_consumer_airflow_evidence.py`。`output` 下旧 r5 finalize 脚本仍指向失败批次，不应运行。

本轮完成条件已经满足。后续如做 HTTP 并发、官方 FHIR Validator、独立备份审计或增量 CDC，应另定范围；当前没有这些成功结论。运行环境为 WSL/JDK21、Spark local[2]，无新增 RSS 测量或多机验证。

2026-09-25 用户已授权将项目代码、测试、阶段证据和教学文档提交至 GitHub。简历与完整数据产物留在本地；正式发布与失败现场保持原样。成功前上下文见 [历史交接](history/HANDOFF_STAGE_M_before_stage_m_success_20260925.md)。

README、复习指南与 15 章教材已同步。396 个本地链接、重复构建一致性、Edge 桌面/手机 30 视图检查通过，新增段落已目检；详见 [页面 QA](html/qa-stage-m-20260925.json)。

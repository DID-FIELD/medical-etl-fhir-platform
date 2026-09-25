> 新任务入口（2026-09-22）：已整理无历史待办混淆的 [当前交接](HANDOFF_SPARK_FULL_ETL.md)，包含 Stage L 结果、失败修复、环境、教材与万人后续步骤。用户准备自行新开任务；本轮未启动新验收。

> **2026-09-22 Stage L 已完成：** 完整 Spark DAG 已串起 FHIR 和隔离 PostgreSQL，千人真实调度与两个下游重试复用通过，73,627 个 FHIR 资源等价、数据库11组内容一致、132次串行API通过。正式发布不变。见 [Stage L](stage-l/README.md)；下方旧待办为历史记录。

> **2026-09-21 最新 Stage K：** Spark 目录产物已接通 PostgreSQL/FHIR；千人 73,627 个 FHIR 资源等价、数据库 11 组双向零差异、132 次串行 API 校验通过，正式文件/数据库发布仍 stage-c-verified。待将下游消费加入完整 Spark DAG，万人消费者规模未验收。见 [当前交接](HANDOFF_SPARK_FULL_ETL.md)、[Stage K](stage-k/README.md)。下文为历史记录。

> **2026-09-21 本轮已完成：** 3 个 Spark 边界用例通过，独立完整 Spark Airflow 千人 DagBag、dag.test、真实 scheduler 及失败后成功复用全部通过（full-spark-airflow-p1000-r1）。工作流回归 21 passed；正式文件发布仍 stage-c-verified，未操作数据库或简历。见 [当前交接](HANDOFF_SPARK_FULL_ETL.md)、[Stage J 证据](stage-j/README.md)。以下为历史记录，旧“待新 Airflow/仅交接”等描述不再表示当前状态。

# 项目任务记忆
> 2026-09-21 最新：[完整 Spark 交接](HANDOFF_SPARK_FULL_ETL.md) / [Stage I](stage-i/README.md)。千人/万人完整 Spark 对账已通过，待补 3 个边界用例和新 Airflow。用户要求晚点继续，本次仅完成交接。下文为旧记录。

更新：2026-09-20。用户要求继续，已完成万人文件ETL、数据库分块加载，API抽样经复验通过。

## 目标
用可复现Synthea实测支撑医疗ETL/FHIR项目的规模、数据流转、质量、恢复和面试讲解。自动驾驶仅作经验迁移讨论。不修改resume.tex。

## 最新交付
万人完整文件ETL成功：1,725,660输入行；11,476患者、677,836就诊、19,435检查、1,036,348实例。进程树采样RSS峰值280.76 MiB，392.618秒；当前一次万人成功运行。
千人UTC口径与旧版九张表、账本、源清单及质量一致；+08:00生日口径恢复45条误隔离。
73项测试通过，含隔离schema数据库测试；十二章HTML在1366/390宽度检查通过。
正式文件及数据库发布仍stage-c-verified，万人输出在独立output/scale-stream-final。没有重新生成数据。

## 继续入口
优先读[进度](PROGRESS.md)、[交接](HANDOFF_MEMORY_500M.md)、[流式验收](stage-e/STREAMING.md)。
下一阶段：FHIR规模导出（检查全量读取并改分块），随后Spark同口径、Airflow。保留首次数据库加载后API OperationalError的未定位事项；重试已通过，不将首次失败抹去。
数据库加载327.112秒，客户端峰值128.75 MiB；独立PostgreSQL进程树峰值948.36 MiB。500 MiB仅约束客户端。隔离schema synthea_scale_db_20260920当前p10000-stream-local-r1，首次提交后API OperationalError，复验132个串行ASGI请求通过且ALREADY_LOADED，没有重写数据。详见[数据库报告](stage-f/README.md)。

根目录F:\project\medical-etl-fhir-platform，使用.venv。数据库127.0.0.1:55432 / medical_etl / synthea_v1。凭据在Git忽略文件，不输出。
工作区大量未提交修改，不reset、不覆盖；resume.tex原有文件不动。没有安排自动任务。

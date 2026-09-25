> 续做（2026-09-24）：已确认 r5 失败来自验收脚本查询 Airflow SQLite 时的 database is locked，随后清理逻辑终止 scheduler。锁冲突有限重试修复在真实 Airflow 环境 13 项测试通过；独立 r6 已启动，结果待定，Stage M 仍未通过。正式数据库启动前复核为 stage-c-verified。[诊断](stage-m/scheduler-lock-diagnosis-20260924-r1/README.md)。

# Stage M 交接：r5 生产通过、scheduler 失败现场待续

更新：2026-09-24 14:00（Asia/Shanghai）。r5 生产 dag.test 已通过，但真实 scheduler 未完成，报告最终为 FAILED（OperationalError）。**Stage M 尚未通过。**

## 当前运行

- Session：`full-consumers-p10000-r5`，DagBag PASSED；生产 dag.test SUCCESS。生产 Spark 405.215 秒，26 项检查、14 组基准对账全部通过；FHIR SUCCESS（11,476/677,836/19,435，共 708,747）。数据库 try=1 超过 1800 秒后，try=2 SUCCESS，状态为 `ALREADY_LOADED`，并完成数据库核验；正式数据库指针仍 `stage-c-verified`。
- r5 总报告当前为 `FAILED`，`error_type=OperationalError`。失败点在真实 scheduler 的 Spark 任务：scheduler run `manual__full-consumers-p10000-r5` 尚未完成，`full_spark_etl` try=1 的 manifest 遗留 `RUNNING`，FHIR/数据库尚未开始。不能把生产通过拼接成完整 scheduler 通过。
- 启动入口：`output/run-stage-m-r5.sh`；参数包含 `--population 10000 --spark-driver-memory 2g --scheduler-timeout 7200`。消费者保持 1800 秒上限。
- Windows 报告：`output/full-consumers-p10000-r5/acceptance.json`；简要核查：`output/stage-m-r5-status.py`。
- WSL 主日志：`/opt/medical-etl-airflow/runs/full-consumers-p10000-r5.log`。报告与 Airflow 元数据出现不一致，接手时先查实际进程、主日志和 worker 是否仍存活；不要凭 scheduler 状态断言仍在执行。
- 本次生产启动工具 session 43550，仅供历史参考；跨会话不要假定有效。自动审批额度目前不足，WSL 读取需等待额度恢复或由用户授权后再执行。

## 已完成的修复与验证

FHIR SQLite 索引改用 worker 系统临时目录。独立万人消费成功用时 742.634 秒；708,747 资源逐条等价、成功重试复用和暂存清理通过。测试分三批：36 项消费/FHIR 回归、4 项数据库回归、8 项验收参数测试，不混为一次运行。详见 [诊断记录](stage-m/fhir-timeout-diagnosis-20260923-r1/README.md)。

r3 生产 dag.test 成功，但 scheduler 两次 Spark Java heap space，整轮 FAILED；实际默认堆为 1 GiB。验收入口新增显式堆参数，默认 2g，记录到报告。r4 已实采确认 -Xmx2g 但数据库消费超时后主机关机。r5 实采 `-Xmx2g`，生产 Spark/FHIR/数据库通过；scheduler 仍在 Spark 阶段失败或遗留运行状态，需补充日志根因。

## 必须保留的失败历史

- r1/r2 中断；r2 首次 FHIR 明确超时，不能只归因于关机。
- [r3](stage-m/full-consumers-p10000-r3/README.md)：scheduler Spark 堆不足，FAILED。
- [r4](stage-m/full-consumers-p10000-r4-interrupted/README.md)：Spark 首次成功；数据库首次 1800 秒超时，FHIR 随后留下 RUNNING。09-24 01:58 主机关机，12:37 日志服务启动。12:44 无验收进程，数据库本轮未提交；原回执不改写。
- r4 曾观测宿主内存压力，但这不证明全部超时原因。12:39 可用内存已恢复约 5.4 GiB；没有结束项目外进程。
- r5 数据库 try=1 实际提交后超时，try=2 复用同一 schema 并返回 `ALREADY_LOADED`，不要删除 schema 或重跑装载。此前数据库复核曾在来源记录/血缘查询上等待磁盘 I/O。

## 环境与保护范围

实际目录 `F:\project\medical-etl-fhir-platform`，D: 旧路径无效；所有 shell 显式指定 F: workdir。Windows Python 为 `.venv/Scripts/python.exe`。WSL 为 MedicalETL-Airflow，root；runtime 位于 `/opt/medical-etl-airflow/runtime/`。WSL 重启后需在启动脚本同一 shell 恢复只读 bind mount。项目 PostgreSQL 已恢复，12:44 只读确认正式数据库指针为 stage-c-verified。

复用既有万人 ZIP、Python 基准和 FHIR 参考，不重新生成数据。保留全部未提交改动和失败现场，不改简历，不切换正式文件/数据库发布，不提交 Git。

## 完成条件和下一步

下一步先恢复 WSL 可读检查，定位 r5 scheduler OperationalError 和遗留 Spark RUNNING manifest；确认没有活进程后，保留 r5 原始 FAILED 现场。只有一轮新的完整 scheduler SUCCESS，且两个消费者均完成故障注入后 try=2 复用、11 组数据库内容核验、132 次串行 ASGI、708,747 FHIR 资源等价和正式发布不变，才运行标准收集器归档并提升 Stage M。不要运行待 SUCCESS 门槛脚本。

`output/verify-stage-m-evidence-r5.py` 和两个 `finalize-stage-m-*-r5.py` 是待审核、带 SUCCESS 门槛的辅助脚本，目前未执行。成功后先独立验据，再更新文档和 HTML；失败则保留事实继续定位。

教学第 14/15 章已补充堆配置与 FHIR 暂存案例。当前 15 页、388 本地链接、重复构建字节一致、Edge 30 视图检查通过；手机第 14 章新增段落已目检。[页面 QA](html/qa-stage-m-20260924.json) 不代表业务验收通过。

详细环境、历史命令和旧原文见 [历史交接](history/HANDOFF_STAGE_M_before_r5_20260924.md)。本轮没有 RSS、多机集群、HTTP 并发或官方 FHIR Validator 的新结论。

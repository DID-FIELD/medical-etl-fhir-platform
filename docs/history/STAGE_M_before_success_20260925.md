> 续做（2026-09-24）：r5 生产 dag.test 成功，但真实 scheduler 被验收脚本的 SQLite 状态读取锁冲突中断，整轮 FAILED。已修复有界重试并通过真实 Airflow 环境 13 项测试；r6 正在完整复验，Stage M 尚未通过。[诊断与原始日志](scheduler-lock-diagnosis-20260924-r1/README.md)。

> 最新核查（2026-09-24 14:00）：r5 生产 dag.test 已成功；Spark 405.215 秒、FHIR 708,747 资源通过；数据库首次超时后 try=2 返回 ALREADY_LOADED 并完成核验。真实 scheduler 阶段因 OperationalError 未完成，整轮 FAILED；Stage M 仍未通过。详见 [当前交接](../HANDOFF_STAGE_M.md)。

> 当前状态（2026-09-24）：r4 中断已归档，r5 使用显式 2 GiB Spark 堆执行中；Stage M 尚未通过。见 [当前交接](../HANDOFF_STAGE_M.md) 与 [r4 核查](full-consumers-p10000-r4-interrupted/README.md)。以下时间线保留历史状态。

> 最新核查（2026-09-23 23:50）：r3 已结束并归档 FAILED。生产四任务 dag.test 成功，但真实 scheduler 的 Spark 两次 Java heap space；新 r4 已显式配置并实采确认 2 GiB 堆；Spark 首次尝试 SUCCESS（26 项检查、14 组对账通过），生产数据库消费执行中。独立 FHIR 修复验证保持成功；下方运行中描述为历史。[r3 失败证据](full-consumers-p10000-r3/README.md)。

> 续做更新（2026-09-23）：FHIR 暂存改用 worker 本地目录后，独立万人消费 742.634 秒 SUCCESS；708,747 资源逐条等价、成功重试复用均通过。新 r3 完整 DAG 验收执行中，尚无整轮通过结论。[诊断证据](fhir-timeout-diagnosis-20260923-r1/README.md)。

> 最新状态（2026-09-23 11:20）：r2 Spark 和数据库 11 组核验成功；FHIR 首次 1800 秒超时、第二次遗留 RUNNING，当前无验收进程。详见 [当前交接](../HANDOFF_STAGE_M.md) 和 [r2 现场核查](full-consumers-p10000-r2-interrupted/inspection.json)。

# Stage M — 万人完整 Spark 下游调度验收

2026-09-22。承接 Stage L 千人结果，使用新 session `full-consumers-p10000-r1` 验证万人四任务 DAG。该轮异常中断，未得到 SUCCESS；2026-09-23 独立 r2 复验也未收尾；最新情况见下方及当前交接。

## 输入与验收方法

复用 `data/generated/scale-v4-p10000/source.zip`、Python 基准 `output/scale-stream-final/runs/p10000-stream-local-r1` 与 Stage G 已成功的 FHIR 参考 `output/fhir/p10000-stream-r2`。参考绑定源 manifest，并在运行前后校验三个 NDJSON 的哈希；按资源类型、ID、规范化内容比较，允许输出行序不同。

验收入口现要求显式传入人口规模、ZIP、基准和 FHIR 参考，调度等待超时可配置（默认 7200 秒）。声明万人时必须匹配 11,476 Patient、677,836 Encounter、19,435 ImagingStudy，共 708,747 个资源，不能只修改 session 名。

计划依次验证 DagBag、生产 dag.test、真实 SequentialExecutor scheduler、两个消费者成功后注入失败及重试复用、数据库 11 组完整内容、132 次串行 ASGI API、正式文件与数据库指针不变。每轮使用独立 run、隔离 schema 和全新输出目录；失败现场保留。

## 参数校验测试

`tests/test_full_consumers_acceptance.py`：7 passed / 2.70 秒。覆盖混用规模、来源 run/hash 不匹配、失败参考、校验失败、数量篡改、参考文件篡改，以及无效参数拒绝。日志：`output/full-consumers-scale-tests-r1.log`。

## 边界

本次不新增 RSS 测量。历史 Python 客户端 500 MiB 结论不能套用到 Spark/PostgreSQL；本机 WSL local[2] 和串行 ASGI 检查不代表多机集群或 HTTP 并发。FHIR 为摘要导出，非官方 Validator 或 FHIR Server 验收。

## 可复现命令

先按交接恢复项目只读 bind mount，在同一 WSL 会话执行下列命令。复跑时必须替换 session 为尚不存在的新名称，不覆盖本次现场。

```bash
/opt/medical-etl-airflow/runtime/airflow-venv/bin/python \
  /opt/medical-etl-airflow/project/scripts/airflow/full_consumers_acceptance.py \
  --session full-consumers-p10000-r1 \
  --population 10000 \
  --archive /opt/medical-etl-airflow/project/data/generated/scale-v4-p10000/source.zip \
  --baseline /opt/medical-etl-airflow/project/output/scale-stream-final/runs/p10000-stream-local-r1 \
  --fhir-reference /opt/medical-etl-airflow/project/output/fhir/p10000-stream-r2 \
  --scheduler-timeout 7200
```

保留每个 worker 原有 1800 秒上限；7200 秒是整轮真实 scheduler 的等待上限，不是新增性能指标。千人复跑同样必须显式传入千人 ZIP、基准、Stage K FHIR 参考和 `--population 1000`。原 Stage L 命令只有 session 参数的形式已不适用于新入口。

完成后通过 `scripts/collect_consumer_airflow_evidence.py --session <新session> --stage stage-m` 归档。收集器拒绝覆盖已有证据目录，Stage M 不混入旧 Stage L 测试日志。

## 2026-09-23 中断核查与复验

r1 的 Spark manifest 为 SUCCESS，数据库 manifest 和总报告遗留 RUNNING。接手时没有验收进程，项目端口 55432 不接受连接。恢复既有项目 PostgreSQL 后，服务日志明确记录上次未正常关闭并执行自动恢复；具体触发原因未确认，不归因为代码超时。

只读核查确认隔离 schema `synthea_airflow_3dac2edb120ad71dab3da76e630295dc` 已有该次 run 的提交记录，正式数据库仍 `stage-c-verified`。这只证明事务已提交，不代表后续 11 组逐行核对或完整 DAG 验收通过。原 manifest 不改写为 SUCCESS/FAILED，保留中断现场，核查证据见 [只读回执](interruption-check-20260923.json)。

全新 session `full-consumers-p10000-r2` 复用相同输入、基准及 FHIR 参考，使用独立输出和派生 schema。r2 尝试执行生产 dag.test，但没有整体成功；真实 scheduler 验收尚未开始。不覆盖 r1，不切换正式发布。

Windows System 日志补证：2026-09-22 19:31:25 的 1074 事件记录开始菜单进程发起关机，19:31:34 日志服务停止、19:33:34 恢复。确认未完成验收期间发生宿主机关机；这解释运行环境退出，但不能证明此前数据库核验耗时的原因。见 [系统事件摘要](full-consumers-p10000-r1-interrupted/system-events.json)。

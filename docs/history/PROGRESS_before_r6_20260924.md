> 最新核查（2026-09-24 14:00）：r5 生产 dag.test 已成功；Spark 405.215 秒、FHIR 708,747 资源通过；数据库首次超时后 try=2 返回 ALREADY_LOADED 并完成核验。真实 scheduler 阶段因 OperationalError 未完成，整轮 FAILED；Stage M 仍未通过。详见 [当前交接](HANDOFF_STAGE_M.md)。

> 最新续做（2026-09-24 12:44）：r4 已确认中断，数据库首次 1800 秒超时且未提交；FHIR 遗留 RUNNING，WSL 无验收进程；主机 01:58 关机。正式数据库指针复核仍 stage-c-verified。[中断证据](stage-m/full-consumers-p10000-r4-interrupted/README.md)。r5 正在启动，继续使用 2 GiB Spark 堆；Stage M 尚未通过。以下 r4 执行中描述均为历史。

> 最新核查（2026-09-23 23:50）：r3 已结束并归档 FAILED。生产四任务 dag.test 成功，但真实 scheduler 的 Spark 两次 Java heap space；新 r4 已显式配置并实采确认 2 GiB 堆；Spark 首次尝试 SUCCESS（26 项检查、14 组对账通过），生产数据库消费执行中。独立 FHIR 修复验证保持成功；下方运行中描述为历史。[r3 失败证据](stage-m/full-consumers-p10000-r3/README.md)。

> 续做更新（2026-09-23）：已开始 FHIR 超时诊断，局部 SQLite 写入对照显示 DrvFS 21.05 秒、Linux 临时目录 2.43 秒（各 10 万条）。导出器已改用系统临时目录，36 项针对性测试及另一批 4 项数据库测试通过。复用 r2 Spark 的隔离万人导出已 SUCCESS（742.634 秒），708,747 资源逐条等价及成功重试复用通过；r3 完整 DAG 首次 Spark 尝试发生 Java heap space，当前由 Airflow 自动进行第二次尝试，总结果待完成；本段之后的 11:20 状态为历史核查。详见 [诊断记录](stage-m/fhir-timeout-diagnosis-20260923-r1/README.md)。

# 当前进度

更新：2026-09-23。实际项目目录为 `F:\project\medical-etl-fhir-platform`。

## 当前待处理

最新交接：[Stage M 交接](HANDOFF_STAGE_M.md)（2026-09-23 11:20 核查）。当前没有验收进程，未启动新一轮。

r2 的完整 Spark 和数据库消费已 SUCCESS，11 组内容核验全部零差异；FHIR try=1 在 1800 秒超时，try=2 遗留 RUNNING。宿主机 03:24 关机，r2 总报告未收尾；生产 dag.test 没有整体成功，未进入真实 scheduler 完整验收。下一步先定位 FHIR 超时，不能直接把 r2 当作后台运行或已通过。

r1 同样保留中断现场。正式文件指针本次只读检查仍 stage-c-verified；正式数据库最后一次核查为 r2 启动前同值，本次未复连。详见 [Stage M 记录](stage-m/README.md)。

## 已有成功证据

| 范围 | 已验证结果 | 证据 |
| --- | --- | --- |
| Python 万人文件 ETL | 1,725,660 源行，392.618 秒，采样 RSS 280.76 MiB | [Stage E](stage-e/STREAMING.md) |
| Python 万人数据库/API | 加载成功，132 次串行 ASGI 检查通过 | [Stage F](stage-f/README.md) |
| Python 万人 FHIR | 708,747 个摘要资源 | [Stage G](stage-g/README.md) |
| 完整 Spark 千人/万人 | 九表、ODS、账本、warnings 共 14 组双向零差异 | [Stage I](stage-i/README.md) |
| 千人完整四任务 DAG | dag.test、真实 scheduler、双下游重试、11 组数据库核对及 132 次 API 通过 | [Stage L](stage-l/README.md) |

## 边界和收尾

- 万人新四任务 DAG 最终结果以 Stage M 证据为准；不把旧快照 DAG 的万人成功拼接成新链路成功。
- 本轮未新增 RSS、HTTP 并发、多机集群或官方 FHIR Validator 验证。
- 保留现有未提交改动与失败现场；未修改简历、未提交 Git、未切换正式发布。
- 旧进度完整保存在 [历史快照](history/PROGRESS_before_stage_m_20260923.md)，当前状态以本页和 Stage M 为准。

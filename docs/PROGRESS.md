# 当前进度：Stage M 已完成

更新：2026-09-25。独立 r6 万人完整 Spark → FHIR/数据库 DAG 于 09-24 成功，今日归档与独立验据通过。

| 范围 | 已验证结果 | 证据 |
| --- | --- | --- |
| Python 万人流式 ETL | 1,725,660 源行，392.618 秒，采样 RSS 280.76 MiB | [Stage E](stage-e/STREAMING.md) |
| Python 万人数据库/API | 加载成功，132 次串行 ASGI 检查 | [Stage F](stage-f/README.md) |
| Python 万人 FHIR | 708,747 摘要资源 | [Stage G](stage-g/README.md) |
| 完整 Spark 千人/万人 | 九表、ODS、处置账本、warnings 共 14 组双向零差异 | [Stage I](stage-i/README.md) |
| 千人完整四任务 DAG | 生产 dag.test、真实 scheduler、双消费者重试通过 | [Stage L](stage-l/README.md) |
| 万人完整四任务 DAG | 生产与真实 scheduler 成功；两个消费者 try=2 复用；11 组数据库核验、132 次 API、708,747 FHIR 资源等价 | [Stage M](stage-m/README.md) |

r6 的生产和 scheduler Spark 分别用时 375.373 / 373.898 秒，均首次通过 26 项检查和 14 组基准对账。运行记录 8 次状态读取锁冲突，修复后成功重试。正式文件与数据库发布在本轮验收前后均保持 stage-c-verified。

r1/r2/r4 中断、r3 Spark 堆不足、r5 SQLite 状态读取锁冲突现场全部保留。r6 为独立完整成功证据，不拼接历史失败批次。

当前没有待跑的 Stage M 验收。无本轮 RSS、多机集群、HTTP 并发或官方 FHIR Validator 新结论。详细命令、保护范围及可选后续工作见 [当前交接](HANDOFF_STAGE_M.md)。09-25 用户授权同步项目至 GitHub，简历保持本地原样。

[独立验据回执](stage-m/full-consumers-p10000-r6/independent-verification.json) · [成功前进度](history/PROGRESS_before_stage_m_success_20260925.md)。

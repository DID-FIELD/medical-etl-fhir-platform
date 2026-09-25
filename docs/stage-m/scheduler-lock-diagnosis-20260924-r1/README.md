# r5 scheduler 状态轮询锁冲突诊断

> 2026-09-25 收尾：r6 整轮 SUCCESS，运行中 8 次锁冲突均重试后继续，生产 DAG 与真实 scheduler、双消费者复用和最终 FHIR/数据库/API 核验全部通过。[独立验据](../full-consumers-p10000-r6/independent-verification.json)。下文“r6 执行中观察”保留采集当时的状态。

2026-09-24 续做。WSL 已恢复读取，检查时没有存活的 Airflow、Spark 或验收进程。

r5 主日志显示：生产 dag.test SUCCESS；进入真实 scheduler 后，验收脚本读取 DagRun 的 SELECT 抛出 `sqlalchemy.exc.OperationalError: (sqlite3.OperationalError) database is locked`。scheduler 日志显示 13:46:16 接到 SIGTERM，而 Spark 当时仍有 Stage 166 进度。验收脚本 finally 会向 scheduler 进程组发送 SIGTERM。因此不能将本次中断归因为 Spark 堆不足或成功完成。

修复只对 SQLite BUSY/LOCKED 状态读取重试。每次失败关闭原会话，下次创建新会话；仍受 scheduler 存活和原总超时限制，其他 OperationalError 不重试。报告记录锁重试次数，不改消费者 1800 秒上限和验收标准。

真实 Airflow Python 使用现有 Windows pytest 纯 Python 包运行验收测试：13 passed。包括实际 SQLite EXCLUSIVE 锁释放恢复、持续锁定截止、进程退出及非锁 I/O 错误传播。Windows 核心环境无 SQLAlchemy：8 passed、5 skipped；不是全部测试通过的替代证据。

启动前只读数据库探针 SUCCESS，正式指针 stage-c-verified。新 r6 使用独立目录，显式 2g Spark 堆、7200 秒 scheduler 上限。r5 原始 FAILED 报告与遗留 RUNNING manifest 保留。Stage M 等待新一轮完整验收结果。

## r6 执行中观察

r6 生产 Spark 首次 SUCCESS，375.373 秒、26 项检查与 14 组基准对账通过。生产 FHIR SUCCESS，共 708,747 资源。生产数据库仍执行中，尚未进入真实 scheduler。额外只读探针曾发生一次 5 秒连接超时；PostgreSQL 日志多次记录 WAL 触发检查点，并提示检查点过于频繁，单次检查点观察到 30–79 秒。此为运行观察，不能直接推断最终失败原因或完整链路成功。

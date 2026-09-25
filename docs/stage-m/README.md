# Stage M：万人完整 Spark 下游调度验收通过

2026-09-25 归档与独立复核。`full-consumers-p10000-r6` 于 09-24 完成 SUCCESS，生产 dag.test 和真实 SequentialExecutor scheduler 均完整通过。

| 验收项 | r6 结果 |
| --- | --- |
| 输入与隔离 | 复用万人 ZIP、Python 基准和绑定的 FHIR 参考；独立 run、schema、输出目录 |
| 生产 DAG | 四任务成功；Spark 375.373 秒，26 项检查、14 组对账通过 |
| 真实 scheduler | 四任务成功；Spark 373.898 秒，26 项检查、14 组对账通过 |
| 故障注入与重试 | 输入/Spark try=1；FHIR/数据库首次成功后注入故障，try=2 复用同一 manifest 路径/哈希，无 attempt-0002 产物 |
| FHIR 内容 | 11,476 Patient + 677,836 Encounter + 19,435 ImagingStudy = 708,747，与 Python 参考按类型、ID、内容逐条等价 |
| 数据库内容 | 九表、source_records、row_dispositions 共 11 组零差异，SQLite 磁盘索引保留重复次数 |
| API | 132 次串行 ASGI 检查通过 |
| 发布保护 | 验收前后正式文件和数据库指针均为 stage-c-verified |
| 状态读取 | 8 次 SQLite 锁冲突有限重试，最终成功 |

[完整报告](full-consumers-p10000-r6/acceptance.json) · [独立验据回执](full-consumers-p10000-r6/independent-verification.json) · [证据说明](full-consumers-p10000-r6/README.md)。

## 故障历史与修复

- r1/r2 中断现场保留；r2 首次 FHIR 明确 1800 秒超时。FHIR SQLite 索引改用 worker 系统临时目录后，独立万人导出 742.634 秒成功，资源等价与成功重试复用通过。[FHIR 诊断](fhir-timeout-diagnosis-20260923-r1/README.md)。
- r3 生产 DAG 成功，但 scheduler 两次 Spark Java heap space；运行时默认堆 1 GiB。[r3 证据](full-consumers-p10000-r3/README.md)。
- r4 显式 2g 后 Spark 首次成功，数据库首次 1800 秒超时且未提交，后续主机关机。[r4 证据](full-consumers-p10000-r4-interrupted/README.md)。
- r5 生产成功，数据库首次提交后超时，try=2 返回 ALREADY_LOADED 并核验成功；scheduler 状态读取抛出 database is locked，验收脚本清理终止 scheduler，整轮 FAILED。[r5 日志与诊断](scheduler-lock-diagnosis-20260924-r1/README.md)。
- r6 只对 SQLite BUSY/LOCKED 状态读取重试，每次重新建会话，保留总截止时间、进程退出检查与其他错误传播。真实 Airflow 环境 13 项测试通过；Windows 核心环境 8 passed、5 skipped（缺 SQLAlchemy）。这些测试不与历史批次累加。

本轮未修改转换口径、消费者 1800 秒上限或验收门槛。r6 运行曾观察到频繁 WAL 检查点，但未据此修改数据库参数或声称已解决全部 I/O 性能问题。

## 复核与复现

在项目 F: 目录执行 `.venv/Scripts/python.exe scripts/verify_stage_m_evidence.py` 可复核归档与保留的 output 产物；独立复核包含两次 Spark 完整清单和哈希、消费产物哈希、代码绑定及重试回执。该命令不重新执行 ETL。

本轮启动脚本为 `output/run-stage-m-r6.sh`，包含 `--population 10000 --spark-driver-memory 2g --scheduler-timeout 7200`。未来复跑必须使用新 session，不能覆盖 r6。WSL 重启后需先恢复项目只读 bind mount；详细环境见 [交接](../HANDOFF_STAGE_M.md)。

标准收集器 `scripts/collect_consumer_airflow_evidence.py --session full-consumers-p10000-r6 --stage stage-m` 已运行，拒绝覆盖现有证据目录。归档内的 `full-consumers-scale-tests-r1.log` 是历史参数测试，不是新的整套回归。

## 边界

单机 WSL/JDK21、Spark local[2]、SequentialExecutor；不是多机或并发 worker 验证。本轮未采样 RSS，2g 堆不是总内存上限。132 次 ASGI 是串行检查，非 HTTP 并发压测；FHIR 是 R4B 摘要映射，非官方 Validator/完整 Server 验收。无 CDC 或跨系统分布式事务结论。

未切换正式发布、未修改简历；09-25 用户授权将项目与证据同步 GitHub。完整历史准备与失败时间线见 [成功前阶段说明](../history/STAGE_M_before_success_20260925.md)。

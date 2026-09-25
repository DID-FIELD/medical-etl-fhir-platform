# r6 万人完整链路成功证据

2026-09-24 执行成功，2026-09-25 通过标准收集器归档及独立验据。

[验收报告](acceptance.json) · [生产日志](acceptance.log) · [scheduler 日志](scheduler.log) · [API 明细](database-after.api.json) · [独立复核](independent-verification.json)。

生产 dag.test 于 18:35:22 成功，真实 scheduler 于 19:23:43 成功，随后完成最终 API/FHIR/发布保护校验，19:26 正常清理 scheduler（Asia/Shanghai）。两次 Spark 首次成功，用时 375.373 / 373.898 秒，无失败 Spark 尝试。

scheduler 输入和 ETL 为 try=1；FHIR、数据库各为 try=2。首次成功持久化后由验收 DAG 注入失败，第二次返回相同 manifest 和哈希；没有新的 attempt-0002 消费产物。两个消费者均完成成功复用验证。

数据库九表、来源记录、处置账本 11 组逐行内容一致；来源记录和账本各 1,725,660 行。FHIR 708,747 个资源与绑定的 Python 参考逐条等价。132 次串行 ASGI 检查通过。正式文件/数据库指针验收前后均为 stage-c-verified。

验收器经历 8 次 SQLite 状态读取锁冲突并有限重试成功。此项证明本轮遇到锁冲突后可以继续，不能推广为并发执行器可靠性保证。r5 FAILED 原始证据仍在 [诊断目录](../scheduler-lock-diagnosis-20260924-r1/README.md)。

独立复核程序为 `scripts/verify_stage_m_evidence.py`，复核回执记录验收 JSON 与程序 SHA-256，覆盖两个 Spark 产物清单、消费者产物、代码绑定和重试回执。完整数据仍保留在 `output/full-consumers-p10000-r6`；本目录是日志与 manifest 证据归档，不是独立数据备份。

无新增 RSS、多机集群、HTTP 并发或官方 FHIR Validator 结论。归档附带的 r1 参数测试日志为历史批次，不与本轮锁冲突专项的 13 项测试混算。

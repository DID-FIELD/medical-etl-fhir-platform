# 数据质量与验收口径

更新：2026-09-25。当前 Synthea 质量治理位于 [synthea_pipeline.py](../src/synthea_pipeline.py)、[streaming_pipeline.py](../src/streaming_pipeline.py) 和 [Spark 规则](../src/spark/synthea_rules.py)，不是只运行旧 `src/quality/` 的通用规则。

## 输入、处置与告警

| 情况 | 行为 |
| --- | --- |
| 必需 CSV/列缺失，CSV 记录宽度错误 | 批次失败，不发布不完整结果 |
| 同一业务键、全部原始字段相同 | 保留一条，重复行记为 `duplicate` |
| 同一业务键、任一原始字段冲突 | 该键相关记录全部隔离 |
| 必填、值域或时间规则不合法 | 记录处置原因，按规则隔离 |
| 患者/就诊父级不存在或未被接受 | 关联记录级联隔离 |
| 检查含无效实例，或检查/序列元数据冲突 | 整项检查隔离，避免发布不完整检查 |
| 影像时间越出有效就诊区间 | 保留 warning，不直接因此丢弃检查 |
| 无有效患者 | 拒绝发布 |
| 有有效患者但无影像 | 允许发布，保留零检查患者 |

处置账本覆盖每条源逻辑记录，最终状态为 `accepted`、`duplicate` 或 `quarantined`。warnings 独立保留，重复行或最终隔离行也可能带 warning，不能仅统计 accepted 行上的警告。

## 时间与追溯

事件时间规范化为 UTC，日×模态统计也按 UTC。已验收流式基准的出生日期比较使用显式 `+08:00`；CLI 默认是 `+00:00`，复现时必须传参，见[运行说明](runtime.md)。

ODS 保留原始字段，实例来源由源 CSV SHA-256 与从 1 开始的逻辑记录序号定位。稳定 UUID5 替代键属于假名化；ODS 中仍保留完整源信息。当前数据是 Synthea 合成数据。

## 如何验证正确性

- **结构约束**：九表唯一键、父子关系、患者覆盖、检查计数及桥表统计守恒。
- **来源账平**：源记录都有处置；accepted 来源与下游血缘一致。
- **内容等价**：完整 Spark 对 Python 九表、三份 ODS、dispositions、warnings 共 14 组双向多重集对账，保留重复次数和原因顺序。
- **消费者核验**：PostgreSQL 九表、source_records、row_dispositions 共 11 组完整内容核验；FHIR 与绑定参考资源逐条语义比较。
- **恢复契约**：失败保留现场；重试核验成功 manifest 和产物哈希后复用；损坏的成功产物明确报错。

26 项 Spark 内部检查和 14 组基准对账见 [Stage I](stage-i/README.md)；万人消费、双消费者重试与 132 次串行 ASGI 验证见 [Stage M](stage-m/README.md)。Stage K 使用 SQL 双向比较，Stage L/M 使用保留重复次数的 SQLite 磁盘索引；不能把不同验收器说成同一实现。

## 查看结果

从对应运行目录的 `manifest.json`、质量账本及阶段验收报告开始；输出格式与规模随小样本、streaming 和 Spark 链路不同，不能假设所有质量结果都写入 `output/quality`。失败批次与早期诊断保留在阶段记录中，不改写为成功。

## 旧 EMR 规则

旧示例通过 `src/quality/` 执行 `not_null`、`unique`、`allowed_values`、`date_parseable` 并将 CSV 报告写入 `output/quality`。这些规则用于原 EMR 演示，不代表当前 Synthea 的父子关系、检查级隔离或完整内容对账。

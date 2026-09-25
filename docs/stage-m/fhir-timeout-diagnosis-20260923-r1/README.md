# FHIR 超时诊断与修复

2026-09-23。r2 两次 FHIR 现场保留，首次在 1800 秒超时；两次均已写出 Patient 和部分 Encounter，没有 ImagingStudy。当前诊断没有改写其 manifest。

## 隔离 I/O 对照

在同一 WSL worker、同一来源的前 100,000 条 Encounter 上，使用与导出器相同的 SQLite 表结构和 8 MiB 缓存，分别把临时数据库放在 `/mnt/f` 与 `/tmp`。读取同一 NDJSON，单事务逐条插入，不更改源文件。

- DrvFS：21.0547 秒，4,749.5 行/秒。
- Linux 临时目录：2.4338 秒，41,087.8 行/秒。
- 本次局部写入对照约 8.65 倍差异；不是完整导出加速比，也不是多次统计结果。顺序固定为 DrvFS 再 Linux，文件缓存可能影响绝对数值。

证据：[测量 JSON](io-probe.json)、[诊断脚本](io_probe.py)。

## 最小修复

`src/fhir/streaming_export.py` 改用 worker 的系统临时目录保存 SQLite 索引，NDJSON 与回执仍写原输出目录。没有增大 1800 秒超时，没有取消模型、唯一性、引用、回读或来源哈希检查。运行环境应将系统临时目录放在 worker 本地磁盘；本次 WSL 为 `/tmp`。正常成功或异常退出会清理索引；强杀/断电仍可能留下临时目录，不提供断电恢复保证。

36 项针对性测试通过，4 项数据库相关用例未在该批执行，2 条已有依赖弃用警告。覆盖外部暂存位置、成功/失败清理、JSON/Parquet 等价、失败拒绝、来源绑定及重试复用。[日志](tests.log)。

项目数据库恢复后，另外一批数据库相关回归 4 passed / 23.76 秒（14 deselected），包含事务提交后回执失败恢复和同数量内容篡改拒绝。不与前一批合称单次测试。[日志](database-tests.log)。正式数据库只读复核仍为 stage-c-verified。

## 万人验证

使用原 r2 Spark SUCCESS 快照，在独立诊断根目录下保持原 run_id；先校验 Spark 代码、输入和全部产物哈希，再执行新代码消费者。消费者报告绑定新的代码哈希，不复用旧代码的成功消费者回执。

独立验收 SUCCESS：消费者运行 742.634 秒，低于保持不变的 1800 秒限制；重试返回同一 manifest 与哈希，没有 attempt-0002。11,476 Patient、677,836 Encounter、19,435 ImagingStudy，合计 708,747 个资源逐条内容一致。此耗时包含消费者内部来源校验与导出回读，不包含前置 Spark 验证和后续独立等价比较；本轮未采样 RSS，也不是多次稳定性性能测量。

[独立验收报告](independent-export.json) · [旧尝试数量与暂存现场](old-attempts.json)。

独立验证后，r3 生产四任务 dag.test 已通过；真实 scheduler 的 Spark 两次发生 Java heap space，整轮 FAILED。这是另一项资源问题，并非 FHIR 超时重现。r4 现以显式 2g Spark 堆配置继续完整验收。详见 [r3 记录](../full-consumers-p10000-r3/README.md)。

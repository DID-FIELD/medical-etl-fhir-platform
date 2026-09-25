# r3 完整验收失败：真实 scheduler 的 Spark 堆不足

2026-09-23。总报告 FAILED，进程已退出。生产 dag.test 的 Spark 首次 Java heap space 后 try=2 成功，随后数据库和 FHIR 均 SUCCESS，四任务生产 DAG 成功。真实 scheduler 的 Spark try=1/2 均 Java heap space，下游为 upstream_failed，因此没有本轮双消费者成功重试、全量 FHIR 比较及最终 API 验收成功结论。

运行中的 JVM 采集为 -Xmx1g，没有额外 Spark 内存环境变量。错误位于 Spark 缓存反序列化，不能将一轮自动重试通过写成可靠性保证。[报告](acceptance.json) · [生产日志](acceptance.log) · [scheduler 日志](scheduler.log) · [实际参数](../fhir-timeout-diagnosis-20260923-r1/r3-spark-runtime.json)。

后续验收入口新增 --spark-driver-memory（默认 2g）并记录配置，参数校验 8 项测试通过；新 r4 使用显式 2g 复验。1800 秒消费者超时不变，不跳过任何内容核对。本轮不新增 RSS 测量。

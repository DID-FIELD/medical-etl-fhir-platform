# Stage K — Spark 目录产物接入 PostgreSQL / FHIR

> 后续更新：Stage L 已完成千人完整 Spark DAG 的 FHIR/数据库下游接入和重试验收，见 [报告](../stage-l/README.md)。下文保留本阶段当时的范围。

2026-09-21。本轮复用 Stage I 已成功的千人 Spark 输出，不重跑 ETL，不生成数据。Windows 消费副本位于 output/spark-consumers-p1000-r1/source，原 WSL 产物保留。

## 实现

- `src/snapshot_reader.py`：按 manifest 区分旧 JSON 快照与完整 Spark Parquet 目录；递归核验文件清单和 SHA-256，拒绝缺失、额外、损坏文件及符号链接。Parquet 每批最多 500 行，逐文件读取，不依赖 Spark/JVM。
- 九表读取检查列名、run_id 和总行数；ODS 直接使用持久化的 source_row 与 raw map，不按分区顺序重新编号。
- `src/fhir/streaming_export.py`：沿用 R4B 摘要映射、SQLite 磁盘索引、唯一 ID、引用及回读校验，新增格式读取并在结束时复核来源。
- `src/database/streaming_load.py`：两种格式共用分批事务加载；新增 ODS 序号范围检查，并在切换隔离 schema 的 current 前复核源文件及 manifest。源在加载中变化会回滚。
- `scripts/verify_database_scale.py`：串行 API 校验支持两种格式；`scripts/verify_spark_consumers.py` 负责全链路隔离验收。

## 测试与失败记录

- r1：旧 FHIR/JSON/数据库相关测试 31 passed；新增 10 项因测试 fixture 引用了旧小样本没有的 warnings.json 而 setup error。不是产品运行失败，现场与日志保留。
- r2：移除未消费的 warnings fixture 引用，新增 10 passed / 11.61 秒，包含乱序分区血缘、FHIR 逐资源等价、缺失/额外/损坏文件、run_id/计数不匹配、真实隔离数据库双向逐行对账及加载期间源变化回滚。
- 两批为不同测试范围，不宣称一次完整回归 41 passed。两次均有 2 条既有依赖弃用警告。

## 千人真实验收

会话 output/spark-consumers-p1000-r1/acceptance，隔离 schema synthea_scale_consumers_p1000_r1，**SUCCESS**。证据：[验收报告](p1000-r1/acceptance.json)、[API 明细](p1000-r1/api.json)、[源 manifest](p1000-r1/source-manifest.json)。

- FHIR：Patient 1,159、Encounter 70,229、ImagingStudy 2,239，共 73,627 个资源；与 Python 基准双向内容差异均为 0，唯一 ID、引用和回读检查通过。
- PostgreSQL：九表 + source_records + row_dispositions 共 11 组 SQL 双向多重集差异均为 0。Python 快照加载 23.732 秒，Spark 快照加载 24.066 秒，均包含校验；这不是 ETL 时间，也未采样本轮 RSS。
- 幂等重放：ALREADY_LOADED；132 次串行 ASGI API 请求通过，包含零检查患者、检查列表和实例血缘。
- 正式文件 current 和正式 synthea_v1.current_snapshot 前后均 stage-c-verified。只更新新隔离 schema 的指针，不发布正式批次。
- 测试日志：[r1（31 passed / 10 fixture errors）](spark-consumers-tests-r1.log)、[r2（新增 10 passed）](spark-consumers-tests-r2.log)。

验收内容：真实 Spark Parquet 与绑定 Python 基准分别导出 FHIR，SQLite 按资源类型/ID/规范化 JSON 做双向内容比较；两个快照加载到同一个全新隔离 schema 后，对九表、ODS、处置账本做 SQL EXCEPT ALL 双向多重集比较；重复加载必须 ALREADY_LOADED；继续验证 API。正式文件和 synthea_v1 的指针前后只读核对。

## 范围

- 本轮仅千人消费验收；没有万人消费者规模或内存峰值结论。
- Spark 文件分区顺序不同，FHIR 以内容/ID 等价为准，不要求 NDJSON 字节哈希与 Python 输出相同。
- PostgreSQL 继续保留原模型，不新增 warnings 数据库表；warnings 仍留在源快照中。
- 尚未把消费者追加到完整 Spark Airflow DAG，也未改变旧 snapshot_validation DAG 的输入契约。本轮提供消费函数、现有 CLI 和独立验收入口。
- R4B 摘要映射的边界不变：不是完整 FHIR Server、官方 Validator 或 Bulk Data 服务；API 验收仍为串行 ASGI 请求。

```powershell
.\.venv\Scripts\python.exe -m scripts.verify_spark_consumers `
  --spark output/spark-consumers-p1000-r1/source `
  --baseline output/scale-stream-final/runs/p1000-stream-local-r1 `
  --output output/NEW-UNUSED-DIRECTORY `
  --schema synthea_scale_consumers_new_unused_schema
```

schema 名需符合小写字母、数字、下划线约束；输出目录/schema 已存在均拒绝，不覆盖历史验收。

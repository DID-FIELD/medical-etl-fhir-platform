# Stage I — 完整 Spark 千人/万人与 15 组转换用例通过

本阶段复用归档，不生成数据，不发布正式 current，不操作数据库。2026-09-21 交接核实了此前万人作业的 SUCCESS，已复制最终证据；规模基准没有重跑；续做已补齐 3 个边界用例，新完整 Spark Airflow 千人验收见 [Stage J](../stage-j/README.md)。下一会话入口：[完整交接](../HANDOFF_SPARK_FULL_ETL.md)。

## 实现

- `src/spark/synthea_ingest.py`：严格、分批 CSV → Parquet；保留全字段字符串、逻辑 source_row、参考原始字段签名。
- `src/spark/synthea_etl.py`：Spark window/groupBy/join 进行冲突检测、父表关联、检查级隔离和九表转换；worker 规则仅依赖标准库。
- `src/spark/synthea_full.py`：独立 CLI、失败留档、不可覆盖、回读校验、九表/ODS/账本/warnings 双向多重集对账；模型 run_id 不参与比较。
- `scripts/benchmark_spark_full.py`：Linux 进程树 RSS 250 ms 采样，包含 JVM 和 Python workers，共享页可能重复计数。不可套用 Python 的 500 MiB 结论。

## 已有验证

- Windows 规则及 CSV adapter：62 passed。
- WSL 小样本 r3：71 passed / 81.45 秒。含 8 组真实 Spark 九表、账本、warnings 对照，以及入口回读、空患者失败、同目录拒绝覆盖。
- Windows 相关回归：97 passed、3 skipped / 49.71 秒；3 项数据库测试未启用，不是数据库重新验收。
- 新增 4 个小样本边界用例：4 passed / 46.07 秒。以上测试批次有重叠，不累加为完整回归总数。
- 千人全量 SUCCESS：14 组（九表、账本、warnings、三份 ODS）双向零差异；ETL/回读/对账 80.961 秒，外层计时 84.597 秒，进程树 RSS 峰值 2,432,512,000 bytes。见 [manifest](manifest-p1000-r1.json)、[测量](measurement-p1000-r1.json)。
- 万人全量 SUCCESS：14 组双向零差异，26 项内部检查全部通过；ETL/回读/对账 283.201 秒，外层 284.082 秒，RSS 峰值 2,701,537,280 bytes（约 2.52 GiB）。见 [manifest](manifest-p10000-r1.json)、[测量](measurement-p10000-r1.json)。这是 WSL/JDK 21、Spark local[2] 单机实测；时间包含基准对账，不能直接与 Python 纯 ETL 时间比较。
- 万人九表关键数量：11,476 患者、677,836 就诊、19,435 检查/系列、1,036,348 实例、19,435 桥表记录、14,682 日×模态记录；8,473 零检查患者，warnings=0。源归档/基准 manifest 哈希、生日口径 +08:00 与已验收基准一致。
- 本次核对当前五个 src/spark/synthea_*.py 文件哈希与万人 manifest 一致；完整产物仍在 WSL，项目目录保存证据副本。本次未重新读取所有规模产物，回读/哈希验证证据来自成功运行 manifest。

## 本轮续做与剩余范围

1. study_metadata、series_metadata、encounter_conflict 已补跑：3 passed、13 deselected / 42.57 秒，见 [WSL r5](tests-wsl-r5.log)。当前 15 组完整转换参数用例均有真实 Spark 执行证据；各批次不合并宣称为单次完整回归。
2. 独立完整 Spark Airflow task/DAG 已实现，千人 DagBag、dag.test、真实 scheduler 注入失败与重试复用通过，见 [Stage J](../stage-j/README.md)。失败留档、损坏结果拒绝与运行隔离由 21 项工作流回归覆盖。旧 Stage H r3 仍仅指旧快照链路。
3. 新 Parquet 目录已适配 PostgreSQL/FHIR 消费器并完成千人验收，见 [Stage K](../stage-k/README.md)。README、复习指南、HTML 前五章已同步当前证据；后续专项章节保留历史案例。

## 环境和失败记录

- Windows r1：JDK 21 + 项目 Python 3.13 下，Spark Python worker 首次 UDF 执行时 WinError 10038 / EOFException 退出；尚未归因。不代表数据对账失败。
- WSL r2：临时项目挂载随 WSL 退出丢失，pytest 找不到测试文件，未运行测试。
- WSL r3：挂载恢复与执行置于同一会话，项目保持只读，测试通过。
- 既有 worker 新增 pandas 2.2.3、pyarrow 25.0.0、pytest 8.4.2。未改 Airflow venv。
- Linux 结果在 `/opt/medical-etl-airflow/runs/`；保留 r1/r2/r3，不覆盖旧 Stage H r3。

正式文件 current 本轮复核仍为 `stage-c-verified`。未修改简历，未提交或清理原有工作区改动。

## 命令与产物边界

在 JDK 21、PySpark 4.0.1、Pandas 2.2.3、PyArrow 25.0.0 的 WSL worker 中，从只读项目目录执行：

```bash
python -m src.spark.synthea_full \
  --archive data/generated/scale-v4-p1000/source.zip \
  --baseline output/scale-stream-final/runs/p1000-stream-local-r1 \
  --output /opt/medical-etl-airflow/runs/NEW-UNUSED-DIRECTORY \
  --run-id NEW-RUN-ID --birth-date-offset +08:00
```

输出为 Spark Parquet 数据集目录（包含 part 文件），不是旧单文件 Parquet 发布接口；新产物现已通过公共读取器接入 PostgreSQL/FHIR 加载器并完成千人验收（Stage K），与本阶段 Spark CLI 规模验收分开记录。ODS 保存 source_row 和完整 raw map。对账保留 reasons 数组顺序、warnings 重数及 ODS 原始行号。基准必须提供成功 manifest、归档哈希、源文件哈希和相同生日时区。

所有输出先 RUNNING，回读核验及产物哈希复核通过后才 SUCCESS；异常写 FAILED，并保留现场。此 CLI 不维护 current 指针。

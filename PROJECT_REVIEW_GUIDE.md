# 医疗数据项目复习指南

更新：2026-09-25。项目使用 Synthea 合成数据，重点是患者、就诊、检查、序列和实例的粒度、质量治理、来源追溯与可复现对账。当前进度见 [交接](docs/HANDOFF_STAGE_M.md)，旧教学样本和探索过程保留在 [HTML 分章](docs/html/01-overview.html)。

## 1. 已实现的数据流

```text
Synthea ZIP：patients / encounters / imaging_studies
  → ODS：完整原始字段、文件 SHA-256、逻辑记录序号
  → 质量治理：同键冲突、父子关系、检查级级联隔离
  → DIM 患者
  → DWD 就诊 / 检查 / 序列 / 实例
  → 检查×模态桥表
  → DWS 患者汇总 / UTC日×模态统计
  → ADS 患者概览
```

同一业务口径有两条执行路径：Python streaming 用分批读取与磁盘暂存控制内存；完整 Spark 用严格 CSV adapter 保留输入，再用 window/groupBy/join 完成质量治理、关联与聚合。Spark worker 中的逐行规则依赖标准库。Python 已有单文件快照可继续加载 PostgreSQL、提供 API 或导出 FHIR；新 Spark 输出是 Parquet 目录，现已通过公共分批读取器接入 PostgreSQL/FHIR，并完成千人消费验收，详见 [Stage K](docs/stage-k/README.md)。

## 2. 当前可信实测

| 部分 | 实测结果 | 边界与证据 |
| --- | --- | --- |
| Python 万人流式 ETL | 1,725,660 源行；392.618 秒；进程树采样 RSS 280.76 MiB | [Stage E](docs/stage-e/STREAMING.md)，500 MiB 是此客户端范围 |
| 万人九表 | 11,476 患者、677,836 就诊、19,435 检查/序列、1,036,348 实例；8,473 零检查患者 | 患者数、检查数与实例数不能混用 |
| PostgreSQL/API | 加载 327.112 秒；客户端 128.75 MiB、数据库进程树 948.36 MiB；132 次串行 ASGI 请求通过 | [Stage F](docs/stage-f/README.md)，首次 OperationalError 根因未定；不是 HTTP 并发压测 |
| 万人 FHIR | 708,747 资源；253.326 秒；74.07 MiB | [Stage G](docs/stage-g/README.md)，Patient/Encounter/ImagingStudy R4B 摘要映射 |
| 完整 Spark 千人/万人 | 九表、三份 ODS、dispositions、warnings 共 14 组双向多重集零差异；万人 26 项内部检查通过 | [Stage I](docs/stage-i/README.md)，万人 283.201 秒包含回读和基准对账，采样 RSS 约 2.52 GiB |
| 旧快照 Airflow | 万人快照校验 → FHIR → 三张汇总表 Spark 对账，真实 scheduler 重试通过 | [Stage H](docs/stage-h/README.md)，不包含 CSV 清洗 |
| 新完整 Spark Airflow | 千人 DagBag、dag.test、真实 scheduler 通过；首次成功后注入失败，try=2 复用 try=1 产物 | [Stage J](docs/stage-j/README.md)，与旧快照链路分开记录 |

以上时间对应不同任务范围，不能直接推导 Spark 相比 Python 的纯 ETL 加速比。RSS 每 250 ms 采样，可能漏掉短峰，共享页可能重复统计。Spark 是 WSL2/JDK 21、local[2] 单机，不代表多机集群规模能力。

千人 Spark 消费验收：73,627 个 FHIR 资源与 Python 基准内容等价；数据库九表、ODS 和处置账本共 11 组双向零差异；重复加载 ALREADY_LOADED，132 次串行 API 校验通过。正式文件和数据库发布不变。Stage L 后续已通过千人完整 DAG 下游编排及双消费者故障重试；Stage M r6 又完成万人新链路：生产 dag.test、真实 scheduler、双消费者 try=2 复用、708,747 FHIR 资源等价、11 组数据库内容核验与 132 次串行 ASGI 均通过。归档和完整产物哈希已独立复核；本轮没有 RSS 测量。详见 [Stage M](docs/stage-m/README.md)。

## 3. 粒度与质量规则

影像 CSV 的业务唯一键是 INSTANCE_UID；影像 Id 表示检查。一项检查可含多个序列和实例，检查次数必须按检查键统计。检查×模态桥表去重后用于日模态汇总，不能把实例行数当成检查数。患者汇总从全部有效患者出发，保留零检查患者。

同键所有原始字段都参与签名：完全相同记录重复去重，任一字段不同则该键全部记录隔离。只有 accepted 的患者与就诊可作为有效父级；任一实例无效或检查/序列元数据冲突，整项检查隔离。时间越出有效就诊区间形成 warning，重复或最终隔离行的 warning 也保留。

生日按显式 +08:00 比较，事件和日统计按 UTC；两者口径不同。源记录序号从 1 开始、不含表头，指逻辑 CSV 记录；带引号换行不能按物理文本行计数。API 可沿实例上的源文件哈希与记录序号追溯输入。

## 4. 如何证明结果正确

完整 Spark 与独立 Python 基准逐行、双向、多重集对账，覆盖九表、ODS 原字段和行号、含 reasons 顺序的处置账本、保留重复次数的 warnings。仅总数量相同不足以说明内容相同。另做粒度唯一性、父子外键、源记录账平、accepted 血缘、患者覆盖和检查计数守恒，共 26 项检查。

15 组完整转换参数用例都有真实 Spark 执行证据；最后补齐的 study_metadata、series_metadata、encounter_conflict 为 3 passed / 42.57 秒。工作流回归为 21 passed，其中新完整 Spark 工作流 14 项、既有快照工作流 7 项。历史测试批次可能重叠，不能简单累加为统一回归总数。

## 5. 恢复和发布

Spark 批次先写 RUNNING，持久化产物回读、对账和哈希校验后才 SUCCESS；失败写 FAILED 并保留现场，同目录不可覆盖。新 Airflow 工作流将 run_id 哈希为安全目录名，每个 try_number 对应独立尝试。重试先查成功结果，核验来源、代码、全部检查和递归文件清单/哈希；损坏 SUCCESS 明确失败，不能悄悄重算掩盖问题。

文件与 PostgreSQL 分别发布，不是跨系统分布式事务。当前正式文件指针仍为 stage-c-verified，新实验不切换它。稳定替代键属于假名化；本机 API 还没有生产级鉴权与负载验证。

## 6. 代码入口

| 文件 | 职责 |
| --- | --- |
| src/streaming_pipeline.py | 分批文件 ETL 和参考口径 |
| src/spark/synthea_ingest.py | 严格 CSV → Parquet、逻辑行号和签名 |
| src/spark/synthea_etl.py | Spark 质量治理和九表转换 |
| src/spark/synthea_full.py | 完整 Spark CLI、回读、对账与 manifest |
| src/spark_full_workflow.py | 新链路的尝试隔离与成功结果核验复用 |
| src/airflow_tasks.py / dags/synthea_full_spark.py | 独立 worker 适配和完整 Spark DAG |
| scripts/airflow/full_consumers_acceptance.py | 当前四任务 DAG 的 DagBag、dag.test、真实 scheduler 验收 |
| src/database/streaming_load.py / api/synthea.py | Python 快照数据库加载与查询 |
| src/fhir/streaming_export.py | 分块 FHIR 导出 |

Windows 工作目录为 F 盘，使用项目 .venv。Spark/Airflow 用既有 MedicalETL-Airflow WSL 发行版，项目只读挂载，输出保存在 Linux runs 下。详细命令见交接与各阶段报告。

## 7. 面试表达参考

“我用 Synthea 合成数据做了可复现的医疗数仓原型，按患者、就诊、检查、序列和实例建模，并保留来源与处置账本。万人归档约 173 万源行，Python 流式 ETL 约 393 秒，采样峰值约 281 MiB；数据库、串行 API 校验和约 71 万 FHIR 摘要资源导出也有证据。完整 Spark 在相同口径下对九表、ODS 和质量账本做了双向逐行对账，全部一致。Spark 实测是单机 local[2]、约 2.52 GiB，不能说成低于 500 MiB 或多机集群验证。Airflow 的两条链路及其验收规模分别记录，重试会核验并复用成功产物。”

## 8. 后续边界

新 Spark Parquet 目录已接入 PostgreSQL/FHIR 并完成千人验收；Stage L 已将消费者加入完整 Spark DAG 并通过千人真实调度与重试；Stage M r6 已完成万人新链路。r5 曾因验收器查询 SQLite 遇锁而终止 scheduler；增加受截止时间约束的锁冲突重试后，r6 恢复 8 次锁冲突并完成整轮验收。FHIR 暂存已改用 worker 本地目录，Spark 显式配置 2g 堆；消费者仍保持 1800 秒上限。官方 FHIR Validator、术语服务、完整 FHIR Server、并发 HTTP 压测和增量 CDC 尚未验收。备份独立副本与完整性还需审计。继续工作从 [当前交接](docs/HANDOFF_STAGE_M.md) 进入；不修改简历，不覆盖历史失败现场。

# 当前交接：完整 Spark 千人/万人已通过，待补边界测试与新 Airflow

更新时间：2026-09-21。用户要求本次完成交接，晚点再继续项目；本次只核实既有结果、归档证据和更新文档，没有启动新 ETL、测试或 Airflow 作业。

## 最新状态（下一会话从这里继续）

- 完整 Spark 三源 CSV → 质量治理 → 九表已实现。Python adapter 仅负责严格无损解析和逻辑行号；冲突治理、父级关联、级联隔离及聚合由 Spark 执行。
- 千人 `spark-full-p1000-r1`：SUCCESS，九表、dispositions、warnings、三份 ODS 共 14 组双向多重集零差异；内部检查通过；ETL/回读/对账 80.961 秒，外层 84.597 秒；采样 RSS 峰值 2,432,512,000 bytes（约 2.27 GiB）。
- 万人 `spark-full-p10000-r1`：本次重新读取最终 manifest/measurement，均为 SUCCESS、exit_code=0；14 组双向零差异、26 项内部检查全部通过。ETL/回读/对账 283.201 秒，外层 284.082 秒；采样峰值 2,701,537,280 bytes（约 2.52 GiB）。时间包含基准对账，不能直接与 Python 纯 ETL 耗时比较。
- 万人产出：11,476 患者、677,836 就诊、19,435 检查、19,435 系列、1,036,348 实例、19,435 桥表记录、14,682 日×模态记录；8,473 零检查患者；本归档 duplicate/quarantined/warning 均为 0。异常治理能力由故障 fixture 验证。
- 当前五个 `src/spark/synthea_*.py` 的 SHA-256 与万人 manifest 一致。本次核实源归档哈希、基准 manifest 哈希及生日口径 +08:00 均匹配。
- 证据：[Stage I](stage-i/README.md)、[万人 manifest](stage-i/manifest-p10000-r1.json)、[万人测量](stage-i/measurement-p10000-r1.json)、[千人 manifest](stage-i/manifest-p1000-r1.json)。完整产物在 WSL，对应 manifest/measurement 已复制到 F 盘。
- Windows 规则/接入 62 passed；WSL r3 71 passed（含 8 组真实 Spark 对照及入口可靠性）；r4 4 passed（另 4 组边界）。批次存在重叠，不能累加成完整回归总数。Windows 相关回归 97 passed、3 skipped，数据库专项未启用。
- **还有 3 个已写但未执行的参数用例：study_metadata、series_metadata、encounter_conflict。** 当前完整 Spark 参数表共 15 组，12 组有真实执行证据。
- 完整 Spark 尚未接入新 Airflow DAG。已验收 Stage H r3 只编排旧快照校验 → FHIR → 三张汇总表 Spark 对账。
- 这是 WSL/JDK 21、Spark local[2] 单机实测，不是多机集群验收，不满足 500 MiB 内存界限。
- 万人作业已经结束；上次自动审批审核额度用尽导致未能读取结果，本次已恢复读取和归档，不能再写“万人正在运行”。本次没有启动新长任务。
- 文件 current 本次重新读取仍为 stage-c-verified；未操作或查询数据库发布状态，未改简历，未提交或清理工作区改动。

### 本轮文件与运行现场

- `src/spark/synthea_rules.py`：标准库 worker 规则，已接入 UDF。
- `src/spark/synthea_ingest.py`：严格、分批 CSV → Parquet，保留全字段、逻辑 source_row 和参考签名。
- `src/spark/synthea_etl.py`：Spark window/groupBy/join 质量治理和九表转换。
- `src/spark/synthea_full.py`：独立 CLI，RUNNING/FAILED/SUCCESS manifest，拒绝覆盖，回读/外键/粒度/账平/血缘校验，14 组基准对账，产物哈希复核，不写 current。
- `scripts/benchmark_spark_full.py`：Linux /proc 每 250 ms 采样 driver/JVM/workers RSS，共享页可能重复计算、短峰可能漏采。
- `scripts/collect_spark_evidence.py`：复制已完成结果与日志到 Stage I，目标存在则不覆盖。
- 新测试：`tests/spark_fixtures.py`、`test_spark_rules.py`、`test_spark_ingest.py`、`test_spark_full_etl.py`。
- 新结果是 Spark Parquet **目录**；旧结果是单个 Parquet 文件。尚未适配旧 PostgreSQL/FHIR 消费接口，不要直接替换。
- Linux 运行根目录 `/opt/medical-etl-airflow/runs/` 下的 `spark-full-p1000-r1`、`spark-full-p10000-r1` 各有 `snapshot/`、`measurement.json`、`run.log`。均不可覆盖。
- Windows `output/spark-full-tests-r1`：首次 UDF 时 Python worker WinError 10038 / EOFException 退出，根因未定；完整 Spark 已在 WSL 验证。
- WSL `spark-full-tests-r2.log`：挂载失效、找不到测试文件，没有运行测试。r3/r4 通过日志已复制到 Stage I。
- Windows 相关回归现场 `output/spark-full-regression-r1`。下一次 pytest 必须用全新 --basetemp，避免 pytest 清理旧目录。
- 早期跨 Shell 复制曾误写到 WSL 根目录（如 `/manifest-p1000-r1.json`），已用现有脚本复制到正确的 F 盘 Stage I；根目录副本未清理，不作为权威入口。

### 项目讲解入口（纠正上一条答复）

已有独立综合讲解：[PROJECT_REVIEW_GUIDE.md](../PROJECT_REVIEW_GUIDE.md)。另有 [HTML 总览](html/01-overview.html)、[数据流](html/02-data-flow.html)、[对账](html/03-reconciliation.html)、[面试讲解](html/05-interview.html) 及 06–12 章专题。上一条答复声称“缺少单独项目说明文档”不准确，漏查了这些材料。

README、复习指南、HTML 和旧进度段落含过期状态；晚点应在现有材料上同步新证据，不要重复新建。本次只补入口提醒，不重写全部讲解，不改简历。下方旧阶段指标保留为背景，以本节最新状态和 Stage I/G/H 证据为准。

## 1. 用户目标与约束

- 继续项目开发，下一步是完整 Spark 三张 Synthea CSV → 质量治理 → 九张模型表，与既有 Python streaming 快照同口径对账，再推进新链路的 Airflow 编排。
- 简历已由用户接手，不再修改 `resume.tex` 或简历材料。
- **只在 `F:\project\medical-etl-fhir-platform` 工作，Windows Python 使用该目录 `.venv\Scripts\python.exe`。** 会话默认 cwd 可能仍是 D 盘，每次命令显式指定 F 盘 workdir。
- 复用万人归档与快照，不重新生成或下载 Synthea 数据；小型单元测试 fixture 与重生规模数据不同。
- 保留全部未提交修改，不 reset/checkout/clean，不擅自提交。工作区大量 modified/untracked 是现有成果。
- 不覆盖正式文件发布或 PostgreSQL `stage-c-verified`，不把试验运行发布到正式 current 指针。
- 新输出用独立目录/新 run_id；失败结果保留，同名目录拒绝覆盖；不得输出数据库密码。

## 2. 已完成的验证边界

| 部分 | 已有证据与边界 |
| --- | --- |
| Python streaming ETL | 万人归档 1,725,660 源行；11,476 患者、677,836 就诊、19,435 检查、19,435 序列、1,036,348 实例。九表处理约 392.618 秒，进程树采样 RSS 峰值 280.76 MiB。 |
| PostgreSQL/API | 万人加载约 327.112 秒；132 次串行 ASGI 接口对照。不是 HTTP 并发压测；历史提交后首次 OperationalError 根因尚未确认。 |
| FHIR | 万人 708,747 资源（Patient 11,476 / Encounter 677,836 / ImagingStudy 19,435）；约 253.326 秒、74.07 MiB。千人旧实现逐条等价、万人两次文件哈希一致。仅 R4B 摘要映射，不等于 FHIR Server / 官方 Validator / Bulk Data 验收。 |
| Spark（已完成部分） | `src/spark/synthea_compare.py` 从已有 DIM/DWD/桥表重算三张 DWS/ADS；千人/万人双向 exceptAll 零差异。万人约 25.843 秒、929.94 MiB。**不包含 CSV 清洗，不是完整 Spark ETL，也不满足 500 MiB 内存界限。** |
| Airflow（已完成部分） | `docs/stage-h/acceptance-p10000-airflow-r3.json` 为 SUCCESS。DagBag、dag.test、真实 SequentialExecutor scheduler 成功；verify_snapshot try=1、export_fhir try=2、compare_spark try=1；故障后复用 FHIR 成功产物，Windows/WSL FHIR 一致，正式文件发布未变。编排的是旧快照校验→FHIR→三表 Spark 对账。 |
| 回归历史 | 既有记录为 84 passed / 2 Spark skipped，另有真实 Spark 2 passed。**不是本轮新增规则的测试结果**。 |

旧记录入口：[500 MiB 交接](HANDOFF_MEMORY_500M.md)、[进度](PROGRESS.md)、[Stage G](stage-g/README.md)、[Stage H](stage-h/README.md)。旧文档存在重复历史段落和“WSL 未安装”等过时描述，以实际证据及本页边界为准。

## 3. 编辑工具注意

默认 D 盘 cwd 可能触发 Windows `os error 267`。所有命令显式设 F 盘 workdir。内置 apply_patch 本次仍因默认 cwd 失败；可在 F 盘 workdir 下直接调用本机 Codex 可执行文件的 `--codex-run-as-apply-patch`（路径可从 PATH 中 apply_patch.bat 读取），避免 bat 转发多行参数丢失。不得清理或恢复现有未提交文件来处理这个问题。

## 4. 现成输入、产物与运行环境

### Windows F 盘

- 源 ZIP：`data/generated/scale-v4-p10000/source.zip`。
- 万人 Python 基准：`output/scale-stream-final/runs/p10000-stream-local-r1`。
- 基准 source manifest SHA-256：`69a9f6652041c7475bca33fa81ece23d5430f8a912721b5d08853ce266b0a11c`。
- 基准 archive SHA-256：`cde7f394763863dae3123799a54fd21dbc90683c650ceb17fe7db85f79a28559`。
- 万人生日日期口径为 **+08:00**；事件仍标准化为 UTC。默认参考函数口径是 +00:00，不要用错。
- 千人基准：`output/scale-stream-final/runs/p1000-stream-utc-r2`、`p1000-stream-local-r1`；先核对各自 manifest。
- FHIR 已验收输出：`output/fhir/p10000-stream-r2`。
- Python `.venv` 已有 Pandas、PyArrow、PySpark 4.0.1 / Py4J 0.10.9.9；便携 JDK 21 位于 `output/runtime/jdk21/` 子目录。系统 Java 25 曾导致 Spark 失败，不能直接用。
- 生产文件 current：`output/synthea/current.json`。生产数据库 schema：`synthea_v1`；规模实验 schema：`synthea_scale_db_20260920`。
- PostgreSQL 本地端口 55432，数据库 medical_etl；连接文件 `output/local-postgres/connection.json` 不得打印。管理脚本 `scripts/local_postgres.py`。

### WSL / Airflow

- 已安装发行版 `MedicalETL-Airflow`，WSL2 / Ubuntu 24.04.5，Python 3.12.3、Airflow 2.11.2、JDK 21；无需重新安装。
- VHD：`output/runtime/wsl-airflow/ext4.vhdx`；WSL 程序 `C:\Program Files\WSL\wsl.exe`。
- Airflow venv：`/opt/medical-etl-airflow/runtime/airflow-venv`。
- worker venv：`/opt/medical-etl-airflow/runtime/worker-venv`，有 PySpark 4.0.1/FHIR；已新增 pandas 2.2.3、pyarrow 25.0.0、pytest 8.4.2。`scripts/airflow/requirements-worker.txt` 已追加前两项，Airflow venv 未改。
- 项目路径 `/opt/medical-etl-airflow/project` 为 `/mnt/f/project/medical-etl-fhir-platform` 的只读 bind mount，**WSL 重启会失效**，运行前核对挂载及只读属性。
- 将挂载恢复、只读核对和执行放在同一 WSL 会话；单独检查后退出，下一次可能已失去挂载。跨 PowerShell/bash 尽量不用插值变量，证据复制使用已有脚本的绝对路径。
- Linux 输出：`/opt/medical-etl-airflow/runs/`，不要尝试写入只读 project。
- NAT localhost 代理警告不曾阻止已验收作业；这不代表所有联网操作都正常。
- 验收脚本 `scripts/airflow/acceptance.py`；安装脚本 `bootstrap_linux.sh`；依赖 `requirements-worker.txt`；重试测试 DAG `retry_acceptance_dag.py`。
- r1 失败：导入正式 DAG 时重复注册 dag_id，已修复。
- r2：dag.test 成功但 scheduler 找不到 `airflow` 可执行文件，PATH 缺 bin；r3 修复后验收成功。
- 已有 session r1/r2/r3 均不可覆盖。验收脚本 exist_ok=False 是保护行为，不要删除旧目录来绕过。

## 5. 后续修改必须保留的 Python 口径

权威代码：`src/synthea_pipeline.py` 的 `REQUIRED`、`COLUMNS`、`KEYS`、`transform`、`reconcile`；大规模参考实现 `src/streaming_pipeline.py`。不要直接在 Spark worker 导入带顶层 pandas 依赖的模块而未处理依赖。

### 读入与逐行处置

1. 三张 CSV 是 patients、encounters、imaging_studies；全量原始字段保留字符串及空字符串。重复列名、缺 required、行列数不齐应失败。
2. source_row 是排除表头后的逻辑 CSV 记录序号，从 1 起；引号内换行不应变成多行。Spark 单纯 monotonically_increasing_id 不能替代该序号。
3. 业务键分别为 Id、Id、**INSTANCE_UID**；影像 Id 是检查标识，不能当实例唯一键。
4. 对同键的所有原始字段计算参考签名：`sha256(json.dumps(row, sort_keys=True, separators=(',', ':')).encode())`（默认 ensure_ascii=True）。任一字段不同，则同键所有行冲突隔离，包括前面相同的重复行。
5. reasons 顺序：missing_business_key → conflicting_business_key → validator reasons；有 reasons 为 quarantined，否则首次 accepted、后续 duplicate。seen 包括前面隔离行。duplicate reason 是 exact_duplicate。
6. 只有 accepted 的患者/就诊能作为有效父级；父子关系用 Spark join，而非 collect 全表到 Python 字典。
7. UTC 字符串、生日时区、日期格式接受范围及 reasons 顺序都要匹配参考；不要默认 Spark 时间转换行为与 datetime.fromisoformat 一样。

### 检查级隔离与 warnings

- 用**所有原始影像行**检测检查 Id 对 PATIENT/ENCOUNTER/DATE 的元数据冲突。
- SERIES_UID 是跨检查检测冲突：Id/MODALITY_CODE/BODYSITE_CODE/BODYSITE_DESCRIPTION 任一不同，涉及检查均隔离。
- 任一实例初步 quarantined，则整个检查所有行改 quarantined；原因 `sorted(set(old_reasons + ['incomplete_or_conflicting_study']))`，保留先前 exact_duplicate。
- 时间落在有效就诊区间外为 warning；可发生于重复行或最终隔离行，不应只从最终 accepted 实例生成；warnings 按多重集对账，不能 distinct 去重。

### 九表与对账

- dim_patient、dwd_encounter、dwd_imaging_study、dwd_imaging_series、dwd_imaging_instance、bridge_study_modality、dws_patient_imaging_summary、dws_imaging_daily_modality、ads_patient_imaging_profile。
- study/series 的代表行选最早 accepted source_row；实例保留原始 INSTANCE_UID/系列 UID；UUID5 假名键规则见 stable_key。
- 汇总保留零检查患者；daily 按 UTC started_at 前十字符及 modality 分组，桥表是 study×modality 去重，不按实例数计检查数。
- 基准 JSON 不含 run_id；基准 Parquet 首列带 run_id。新运行对账应比较模型字段，不能因运行 ID 不同误判或冒用基准 run_id。
- 必须验证双向逐行多重集差异、九表粒度与外键、源记录账平、accepted_lineage、零检查患者、检查/桥表计数守恒；仅总数一致不够。
- 同时对比 ODS 原始行、dispositions（包括 reasons）、warnings、source SHA 与出生日期口径。
- 输出 RUNNING/FAILED/SUCCESS manifest；捕获失败，保存失败目录；校验产物回读及哈希后才 SUCCESS，不写正式 current。

## 6. 建议下一会话执行顺序

1. 读本页与 Stage I，核对 git status。完整 Spark 千人/万人已通过，不要重复开发或为确认状态而重跑规模数据。
2. 在既有 WSL/JDK 21 补跑 `tests/test_spark_full_etl.py` 的 `study_metadata or series_metadata or encounter_conflict` 三组。设 `RUN_SPARK_TESTS=1`、`PYTHONDONTWRITEBYTECODE=1`、`SPARK_LOCAL_IP=127.0.0.1`、`PYSPARK_PYTHON=/opt/medical-etl-airflow/runtime/worker-venv/bin/python`；pytest 用 `-p no:cacheprovider` 和全新 Linux `--basetemp`，保存证据。
3. 为完整 Spark CLI 实现独立 Airflow task/DAG。参考 `src/snapshot_workflow.py`、`src/airflow_tasks.py` 和 Stage H，但旧 component 白名单仅 fhir/spark，旧产物校验只支持单层文件，不可直接套给目录型新产物。
4. 验证 run_id/try_number 隔离、FAILED 不覆盖、SUCCESS 哈希校验后复用、损坏产物明确失败。注入“产物成功后任务失败”，证明重试复用原成功产物。使用新验收会话，不覆盖 Stage H r3。
5. 分别验收 DagBag、dag.test、真实 scheduler。可先复用千人数据做编排可靠性验证，明确实际规模，不能据此声称万人调度通过。新格式的 FHIR/数据库消费需要另行适配验证。
6. 更新 Stage I、PROGRESS、交接与既有项目讲解。只有代码变化或未解决失败需要复验时才重跑规模任务。
7. 用户本次只要求交接，以上留待晚点继续；本次不启动新测试或编排。

## 7. 尚未闭环的交付事项

之前 `output/final-delivery-20260921` 是同盘文件汇集，不能声称异盘独立灾备；备份完整性与独立副本位置仍需审计。此项不是完整 Spark 已完成的证据。本轮仅更新交接，不拷贝 VHD、不改备份、更不改简历。

## 新会话可复制指令

请读取 `F:\project\medical-etl-fhir-platform\docs\HANDOFF_SPARK_FULL_ETL.md` 和 `docs/stage-i/README.md` 继续。完整 Spark 千人/万人九表、ODS、账本、warnings 已双向零差异，不要重复开发或无故重跑。先在既有 WSL/JDK 21 补跑 study_metadata、series_metadata、encounter_conflict 三组，再实现独立完整 Spark Airflow 编排和重试复用验收。只用 F 盘及既有归档，保留全部未提交修改和失败目录，不覆盖 stage-c-verified，不改简历。项目讲解从 PROJECT_REVIEW_GUIDE.md 与 docs/html/ 继续更新。

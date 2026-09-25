# 运行环境与操作说明

更新：2026-09-25。以下区分当前 Synthea 链路与旧 EMR 示例；具体验收结果见 [Stage M](stage-m/README.md)。

## 环境与依赖

| 环境 | 依赖入口 | 用途 |
| --- | --- | --- |
| Windows 项目 `.venv` | [requirements-core.lock.txt](../requirements-core.lock.txt) | Python ETL、PostgreSQL、FHIR、API 和核心测试 |
| WSL Spark/FHIR worker | [requirements-worker.txt](../scripts/airflow/requirements-worker.txt) | PySpark 4.0.1、JDK 21，完整 Spark ETL 与 FHIR 消费 |
| WSL Airflow 独立 venv | [bootstrap_linux.sh](../scripts/airflow/bootstrap_linux.sh) | Python 3.12、Airflow 2.11.2，独立约束安装 |
| 旧综合示例 | [requirements.txt](../requirements.txt) | 包含 PySpark 3.5.1 与宽范围 Airflow 依赖，不是当前验收环境锁文件 |

不要用 `requirements.txt` 覆盖已验证的核心或 worker 环境。当前 Spark 实测为 WSL `local[2]`，不是分布式集群；Stage M 显式配置 2 GiB JVM 堆。旧文档中的 Java 11/17 建议对应旧 PySpark 3.5 示例，不能套用于当前 worker。

本机项目实际目录为 `F:\project\medical-etl-fhir-platform`；旧 D: 路径不可用。其他机器从自己的项目根目录执行，不需复制此盘符。WSL 发行版为 `MedicalETL-Airflow`，运行环境位于 `/opt/medical-etl-airflow/runtime/`。这是已验收机器的布局，不是通用安装前提。

## Python 文件流程

小样本安装与运行见[首页](../README.md)。输入 ZIP 必须包含各一份 `patients.csv`、`encounters.csv`、`imaging_studies.csv`，必需字段见 [src/synthea_pipeline.py](../src/synthea_pipeline.py)。外部 ZIP 和 `output/` 不随 Git 分发。

大数据应使用独立流式入口，以下显式设置生日比较时区，以匹配已验收的 +08:00 口径：

```powershell
.\.venv\Scripts\python.exe -m src.streaming_pipeline --archive data/external/synthea_csv.zip --output-root output/quickstart-stream --birth-date-offset +08:00
```

CLI 的 `--birth-date-offset` 默认是 `+00:00`，不能省略参数后宣称复现了 +08:00 基准。事件时间及日统计仍按 UTC。省略 `--run-id` 自动生成新批次；成功会更新所选输出根的 `current.json`。不要覆盖已有批次，失败目录保留用于诊断。

## PostgreSQL 配置与发布

Synthea 连接优先级：

1. `SYNTHEA_DATABASE_URL`：存在且非空时直接使用。
2. `SYNTHEA_DB_CONFIG`：指定连接 JSON 路径。
3. 默认项目 `output/local-postgres/connection.json`。

配置字段由 psycopg2 接收，包括 `host`、`port`、`user`、`password`、`dbname`。不要提交或打印真实凭据。`POSTGRES_*` 变量属于旧 EMR 配置，对 Synthea 连接无效。

Windows 管理脚本默认 PostgreSQL 18 安装目录；初始化创建项目自己的数据目录、随机密码及 `127.0.0.1:55432` 实例，数据库为 `medical_etl`。其他二进制目录在每次管理命令中传入 `--pg-bin`；首次自定义端口使用 `--port`。

```powershell
# 第一次使用 init；以后电脑重启使用 start
.\.venv\Scripts\python.exe -m scripts.local_postgres init
.\.venv\Scripts\python.exe -m scripts.local_postgres start
.\.venv\Scripts\python.exe -m scripts.local_postgres status
# 使用结束后按需停止项目实例
.\.venv\Scripts\python.exe -m scripts.local_postgres stop
```

只有需要发布新数据时才执行首页的 `synthea_store --run-dir`，或 `run_etl.py --publish-db`（后者同时运行文件 ETL，默认输出 `output/synthea`）。数据库加载事务会切换当前 `synthea_v1` 批次；文件发布与数据库发布独立，数据库失败可加载已有成功快照重试。相同 run_id 和 manifest 重复加载返回 `ALREADY_LOADED`。

## Spark 与 Airflow

当前生产 DAG 为 [synthea_full_spark.py](../dags/synthea_full_spark.py)：

```text
verify_inputs → full_spark_etl → export_fhir
                              → load_database
```

完整链路验收入口为 [full_consumers_acceptance.py](../scripts/airflow/full_consumers_acceptance.py)。Stage J 的 `full_spark_acceptance.py` 针对旧两任务版本；Stage H 的 snapshot DAG 只消费已有快照，不能替代当前验收。

已有 WSL 环境的路径、只读挂载和独立 worker 调用方法见[历史环境说明](HANDOFF_SPARK_FULL_ETL.md)，使用时以[最新交接](HANDOFF_STAGE_M.md)修正进度。启动脚本会安装系统组件，仅在需要建立 Linux 环境时使用。Stage M 已通过，无需为阅读文档重复运行全量验收。

## 验证与排错

| 现象 | 检查方式 |
| --- | --- |
| 找不到 ZIP | 检查 `--archive` 路径；下载或生成数据不是 ETL 的隐式步骤 |
| 缺少 CSV / 字段 | 核对 ZIP 内唯一文件名与 `REQUIRED` 契约 |
| 找不到 connection.json | 首次执行本地数据库 init，或指定自己的 Synthea 连接配置 |
| 数据库连接失败 | 运行 `local_postgres status`，核对实际配置和端口 |
| API 返回旧批次 | `/api/synthea/status` 查询数据库指针；文件 ETL 本身不会更新数据库 |
| 批次目录已存在 | 使用新的 run_id 或输出根；不要删除失败现场来覆盖运行 |
| Spark / Airflow 导入失败 | 使用对应 WSL venv；Windows 核心锁文件不包含这些服务 |

首页提供不依赖外部数据的核心测试。数据库用例设置 `RUN_DATABASE_TESTS=1` 后才执行，测试使用隔离 schema。Spark 与真实调度验收按阶段运行手册执行；跳过用例不表示验收通过。

## 旧 EMR / DICOM 示例

旧入口 `python run_etl.py --legacy-emr` 会使用 [src/config.py](../src/config.py) 中的 `POSTGRES_HOST/PORT/USER/PASSWORD/DB`，并访问旧数据库模型。旧 Spark 入口为 `python -m src.spark.warehouse_job`，输入为 EMR CSV；它不是完整 Synthea CSV ETL。

旧 `/api/patient/...`、`/api/ads/patient-360/...` 和 `/api/fhir/patient/...` 仍保留在 [api/main.py](../api/main.py)，使用旧模型。当前 Synthea API、资源导出和证据应从首页进入。

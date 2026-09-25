# 医疗多源数据 ETL 与 FHIR 标准化平台复习文档

本文档用于项目复盘和面试前复习，重点说明项目背景、技术架构、核心流程、接口参数、数仓建模、开发中遇到的问题以及可用于回答面试问题的表达方式。

## 1. 项目定位

本项目是一个面向医疗多源数据的数据工程平台原型，主要处理两类数据：

- EMR 电子病历 CSV 数据：包含患者 ID、姓名、性别、出生日期、检查类型、检查日期。
- DICOM 医学影像元数据：从 `.dcm` 文件中提取患者、检查、设备、影像尺寸等元数据。

项目目标不是做一个单纯 Python 脚本，而是覆盖数据开发岗位常见能力：

- 数据接入：CSV、DICOM。
- 数据清洗：字段标准化、日期格式统一、空值处理。
- 隐私处理：患者 ID 哈希脱敏、姓名替换、出生日期泛化为出生年份。
- FHIR 标准化：输出 FHIR R4 Patient、Observation 资源。
- 数仓建模：ODS、DIM、DWD、DWS、ADS 分层。
- 分布式处理：使用 PySpark 构建 Parquet 数仓层。
- 调度编排：Airflow DAG 编排 ETL、质量检查、Spark 数仓任务。
- 数据服务：FastAPI 查询患者、检查记录、Patient 360 和 FHIR 资源。
- 数据质量：非空、唯一性、枚举值、日期合法性校验。

## 2. 技术栈

| 模块 | 技术 |
| --- | --- |
| 开发语言 | Python |
| 单机 ETL | Pandas |
| 分布式处理 | PySpark |
| 医学影像解析 | pydicom |
| 医疗数据标准 | fhir.resources, FHIR R4 |
| 数据库存储 | PostgreSQL |
| 数仓文件格式 | Parquet |
| 调度编排 | Apache Airflow |
| API 服务 | FastAPI, Uvicorn |
| 测试 | pytest |

## 3. 项目目录说明

```text
api/                 FastAPI 接口服务
dags/                Airflow DAG
data/                样例源数据
docs/                数仓、质量、运行环境文档
sql/                 数仓建表 SQL
src/
  data_access/       数据接入层
  processing/        清洗和脱敏层
  quality/           数据质量规则
  spark/             PySpark 数仓构建任务
  fhir/              FHIR 资源转换
  database/          PostgreSQL 连接与写入
tests/               单元测试
```

## 4. 整体数据流程

```text
EMR CSV / DICOM
  -> 数据接入
  -> 字段清洗与日期标准化
  -> 数据质量校验
  -> PHI 脱敏
  -> PostgreSQL ODS / DIM / DWD
  -> FHIR JSON 输出
  -> FastAPI 查询服务

同时：

EMR CSV
  -> PySpark
  -> ODS Parquet
  -> DIM Patient
  -> DWD Exam Detail
  -> DWS Patient Exam Summary
  -> ADS Patient 360 View
```

## 5. 核心模块说明

### 5.1 数据接入

相关文件：

- `src/data_access/emr_loader.py`
- `src/data_access/dicom_reader.py`

EMR 使用 `pandas.read_csv` 读取 CSV。DICOM 使用 `pydicom.dcmread(..., stop_before_pixels=True)` 读取元数据，避免读取大体积像素数据。

开发中修复过一个问题：原先 `dicom_reader.py` 在模块导入时就检查 `data/dicom` 是否存在，如果目录不存在，任何导入该模块的代码都会失败。后来改成在函数内部判断，如果目录不存在则返回空 DataFrame，这样 Airflow、测试、API 导入都不会被空目录阻塞。

### 5.2 数据清洗

相关文件：

- `src/processing/cleaner.py`

主要逻辑：

- `PatientID` 统一重命名为 `patient_id`。
- `study_date` 从 `yyyyMMdd` 转成 `yyyy-MM-dd`。
- `birth_date` 标准化为 `yyyy-MM-dd`。
- `patient_id` 去除前后空格。
- DICOM 中 `body_part`、`modality`、`institution` 空值填充为 `UNKNOWN`。
- EMR 中 `gender`、`exam_type` 空值填充为 `UNKNOWN`。

面试表达：

> 我把清洗逻辑放在独立 processing 层，避免接入、转换、入库逻辑混在一起。日期统一后，后续质量校验、数仓分区、统计聚合都能使用稳定格式。

### 5.3 PHI 脱敏

相关文件：

- `src/processing/anonymizer.py`

主要逻辑：

- 患者 ID 使用 SHA-256 做确定性哈希，保留前 10 位，生成 `PID-XXXXXXXXXX`。
- 患者姓名替换为 `Patient_XXXX`。
- 出生日期删除，只保留 `birth_year`。
- 保留 `original_patient_id` 作为内部调试字段，真实生产环境应进入更严格的权限域或直接不落库。

开发中遇到的问题：

原先脱敏 ID 使用 `PID_...`，FHIR 的 `id` 字段不允许下划线，会触发 FHIR Pydantic 校验错误：

```text
String should match pattern '^[A-Za-z0-9\\-.]+$'
```

修复方式是把 ID 改成 `PID-...`，符合 FHIR id 规范。

面试表达：

> 我没有使用随机脱敏，而是使用确定性哈希。这样同一个患者在不同批次中会得到相同脱敏 ID，便于跨表关联和增量处理，同时避免直接暴露原始 ID。

### 5.4 FHIR 转换

相关文件：

- `src/fhir/converter.py`

支持资源：

- Patient
- Observation

Patient 字段映射：

| 源字段 | FHIR 字段 |
| --- | --- |
| `patient_id` | `Patient.id` |
| `gender` | `Patient.gender` |
| 固定值 | `Patient.active = true` |

Observation 字段映射：

| 源字段 | FHIR 字段 |
| --- | --- |
| `obs_id` 或 `patient_id` | `Observation.id` |
| `exam_type` | `Observation.code.text` |
| `patient_id` | `Observation.subject.reference` |
| `study_date` | `Observation.effectiveDateTime` |

面试表达：

> FHIR 转换层的意义是把内部数仓模型和医疗行业交换标准解耦。内部可以按数仓方式建模，对外可以按 FHIR Patient、Observation 等标准资源输出。

## 6. 数仓建模

相关文件：

- `docs/warehouse_model.md`
- `sql/01_ods_tables.sql`
- `sql/02_dim_tables.sql`
- `sql/03_dwd_tables.sql`
- `sql/04_dws_ads_tables.sql`

### 6.1 ODS 层

作用：保留源数据形态，做少量标准化，便于追溯。

表：

- `ods_emr_raw`
- `ods_dicom_metadata`

### 6.2 DIM 层

作用：沉淀公共维度。

表：

- `dim_patient`
- `dim_exam_type`
- `dim_date`

当前代码重点实现 `dim_patient`。SQL 中预留了 `dim_exam_type` 和 `dim_date`，方便后续扩展。

### 6.3 DWD 层

作用：清洗后的业务明细层，保持明细粒度。

表：

- `dwd_exam_record_detail`
- `dwd_fhir_observation_detail`

### 6.4 DWS 层

作用：公共汇总层，面向可复用指标。

表：

- `dws_patient_exam_summary`
- `dws_exam_type_daily_stats`

### 6.5 ADS 层

作用：面向 API、报表、看板等应用场景。

表：

- `ads_patient_360_view`

面试表达：

> 这个项目不是只把数据清洗后直接入库，而是按 ODS、DIM、DWD、DWS、ADS 做分层。ODS 保留来源，DWD 保留明细，DWS 做通用汇总，ADS 服务具体查询场景，这样后续扩展 API 或看板时不会反复扫描明细表。

## 7. Spark 处理链路

相关文件：

- `src/spark/warehouse_job.py`

执行命令：

```bash
python -m src.spark.warehouse_job --emr-csv data/emr/patients.csv --warehouse-path output/warehouse --batch-date 2026-07-06
```

Spark 作业逻辑：

1. 显式定义 EMR Schema，避免自动推断导致字段类型不稳定。
2. 读取 CSV。
3. 添加 `batch_date` 和 `etl_time`。
4. 写入 `ods/ods_emr_raw`，按 `batch_date` 分区。
5. 构建 `dim/dim_patient`。
6. 构建 `dwd/dwd_exam_record_detail`。
7. 汇总生成 `dws/dws_patient_exam_summary`。
8. 关联维表和汇总层，生成 `ads/ads_patient_360_view`。

开发中遇到的问题：

本机 Java 版本是 Java 25，Spark/Hadoop 在该版本下初始化文件系统时报错：

```text
UnsupportedOperationException: getSubject is not supported
```

解决建议：

- 使用 Java 11 或 Java 17。
- 设置 `JAVA_HOME` 指向兼容 JDK。
- Windows 环境可补充 Hadoop `winutils.exe`，否则可能出现本地 Hadoop 警告。

面试表达：

> Spark 这部分主要体现批处理数仓建设能力。相比 Pandas 单机链路，Spark 链路使用显式 Schema、批次字段、Parquet 分层和分区写入，更接近真实离线数仓处理方式。

## 8. 数据质量设计

相关文件：

- `src/quality/rules.py`
- `src/quality/report.py`
- `docs/data_quality.md`

规则：

| 规则 | 字段 | 目的 |
| --- | --- | --- |
| 非空校验 | `patient_id`, `gender`, `study_date` | 关键字段完整性 |
| 唯一性校验 | `patient_id`, `study_date`, `exam_type` | 检查重复检查记录 |
| 枚举值校验 | `gender` | 保证字段值域合法 |
| 日期可解析校验 | `study_date` | 保证日期格式可被下游使用 |

质量报告输出目录：

```text
output/quality/
```

面试表达：

> 数据质量不是最后才看的，而是在清洗后、入库前执行。这样可以尽早发现问题，并把质量结果持久化，方便排查批次数据异常。

## 9. Airflow 调度

相关文件：

- `dags/medical_etl_dag.py`
- `dags/medical_etl_layered.py`

核心 DAG：

```text
init_postgres_tables
  -> pandas_etl.extract_raw_sources
  -> pandas_etl.clean_anonymize_and_quality_check
  -> pandas_etl.load_ods_layer
  -> pandas_etl.load_dim_dwd_and_export_fhir
  -> build_spark_parquet_warehouse
```

Airflow 配置点：

- `retries = 2`
- `retry_delay = 2 minutes`
- `catchup = False`
- 每天凌晨 2 点调度：`0 2 * * *`
- 使用 TaskGroup 组织 Pandas ETL 子流程。

面试表达：

> Airflow 中我把任务拆成接入、清洗脱敏和质量校验、ODS 入库、DWD/FHIR 输出、Spark 数仓构建几个节点，而不是一个大函数跑到底。这样失败时可以定位到具体阶段，也便于后续扩展重跑和监控。

## 10. API 接口参数

相关文件：

- `api/main.py`

### 10.1 查询患者基础信息

```http
GET /api/patient/{patient_id}
```

路径参数：

| 参数 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `patient_id` | string | 是 | 脱敏后的患者 ID，例如 `PID-1A2B3C4D5E` |

查询表：

```text
dim_patient
```

返回示例：

```json
{
  "patient_id": "PID-1A2B3C4D5E",
  "patient_name": "Patient_4D5E",
  "gender": "M",
  "birth_year": 1990
}
```

异常：

- 404：患者不存在。

### 10.2 查询患者检查记录

```http
GET /api/patient/{patient_id}/observations
```

路径参数：

| 参数 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `patient_id` | string | 是 | 脱敏后的患者 ID |

查询表：

```text
dwd_exam_record_detail
```

返回示例：

```json
{
  "patient_id": "PID-1A2B3C4D5E",
  "total": 1,
  "observations": [
    {
      "exam_record_id": 1,
      "study_date": "2024-01-10",
      "exam_type": "Chest CT",
      "body_part": "UNKNOWN"
    }
  ]
}
```

### 10.3 查询 Patient 360 应用层视图

```http
GET /api/ads/patient-360/{patient_id}
```

路径参数：

| 参数 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `patient_id` | string | 是 | 脱敏后的患者 ID |

查询表：

```text
ads_patient_360_view
```

返回示例：

```json
{
  "patient_id": "PID-1A2B3C4D5E",
  "patient_name": "Patient_4D5E",
  "gender": "M",
  "birth_year": 1990,
  "exam_count": 3,
  "latest_exam_date": "2024-03-20"
}
```

异常：

- 404：Patient 360 视图不存在。

### 10.4 查询 FHIR Patient 资源

```http
GET /api/fhir/patient/{patient_id}
```

路径参数：

| 参数 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `patient_id` | string | 是 | 脱敏后的患者 ID |

返回示例：

```json
{
  "resourceType": "Patient",
  "id": "PID-1A2B3C4D5E",
  "active": true,
  "gender": "male"
}
```

## 11. 测试情况

当前已有测试：

- `tests/test_processing.py`
- `tests/test_quality.py`
- `tests/test_fhir.py`

测试覆盖点：

- 日期标准化。
- 患者 ID 脱敏。
- 出生日期泛化为出生年份。
- 数据质量规则 PASS/FAIL。
- FHIR Patient gender 映射。

已执行命令：

```bash
python -m compileall src api dags tests
python -m pytest -q
```

结果：

```text
5 passed
```

## 12. 开发过程问题复盘

### 问题 1：README 和代码注释乱码

现象：

- 原始 README、注释和接口描述出现中文乱码。

原因：

- 文件编码或编辑器读取编码不一致。

解决：

- 统一重写为 UTF-8 文本。
- README 改为英文工程说明，内部复习文档使用中文。

复盘：

> 对开源项目或简历项目来说，README 是第一印象。乱码会让项目看起来不稳定，即使代码能跑也会影响可信度。

### 问题 2：DICOM 空目录导致导入失败

现象：

- `data/dicom` 不存在时，导入 `dicom_reader.py` 就报错。

原因：

- 目录检查写在模块顶层，导入阶段执行。

解决：

- 把目录检查放进 `extract_dicom_metadata` 函数。
- 目录不存在时返回空 DataFrame。

复盘：

> 模块导入阶段应尽量避免执行依赖外部环境的逻辑，否则 Airflow、测试、API 启动都可能被无关资源阻塞。

### 问题 3：FHIR id 不兼容

现象：

- `PID_001` 这类 ID 触发 FHIR 校验失败。

原因：

- FHIR id 只允许字母、数字、短横线和点号等字符，不允许下划线。

解决：

- 脱敏 ID 统一改为 `PID-...`。

复盘：

> 使用行业标准库时，不要只拼 JSON，要让标准库做校验。校验暴露的问题往往是真实对接时会遇到的问题。

### 问题 4：Spark 本地运行受 Java 版本影响

现象：

- PySpark 安装后，启动 Spark 读 CSV 时报 `getSubject is not supported`。

原因：

- 本机 Java 25 与 Spark/Hadoop 运行时不兼容。

解决：

- 文档中明确建议 Java 11 或 Java 17。
- `requirements.txt` 固定 `pyspark==3.5.1`，减少环境漂移。

复盘：

> 大数据项目不只考代码，还考运行环境。Spark、Hadoop、Java 的版本兼容性需要在文档里明确写出来。

### 问题 5：原项目数仓层次不足

现象：

- 初始版本只有简单 ODS/DWD 表，更像 ETL demo。

解决：

- 增加 SQL 和文档中的 ODS、DIM、DWD、DWS、ADS 分层。
- Spark 作业落地 Parquet 分层。
- API 增加 ADS Patient 360 查询。

复盘：

> 数据开发项目需要能讲清楚“数据从哪里来、经过哪些层、每层解决什么问题、最后服务什么场景”。

## 13. 面试常见问题回答

### Q1：这个项目解决了什么问题？

可以回答：

> 这个项目模拟医疗场景下多源异构数据的处理流程。它把 EMR CSV 和 DICOM 元数据接入后，经过清洗、脱敏、质量校验、FHIR 标准化和数仓分层，最终通过 API 提供患者信息、检查记录和 Patient 360 查询能力。

### Q2：为什么要做数仓分层？

可以回答：

> 分层可以降低耦合。ODS 负责保留原始数据，DWD 负责稳定明细，DWS 负责公共汇总，ADS 面向具体应用。这样新增报表或接口时，不需要反复从原始数据开始加工。

### Q3：为什么引入 Spark？

可以回答：

> Pandas 适合小规模原型，但数据开发岗位更常见的是离线批处理。Spark 可以处理更大规模数据，并且支持分区、Parquet、SQL 转换和集群执行，所以我用 PySpark 构建了一条数仓分层链路。

### Q4：脱敏是怎么做的？

可以回答：

> 患者 ID 使用 SHA-256 做确定性哈希，姓名替换为匿名名称，出生日期只保留年份。确定性哈希可以保证同一个患者在不同表和不同批次中保持同一个脱敏 ID，便于关联分析。

### Q5：数据质量怎么保证？

可以回答：

> 项目在清洗后执行质量校验，包括关键字段非空、患者检查记录唯一性、性别枚举值、检查日期可解析性。结果会写入质量报告目录，后续可以扩展成失败阻断或告警。

### Q6：Airflow 在项目里做了什么？

可以回答：

> Airflow 负责任务编排，把流程拆成初始化表、数据接入、清洗脱敏和质量校验、ODS 入库、DWD/FHIR 输出、Spark 数仓构建。每个任务可以独立重试和定位问题。

### Q7：FHIR 是什么，为什么用它？

可以回答：

> FHIR 是医疗数据交换标准。项目内部按数仓模型存储数据，对外可以转换成 FHIR Patient 和 Observation 资源，便于和医疗系统或标准化接口对接。

### Q8：项目还有哪些可扩展点？

可以回答：

> 可以继续扩展 HL7 v2 接入、更多 FHIR 资源如 Encounter 和 DiagnosticReport、SCD2 患者维表、Great Expectations 数据质量框架、Docker Compose 一键部署、以及基于 Superset 或 BI 的数据看板。

## 14. 当前项目不足与后续优化

当前不足：

- 样例数据量较小，主要用于展示流程。
- PostgreSQL 侧 DWS/ADS 聚合目前以表结构和 API 为主，Spark 侧 Parquet 分层更完整。
- Spark 本地运行依赖 Java 版本，Windows 下还可能需要 Hadoop 工具。
- DICOM 当前主要抽取元数据，没有处理影像像素或复杂检查报告。

后续优化：

- 增加 Docker Compose，整合 PostgreSQL、Airflow、FastAPI。
- 增加更大规模模拟数据生成脚本。
- 增加 SCD2 患者维表。
- 增加 DICOM 与 EMR 的关联逻辑。
- 增加 DiagnosticReport、Encounter 等 FHIR 资源。
- 增加 API 鉴权、限流和审计日志。

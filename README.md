# 医疗多源数据ETL与FHIR标准化处理平台

# 医疗多源数据 ETL 与 FHIR 标准化处理平台

面向医疗领域异构数据的一站式 ETL 解决方案，聚焦 DICOM 影像元数据与电子病历（EMR）数据的全生命周期处理，涵盖**数据接入、清洗标准化、PHI 敏感信息脱敏、FHIR R4 标准转换** 及数仓分层存储，并提供 RESTful API 对外赋能数据查询能力。本项目采用数仓分层设计理念，可作为医疗数据中台的核心原型系统。

## 核心特性

|能力维度|具体能力|
|---|---|
|多源异构数据接入|批量解析 DICOM 影像元数据（患者 / 检查 / 设备等）、加载 CSV 格式电子病历并兼容字段映射|
|数据清洗标准化|异构字段统一映射、多格式日期规范化、空值 / 异常值容错处理|
|PHI 敏感信息脱敏|患者姓名匿名化、ID 掩码处理、出生日期仅保留年份（模糊化）|
|FHIR 标准化转换|支持 Patient/ Observation 等 FHIR R4 核心医疗资源标准化输出|
|数仓分层存储|ODS 原始层（全量清洗后元数据）、DWD 明细层（脱敏后患者 / 检查主表）|
|标准化 API 服务|提供患者信息 / 检查记录 / FHIR 格式数据查询接口，配套自动生成的接口文档|

## 技术栈

|技术类别|核心组件|
|---|---|
|开发语言|Python 3\.9\+|
|数据处理|Pandas|
|影像解析|pydicom|
|FHIR 标准化|fhir\.resources（FHIR R4）|
|数据存储|PostgreSQL 12\+（ODS/DWD 分层）|
|接口服务|FastAPI \+ Uvicorn|
|架构模式|分层 ETL 流水线设计（接入→处理→转换→存储→服务）|

## 项目结构

```tree
medical-etl-fhir-platform/
├── data/                  # 原始数据目录
│   ├── dicom/             # DICOM影像文件存储
│   └── emr/               # 电子病历CSV文件存储
├── src/                   # 核心业务代码层
│   ├── config.py          # 全局配置（数据库/路径等）
│   ├── data_access/       # 数据接入层
│   │   ├── dicom_reader.py # DICOM元数据提取
│   │   └── emr_loader.py  # 电子病历数据加载
│   ├── processing/        # 数据处理层
│   │   ├── cleaner.py     # 数据清洗与标准化
│   │   └── anonymizer.py  # PHI敏感信息脱敏
│   ├── fhir/              # FHIR标准化层
│   │   └── converter.py   # FHIR R4资源转换
│   ├── database/          # 数据存储层
│   │   └── db_manager.py  # 数据库连接/读写操作
│   └── pipeline.py        # ETL全流程调度
├── api/                   # 接口服务层
│   └── main.py            # FastAPI接口实现
├── output/                # 输出文件目录
│   └── fhir/              # FHIR格式文件输出
├── run_etl.py             # ETL流程执行入口
├── requirements.txt       # 项目依赖清单
└── README.md              # 项目说明文档
```

## 快速开始

### 1\. 环境准备

- 基础环境：Python 3\.9\+、PostgreSQL 12\+

- 依赖工具：pip（Python 包管理）、git（可选）

### 2\. 安装依赖

```bash
# 克隆项目（可选）
git clone <项目仓库地址>
cd medical-etl-fhir-platform

# 安装依赖包
pip install -r requirements.txt
```

### 3\. 数据库配置

修改 `src/config.py` 中的数据库连接参数，配置项示例：

```python
# src/config.py 数据库配置段
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "user": "postgres",
    "password": "your_password",
    "db_name": "medical_etl_db"
}
```

### 4\. 执行 ETL 全流程

```bash
# 执行数据接入→清洗→脱敏→FHIR转换→入库
python run_etl.py
```

执行完成后，数据会按 ODS/DWD 分层写入 PostgreSQL 数据库。

### 5\. 启动 API 服务

```bash
# 启动FastAPI服务（热重载模式）
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

- 接口文档（Swagger）：[http://127\.0\.0\.1:8000/docs](http://127.0.0.1:8000/docs)

- 接口调试：可直接在文档页面对接测试各查询接口

## 数据处理流程

```mermaid
flowchart TD
    A[原始异构数据] --> A1[DICOM影像文件]
    A --> A2[EMR电子病历CSV]
    A1 --> B[数据接入层]
    A2 --> B[数据接入层]
    B --> C[数据处理层<br/>清洗标准化+PHI脱敏]
    C --> D[FHIR标准化层<br/>FHIR R4资源转换]
    D --> E[数仓分层存储<br/>ODS原始层 + DWD明细层]
    E --> F[API服务层<br/>对外提供查询能力]```

## 扩展规划

1. **调度能力升级**：集成 Apache Airflow，实现 ETL 任务定时调度、失败重试、监控告警

2. **性能优化**：适配 Spark 分布式框架，支持 TB 级医疗大数据处理

3. **脱敏合规升级**：引入 k \- 匿名、差分隐私等算法，满足等保 2\.0 / 医疗数据合规要求

4. **FHIR 资源扩展**：新增 DiagnosticReport、Medication、Encounter 等医疗资源转换

5. **可视化能力**：配套数据看板，实现患者数据 / 检查量 / 数据质量的可视化展示

6. **多源扩展**：支持 HL7 v2、JSON 格式电子病历、医疗设备日志等更多数据源接入

## 注意事项

1. 敏感数据处理：本项目仅提供基础脱敏能力，生产环境需结合医疗行业合规要求补充校验

2. 数据库性能：大数据量场景下建议为 PostgreSQL 配置分区表、索引优化

3. 接口安全：生产环境需为 API 服务添加认证（如 JWT）、限流、日志审计等安全策略

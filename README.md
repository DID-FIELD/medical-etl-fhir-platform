# 医疗多源数据ETL与FHIR标准化处理平台

\# 医疗多源数据ETL与FHIR标准化处理平台

\#\# 项目简介
面向医疗异构数据的一站式数据处理流水线，实现DICOM影像元数据与电子病历（EMR）数据的统一接入、清洗标准化、PHI敏感信息脱敏、FHIR R4标准格式转换及数仓分层存储，并提供RESTful API对外提供数据查询服务。

项目采用数仓分层设计思想，覆盖数据接入、处理、存储、服务全链路，可作为医疗数据中台的核心原型。

\#\# 技术栈
\- \*\*开发语言\*\*：Python 3\.9\+
\- \*\*数据处理\*\*：Pandas
\- \*\*影像解析\*\*：pydicom
\- \*\*数据标准化\*\*：FHIR R4（fhir\.resources）
\- \*\*数据存储\*\*：PostgreSQL（ODS原始层 \+ DWD明细层）
\- \*\*接口服务\*\*：FastAPI \+ Uvicorn
\- \*\*架构模式\*\*：分层ETL流水线设计

\#\# 核心功能
1\. \*\*多源异构数据接入\*\*
   \- 批量读取DICOM文件，提取患者、检查、设备等核心元数据
   \- 加载CSV格式电子病历数据，支持字段映射与格式兼容
2\. \*\*数据清洗与标准化\*\*
   \- 异构字段名统一映射
   \- 多格式日期统一规范化
   \- 空值、异常值容错处理
3\. \*\*PHI敏感信息脱敏\*\*
   \- 患者真实姓名匿名化替换
   \- 患者ID掩码处理
   \- 出生日期模糊化（仅保留年份）
4\. \*\*FHIR国际标准转换\*\*
   \- 支持Patient患者资源标准化输出
   \- 支持Observation检查资源扩展
5\. \*\*数仓分层存储\*\*
   \- ODS层：存储原始清洗后的全量元数据
   \- DWD层：存储脱敏后的患者主表与检查明细表
6\. \*\*RESTful API服务\*\*
   \- 患者基础信息查询接口
   \- 患者检查记录列表查询接口
   \- FHIR标准格式数据查询接口

\#\# 项目结构

```Plain Text
medical-etl-fhir-platform/
├── data/                   # 原始数据目录
│   ├── dicom/              # DICOM 影像文件
│   └── emr/                # 电子病历 CSV 文件
├── src/                    # 核心业务代码
│   ├── [config.py](config.py)           # 全局配置管理
│   ├── data_access/        # 数据接入层
│   │   ├── dicom\[_reader.py](_reader.py) # DICOM 元数据提取
│   │   └── emr\[_loader.py](_loader.py)   # 电子病历数据加载
│   ├── processing/         # 数据处理层
│   │   ├── [cleaner.py](cleaner.py)      # 数据清洗与标准化
│   │   └── [anonymizer.py](anonymizer.py)   # 敏感信息脱敏
│   ├── fhir/               # FHIR 标准化层
│   │   └── [converter.py](converter.py)    # FHIR 资源转换与输出
│   ├── database/           # 数据存储层
│   │   └── db\[_manager.py](_manager.py)   # 数据库连接与操作
│   └── [pipeline.py](pipeline.py)         # ETL 全流程调度
├── api/                    # 接口服务层
│   └── [main.py](main.py)             # FastAPI 接口实现
├── output/                 # 输出文件目录
│   └── fhir/               # FHIR 格式输出文件
├── run\[_etl.py](_etl.py)              # ETL 流程执行入口
├── requirements.txt        # 项目依赖清单
└── [README.md](README.md)               # 项目说明文档
```

\#\# 快速开始
\#\#\# 1\. 环境准备
\- Python 3\.9 及以上版本
\- PostgreSQL 12 及以上版本

\#\#\# 2\. 安装依赖
\`\`\`bash
pip install \-r requirements\.txt

### 3\. 配置数据库

修改 `src/config.py` 中的数据库连接信息，配置本地 PostgreSQL 地址、账号密码及数据库名。

### 4\. 执行 ETL 全流程

```bash
python run_etl.py
```

执行完成后，数据将完成清洗、脱敏并分层写入数据库。

### 5\. 启动 API 服务

```bash
uvicorn api.main:app --reload
```

- 接口文档地址：[http://127\.0\.0\.1:8000/docs](http://127.0.0.1:8000/docs)

- 接口支持在线调试与参数测试

## 数据架构设计

```Plain Text
原始异构数据
    ↓
数据接入层（DICOM解析 + EMR加载）
    ↓
数据处理层（清洗标准化 + 敏感信息脱敏）
    ↓
FHIR标准化转换
    ↓
数仓分层存储（ODS原始层 / DWD明细层）
    ↓
API服务层（对外提供查询能力）
```

## 扩展方向

1. **调度升级**：接入 Apache Airflow 实现定时调度、任务监控与失败重试

2. **性能优化**：大数据量场景下适配 Spark 分布式处理框架

3. **脱敏升级**：引入 k \- 匿名、差分隐私等高级脱敏算法，满足等保合规要求

4. **FHIR 扩展**：新增 DiagnosticReport、Medication、Encounter 等更多医疗资源

5. **前端可视化**：配套数据看板，实现患者数据、检查量的统计展示



# 文档导航

更新：2026-09-25。当前主线为 Synthea → 完整 Spark ETL → FHIR/PostgreSQL → API，Stage M 万人完整链路已验收。阶段报告按历史时间保留，不把早期“下一步”作为当前待办。

## 按用途阅读

| 目标 | 入口 |
| --- | --- |
| 了解项目并运行小样本 | [项目首页](../README.md) |
| 了解运行环境、依赖与配置 | [runtime.md](runtime.md) |
| 理解九表粒度、来源和发布 | [warehouse_model.md](warehouse_model.md) |
| 理解重复、冲突、级联隔离与对账 | [data_quality.md](data_quality.md) |
| 复习项目、解释设计与实测边界 | [项目复习指南](../PROJECT_REVIEW_GUIDE.md) |
| 系统学习代码与数据流 | [15 章离线教材](html/01-overview.html) |
| 继续当前工作 | [Stage M 最新交接](HANDOFF_STAGE_M.md) |
| 查看进度与上下文 | [PROGRESS](PROGRESS.md)、[TASK_MEMORY](TASK_MEMORY.md) |

## 阶段证据索引

| 阶段 | 范围 |
| --- | --- |
| [B 模型](stage-b/DATA_MODEL.md) / [字段字典](stage-b/FIELD_DICTIONARY.md) | 早期设计；物理实现以当前模型说明与代码为准 |
| [C](stage-c/README.md) | 108 位患者小样本、文件与数据库发布、API |
| [C FHIR](stage-c-fhir/README.md) | Synthea FHIR 映射 |
| [D](stage-d/README.md) | 异常与恢复测试 |
| [E](stage-e/README.md) / [流式验收](stage-e/STREAMING.md) | 规模测试与 Python 内存边界 |
| [F](stage-f/README.md) | Python 万人数据库加载与串行 API |
| [G](stage-g/README.md) | FHIR 流式导出与 Spark 三张汇总表 |
| [H](stage-h/README.md) | 旧快照校验 → FHIR → Spark 汇总的 Airflow 链路 |
| [I](stage-i/README.md) | 从 CSV 开始的完整 Spark 清洗与九表对账 |
| [J](stage-j/README.md) | 完整 Spark 两任务 DAG 与重试 |
| [K](stage-k/README.md) | Spark Parquet 目录接入 FHIR / PostgreSQL |
| [L](stage-l/README.md) | 千人四任务 DAG 与双消费者重试 |
| [M](stage-m/README.md) | 万人四任务 DAG、失败诊断、r6 成功及独立验据 |

Stage G 的汇总作业、Stage H 的旧快照 DAG 与 Stage I—M 的完整 CSV ETL 范围不同，不能合并表述。时间与内存指标须保留对应阶段、数据集和测量范围。

## 文档分工

- 根 README：项目定位、最新结果摘要、快速开始和结构导航。
- 模型、质量、运行说明：当前实现的长期说明，修改行为时同步维护。
- `stage-*`：阶段报告与验收证据，保留原始结果及失败记录。
- `HANDOFF_STAGE_M.md`：当前交接；较早交接只作环境和历史参考。
- `history/`：历史快照；`SYNTHEA_PROJECT_PLAN.md` 为规划背景，不是当前完成清单。
- `html/chapters/`：教材正文源文件；按 [HTML README](html/README.md) 构建生成页面。

更新说明时先核对代码、CLI 与阶段证据。新实测应记录独立运行范围，不累计重叠测试数、不改写历史失败为成功；本地数据、凭据及完整运行产物不放入文档归档。

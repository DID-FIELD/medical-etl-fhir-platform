# 数仓模型与来源追溯

更新：2026-09-25。本文描述当前 Synthea 九表模型；字段和业务键以 [COLUMNS / KEYS](../src/synthea_pipeline.py) 为准，PostgreSQL 约束以 [synthea_store.py](../src/database/synthea_store.py) 为准。早期 [B 阶段设计](stage-b/DATA_MODEL.md) 是设计背景。

## 分层与粒度

| 层 | 表 | 一行代表 / 业务键 |
| --- | --- | --- |
| ODS | patients / encounters / imaging_studies | 一条源逻辑记录；保留全部源字段及来源信息 |
| DIM | `dim_patient` | 一位患者；`patient_key` |
| DWD | `dwd_encounter` | 一次就诊；`encounter_key` |
| DWD | `dwd_imaging_study` | 一项影像检查；`study_key` |
| DWD | `dwd_imaging_series` | 检查内一个序列；`study_key, series_uid` |
| DWD | `dwd_imaging_instance` | 一个影像实例；`instance_uid` |
| 关系 | `bridge_study_modality` | 检查与模态的去重关系；`study_key, modality_code` |
| DWS | `dws_patient_imaging_summary` | 一位患者的检查汇总；`patient_key` |
| DWS | `dws_imaging_daily_modality` | UTC 日期 × 模态；`stat_date, modality_code` |
| ADS | `ads_patient_imaging_profile` | 一位患者的概览；`patient_key` |

九张业务表不含 ODS、处置账本及批次管理表。PostgreSQL 将三张逻辑 ODS 合并存储于 `source_records`，通过 `source_table` 区分，原始字段存为 JSONB；并不是丢弃两张源表。`row_dispositions` 保留每条源记录的最终处置与原因，`pipeline_runs` 保存 manifest，`current_snapshot` 选择当前发布批次。

## 关系与统计口径

```text
patient → encounter → imaging study → series → instance
                            ↓
                    study × modality
                            ↓
                  UTC day × modality summary
patient → patient imaging summary → patient imaging profile
```

- Synthea 影像 CSV 的 `Id` 是检查标识，`INSTANCE_UID` 才是实例行的业务唯一键。
- 检查可有多个序列、实例和模态；患者 `exam_count` 按检查计数。
- 日×模态汇总来自去重桥表；同一多模态检查会计入多个模态，其总和对应桥表行数，不一定等于全局检查数。
- 患者汇总从全部有效患者出发，零检查患者保留，`exam_count=0`，`latest_exam_at=NULL`。
- 患者、就诊、检查使用带 `synthea:` 命名空间的 UUID5 稳定替代键；属于假名化，不保证完全匿名。

## 来源与质量账本

`source_sha256` 标识源 CSV 内容，`source_row` 是从 1 开始、不含表头的逻辑 CSV 记录序号。带引号换行的记录不能按物理文本行定位。实例到检查的聚合不会抹掉各实例的来源；API 的 lineage 接口返回实例级来源位置。

所有源记录进入 ODS，最终分为 `accepted`、`duplicate`、`quarantined`；warning 与最终处置分别保存。详细规则见[数据质量](data_quality.md)。

## 存储、发布与消费

Python 快照/流式流程与 Spark Parquet 目录由 [snapshot_reader.py](../src/snapshot_reader.py) 统一分批读取。Spark 与 Python 对九表、ODS、处置和 warnings 做内容对账，而非仅比较计数。

PostgreSQL 以 `run_id` 隔离历史批次，业务键与父子外键附带批次维度。加载、检查及 `current_snapshot` 切换在同一数据库事务中完成，API 查询当前发布批次。文件端通过原子替换 `current.json` 切换；两端不是一个分布式事务。

FHIR 导出为 R4B 4.3.0 的 Patient、Encounter、ImagingStudy 摘要资源；不是将全部实例展开为完整临床资源或完整 FHIR Server。映射与验证边界见 [FHIR 阶段说明](stage-c-fhir/README.md)、[流式导出](stage-g/README.md)。

## 旧模型说明

旧 EMR/DICOM 演示中的 `ods_emr_raw`、`ods_dicom_metadata`、`dwd_exam_record_detail`、`ads_patient_360_view` 等属于另一条历史链路，相关代码位于 `src/pipeline.py`、`src/spark/warehouse_job.py` 和 `sql/`。不要用它们解释当前 Synthea API 或九表验收结果。

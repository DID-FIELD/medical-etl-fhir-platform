# Synthea 字段字典与目标映射（B 阶段）

本机源文件共 56 列。空值统计为本次剖析实测，目标字段与必填/处置规则为设计提案，尚未写入数据库。

所有源列在受限 ODS 保留原字符串；下表的类型是目标规范化类型。基数仅统计非空值，不展示姓名等原始值。

## patients.csv

108 条记录 · 28 列 · 32,471 字节。

| 源字段 | 非空基数 | 空值数 | 目标类型 | 规则提案 | 去向提案 |
| --- | ---: | ---: | --- | --- | --- |
| Id | 108 | 0 | string | 必需；唯一 | dim_patient.patient_key 的源业务键；原值仅受限映射 |
| BIRTHDATE | 97 | 0 | date | 必需；可解析 | ODS 保留精确值；dim_patient.birth_year |
| DEATHDATE | 9 | 99 | date | 可空；非空须可解析 | ODS 保留；首轮不进入检查概览 |
| SSN | 108 | 0 | string (raw) | 可空；首轮不作业务校验 | 仅受限 ODS，首轮不导出到分析/服务层 |
| DRIVERS | 89 | 19 | string (raw) | 可空；首轮不作业务校验 | 仅受限 ODS，首轮不导出到分析/服务层 |
| PASSPORT | 80 | 28 | string (raw) | 可空；首轮不作业务校验 | 仅受限 ODS，首轮不导出到分析/服务层 |
| PREFIX | 3 | 22 | string (raw) | 可空；首轮不作业务校验 | 仅受限 ODS，首轮不导出到分析/服务层 |
| FIRST | 108 | 0 | string (raw) | 可空；首轮不作业务校验 | 仅受限 ODS，首轮不导出到分析/服务层 |
| MIDDLE | 84 | 23 | string (raw) | 可空；首轮不作业务校验 | 仅受限 ODS，首轮不导出到分析/服务层 |
| LAST | 96 | 0 | string (raw) | 可空；首轮不作业务校验 | 仅受限 ODS，首轮不导出到分析/服务层 |
| SUFFIX | 1 | 107 | string (raw) | 可空；首轮不作业务校验 | 仅受限 ODS，首轮不导出到分析/服务层 |
| MAIDEN | 20 | 88 | string (raw) | 可空；首轮不作业务校验 | 仅受限 ODS，首轮不导出到分析/服务层 |
| MARITAL | 4 | 39 | string (raw) | 可空；首轮不作业务校验 | ODS 保留，首轮不用于检查主题模型 |
| RACE | 3 | 0 | string (raw) | 可空；首轮不作业务校验 | ODS 保留，首轮不用于检查主题模型 |
| ETHNICITY | 2 | 0 | string (raw) | 可空；首轮不作业务校验 | ODS 保留，首轮不用于检查主题模型 |
| GENDER | 2 | 0 | string/code | 必需；值域待固定 | dim_patient.gender；FHIR 另做代码映射 |
| BIRTHPLACE | 77 | 0 | string (raw) | 可空；首轮不作业务校验 | 仅受限 ODS，首轮不导出到分析/服务层 |
| ADDRESS | 108 | 0 | string (raw) | 可空；首轮不作业务校验 | 仅受限 ODS，首轮不导出到分析/服务层 |
| CITY | 73 | 0 | string (raw) | 可空；首轮不作业务校验 | ODS 保留，首轮不用于检查主题模型 |
| STATE | 1 | 0 | string (raw) | 可空；首轮不作业务校验 | ODS 保留，首轮不用于检查主题模型 |
| COUNTY | 12 | 0 | string (raw) | 可空；首轮不作业务校验 | ODS 保留，首轮不用于检查主题模型 |
| FIPS | 11 | 29 | string (raw) | 可空；首轮不作业务校验 | ODS 保留，首轮不用于检查主题模型 |
| ZIP | 69 | 0 | string (raw) | 可空；首轮不作业务校验 | 仅受限 ODS，首轮不导出到分析/服务层 |
| LAT | 108 | 0 | string (raw) | 可空；首轮不作业务校验 | 仅受限 ODS，首轮不导出到分析/服务层 |
| LON | 108 | 0 | string (raw) | 可空；首轮不作业务校验 | 仅受限 ODS，首轮不导出到分析/服务层 |
| HEALTHCARE_EXPENSES | 108 | 0 | string (raw) | 可空；首轮不作业务校验 | ODS 保留，首轮不用于检查主题模型 |
| HEALTHCARE_COVERAGE | 103 | 0 | string (raw) | 可空；首轮不作业务校验 | ODS 保留，首轮不用于检查主题模型 |
| INCOME | 99 | 0 | string (raw) | 可空；首轮不作业务校验 | ODS 保留，首轮不用于检查主题模型 |

## encounters.csv

5,571 条记录 · 15 列 · 1,857,081 字节。

| 源字段 | 非空基数 | 空值数 | 目标类型 | 规则提案 | 去向提案 |
| --- | ---: | ---: | --- | --- | --- |
| Id | 5571 | 0 | string | 必需；唯一 | dwd_encounter.encounter_key 的源业务键 |
| START | 5530 | 0 | UTC timestamp | 必需；可解析 | dwd_encounter.start_at |
| STOP | 5567 | 0 | UTC timestamp | 允许未结束就诊为空；非空不早于 START | dwd_encounter.stop_at |
| PATIENT | 108 | 0 | string | 必需；患者外键 | dwd_encounter.patient_key |
| ORGANIZATION | 245 | 0 | string | 首轮不检查未接入组织表外键 | ODS；后续组织维度候选 |
| PROVIDER | 245 | 0 | string | 首轮不检查未接入医生表外键 | ODS；后续医生维度候选 |
| PAYER | 10 | 0 | string | 首轮不检查未接入支付方表外键 | ODS；本期不接入支付模型 |
| ENCOUNTERCLASS | 10 | 0 | string/code | 保留源代码；值域监测 | dwd_encounter.encounter_class |
| CODE | 44 | 0 | string/code | 保留为字符串 | dwd_encounter.code |
| DESCRIPTION | 44 | 0 | string | 可空；不作为主键 | dwd_encounter.description |
| BASE_ENCOUNTER_COST | 11 | 0 | decimal candidate | 首轮保留原串；未做金额校验 | ODS；本期不聚合费用 |
| TOTAL_CLAIM_COST | 1763 | 0 | decimal candidate | 首轮保留原串；未做金额校验 | ODS；本期不聚合费用 |
| PAYER_COVERAGE | 1845 | 0 | decimal candidate | 首轮保留原串；未做金额校验 | ODS；本期不聚合费用 |
| REASONCODE | 104 | 2044 | string/code | 可空 | ODS；可选扩展就诊原因 |
| REASONDESCRIPTION | 104 | 2044 | string | 可空 | ODS；可选扩展就诊原因 |

## imaging_studies.csv

478 条记录 · 13 列 · 187,543 字节。

| 源字段 | 非空基数 | 空值数 | 目标类型 | 规则提案 | 去向提案 |
| --- | ---: | ---: | --- | --- | --- |
| Id | 413 | 0 | string | 必需；在实例源表中可重复 | dwd_imaging_study.study_key 的源业务键 |
| DATE | 410 | 0 | UTC timestamp | 必需；同检查一致 | dwd_imaging_study.started_at；源串留 ODS |
| PATIENT | 96 | 0 | string | 必需；与关联就诊患者一致 | dwd_imaging_study.patient_key |
| ENCOUNTER | 379 | 0 | string | 必需；就诊外键 | dwd_imaging_study.encounter_key |
| SERIES_UID | 413 | 0 | UID string | 必需；同序列父检查一致 | dwd_imaging_series.series_uid |
| BODYSITE_CODE | 11 | 0 | string/code | 序列内一致 | dwd_imaging_series.body_site_code |
| BODYSITE_DESCRIPTION | 11 | 0 | string | 序列内一致；描述不作键 | dwd_imaging_series.body_site_description |
| MODALITY_CODE | 6 | 0 | string/code | 序列内一致；不可转数字 | dwd_imaging_series.modality_code；派生 bridge |
| MODALITY_DESCRIPTION | 6 | 0 | string | 可空；不作为键 | ODS；可选模态描述字段 |
| INSTANCE_UID | 478 | 0 | UID string | 必需；实例业务键 | dwd_imaging_instance.instance_uid |
| SOP_CODE | 6 | 0 | string/code | 首轮保留；标准值域另验 | dwd_imaging_instance.sop_code |
| SOP_DESCRIPTION | 7 | 0 | string | 可空；描述不作键 | ODS；可选实例描述字段 |
| PROCEDURE_CODE | 14 | 0 | string/code | 可能与实例/检查关联；不任取 first | ODS；后续按实际关系设计关联表 |

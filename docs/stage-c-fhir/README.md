# C 阶段补充：FHIR R4B 摘要导出

实测日期：2026-09-20。标准版本固定为 **FHIR R4B 4.3.0**，显式使用 `fhir.resources.R4B`；包版本 8.2.0 不是标准版本。该选择沿用已安装的 R4B 模型；不与 R4 4.0.1 混称。

源批次 `stage-c-verified`，本次从成功文件快照读五张 DIM/DWD 表，校验原产物哈希。数据库当前发布批次未改变。

| 资源 | 数量 |
| --- | ---: |
| Patient | 108 |
| Encounter | 5,571 |
| ImagingStudy | 413 |
| 合计 | 6,092 |

Patient 包括12位零影像患者。序列总计413，实例478；413个检查摘要携带各自数量与模态。所有导出资源先通过R4B模型，再检查主键集合、患者/就诊引用、患者归属、映射和落盘回读。共9项导出核验。完整测试32项通过（新增10项），2条依赖弃用警告。数据库/API的17项样本核验再次通过。

## 映射与限制

- Patient：稳定患者键作id，M/F映射male/female，否则unknown；生日只保留源数仓年份。不输出姓名、地址与证件。不设置无法从当前维表证明的active/deceased。
- Encounter：稳定就诊键、患者引用、起止时间；有STOP使用finished，否则unknown。class按Synthea官方枚举映射，type在项目示例命名空间保留原类。遇到未知类别直接失败，要求补充规则。该示例命名空间未发布为术语服务。
- ImagingStudy：稳定检查键、患者和就诊引用、检查时间、序列/实例数量、模态列表。status为unknown，当前无法证明影像服务的可用状态。
- **CSV缺少StudyInstanceUID**：其Id是检查业务UUID，不能充当DICOM UID。R4B规定提供series时须提供DICOM检查UID。因此本轮只输出摘要，省略series、identifier、endpoint；序列实例明细继续保留在DWD及追溯API。取得可信的检查UID关联后再扩展。
- 不生成Observation：目前没有接入观察值源表，不把一次影像检查当作测量结果。
- 当前是模型+项目规则校验，没有官方HL7 Validator、全量术语服务、US Core校验或FHIR服务集成；NDJSON不代表实现Bulk Data协议。

## 复现

在 `F:\project\medical-etl-fhir-platform` 执行，输出目录必须尚不存在：

```powershell
.\.venv\Scripts\python.exe -m src.fhir.synthea_export --snapshot output/synthea/runs/stage-c-verified --output output/fhir/review-r4b-01
$env:RUN_DATABASE_TESTS='1'
.\.venv\Scripts\python.exe -m pytest -q --tb=short
.\.venv\Scripts\python.exe -m scripts.verify_stage_c
.\.venv\Scripts\python.exe docs/stage-c-fhir/render_report.py
```

本次产物：`output/fhir/stage-c-verified-r4b-20260920/` 下三个NDJSON及manifest.json。导出入口 `src/fhir/synthea_export.py`，测试 `tests/test_synthea_fhir.py`。

同样源快照生成相同资源文件；清单运行时间不同。已存在的输出目录拒绝覆盖。消费者只读SUCCESS清单；写入失败标记FAILED。这里没有自动发布指针，也没有改动数仓current.json。原始成功批次完整保留。

资源id对应数仓主键，结合source_run_id查询DIM/DWD的source_sha256与source_row，可回溯源记录。文件清单包含源清单哈希、导出代码哈希、模型包版本及产物哈希。没有宣称生成了FHIR Provenance资源。

## 标准与映射来源

- [HL7 R4B ImagingStudy定义](https://hl7.org/fhir/R4B/imagingstudy-definitions.html)：series与检查UID约束、摘要计数、unknown状态。
- [HL7 R4B Encounter](https://hl7.org/fhir/R4B/encounter.html)：就诊结构、患者引用和class字段。
- [Synthea HealthRecord源代码](https://github.com/synthetichealth/synthea/blob/master/src/main/java/org/mitre/synthea/world/concepts/HealthRecord.java)：EncounterType映射，2026-09-20核对；规则已固定在项目代码中，不运行时拉取master。

[第八章HTML](../html/08-fhir.html) · [核验清单](verification.json) · [测试证据](test-results.txt)

# B 阶段交付：Synthea 数据剖析与模型草案

最新说明：C 阶段已按本草案落地并核验核心模型；此文保留 B 阶段设计语境。当前暂停与恢复入口见 [PROGRESS.md](../PROGRESS.md)。

日期：2026-09-18。范围：读取官方样本、验证关联、确定字段映射和模型契约；未建库、未迁移表、未修改 ETL 主流程。配套入口：[HTML 报告](../html/06-stage-b.html)、[字段字典](FIELD_DICTIONARY.md)、[机器可读证据](profile.json)。

## 1. 可复现输入与方法

输入为 `data/external/synthea_csv.zip`，大小 5,960,866 字节，SHA-256 为 `d61417b551e5b0997c33851b339c157421751f0ea68c18ea686ceb1850907c35`。本轮直接读取 ZIP 中三个原始 CSV，不使用先前六列适配后的 EMR 文件。

从项目根目录执行：

```powershell
.\.venv\Scripts\python.exe docs\stage-b\profile_synthea.py
```

脚本不修改源数据或数据库。保留所有字段为字符串，空字符串作为缺失，避免把可能合法的 `NA` 代码自动转为空值；时间另解析为 UTC 进行校验。输出 `docs/stage-b/profile.json`，包含 56 列的空值/基数统计、关联检查、分布、时间范围及一个脱敏引用的源行追溯示例。

官方资料：[CSV 数据字典](https://github.com/synthetichealth/synthea/wiki/CSV-File-Data-Dictionary)、[下载页](https://synthetichealth.github.io/downloads.html)。下列数量均来自本机这份归档，不泛化为所有 Synthea 数据集的规律。

## 2. 真实数据画像

| 源表 | 记录数 | 列数 | 原始 CSV 字节数 | 粒度 |
| --- | ---: | ---: | ---: | --- |
| patients | 108 | 28 | 32,471 | 一位患者 |
| encounters | 5,571 | 15 | 1,857,081 | 一次就诊 |
| imaging_studies | 478 | 13 | 187,543 | 本样本每行一个影像实例 |

- 108 位患者都有就诊记录；96 位有影像检查，12 位没有影像记录。
- 5,571 次就诊中，379 次有影像检查，5,192 次没有影像记录。
- 413 次检查、413 个序列、478 个不同实例；本样本每次检查恰好一个序列，但模型必须支持一次检查多个序列。
- 348 次检查各 1 个实例，65 次各 2 个实例。因此 348 + 65 = 413；348 + 65 × 2 = 478。
- 模态按检查计数：CR 23、CT 1、DX 307、OP 45、OPT 20、US 17；合计 413。
- 本样本没有跨模态检查，但不能将“检查只有一个模态”设为通用强约束。
- 影像时间范围为 1981-01-08 至 2026-08-09（UTC 日期）。不是一天或一月的增量批次，不能把总量当作日吞吐量。

### 缺失不一定代表脏数据

患者 DEATHDATE 为空 99 条；不能按“必填列非空”全部隔离。就诊 REASONCODE 和 REASONDESCRIPTION 各空 2,044 条，也不能直接视为无法接入。其余可选字段缺失见完整字典。

## 3. 实际通过的检查及验证边界

- patients.Id、encounters.Id 均无空值、无重复；三表均无完全重复记录。
- 影像关键字段 Id、DATE、PATIENT、ENCOUNTER、SERIES_UID、INSTANCE_UID 无缺失。
- 就诊引用未知患者：0；影像引用未知患者：0；影像引用未知就诊：0。
- 影像 PATIENT 与其引用就诊的 PATIENT 不一致：0。
- 同一检查键对应多个患者、多个就诊或多个时间：均为 0。
- 实例 UID 重复：0；同一序列 UID 跨检查：0；序列内模态/部位冲突：0。
- 被检查日期字段的非空非法值：0；就诊结束早于开始：0；影像早于就诊开始或晚于结束：均为 0。
- 分布加总、患者分组、就诊分组等 5 项独立对账全部通过。

另外使用 Python 标准库 csv/Counter 独立重算患者/就诊键、检查实例分布和样例源记录关联，结果一致；UTC 日×模态组合为 401 组。

这不是所有医疗业务质量规则的完整验证：未验证费用、所有术语编码、组织/医生/支付方外键、影像像素和 FHIR 符合性。时间超出就诊范围在未来数据中出现时，应先分析业务含义，不自动认定临床错误。

## 4. 关系与 JOIN 契约

```text
patients.Id ──1:N── encounters.PATIENT
patients.Id ──1:N── imaging_studies.PATIENT
encounters.Id ──1:N── imaging_studies.ENCOUNTER
影像 Id（检查）──1:N── SERIES_UID（序列）──1:N── INSTANCE_UID（实例）
```

事实关联维度采用 many-to-one 验证：就诊关联患者后仍为 5,571 行；影像实例关联就诊后仍为 478 行。对预期多对一的 JOIN，如果行数增大，首先检查维度键重复。

计数使用对应业务键：患者数 COUNT(DISTINCT patient_key)，就诊数 COUNT(DISTINCT encounter_key)，检查数 COUNT(DISTINCT study_key)。不能用实例 JOIN 结果的 COUNT(*) 代替检查数。

旧的“患者 + 日期 + 类型”键在本样本中未观察到同日同模态的检查冲突，但这一结果不能证明它是可靠业务键。最终优先使用源检查 Id；不能以当前没有冲突作为舍弃源键的理由。

## 5. 模型契约 v0.1（计划，未执行 DDL）

所有表属于一个完整快照批次。下表中的键在物理存储中加 `run_id` 形成复合主键/外键，确保跨表关联不会混入其他批次。查询只读取已发布成功批次。

| 层与建议表名 | 一行的粒度 | 主键（不含公共 run_id） | 关键关系/字段 | 样本目标行数* |
| --- | --- | --- | --- | ---: |
| ods_synthea_patient_raw | 患者源记录 | source_file_sha256 + source_row | 原始字段、源系统、时间 | 108 |
| ods_synthea_encounter_raw | 就诊源记录 | source_file_sha256 + source_row | 原始字段、来源 | 5,571 |
| ods_synthea_imaging_raw | 影像源记录 | source_file_sha256 + source_row | 保留检查/序列/实例全部源字段 | 478 |
| dim_patient | 有效患者 | patient_key | source_patient_id 仅受限映射区；gender、birth_year | 108 |
| dwd_encounter | 一次就诊 | encounter_key | patient_key、start_at、stop_at、encounter_class、code | 5,571 |
| dwd_imaging_study | 一次检查 | study_key | patient_key、encounter_key、started_at | 413 |
| dwd_imaging_series | 一个检查中的序列 | study_key + series_uid | modality_code、body_site_code/description | 413 |
| dwd_imaging_instance | 一个序列中的实例 | instance_uid | study_key + series_uid 外键、SOP code、源行 | 478 |
| bridge_study_modality | 检查与模态关系 | study_key + modality_code | 去重关系，从序列构建 | 413 |
| dws_patient_imaging_summary | 每位患者 | patient_key | exam_count、latest_exam_at、modality_count | 108 |
| dws_imaging_daily_modality | UTC 日 + 模态 | stat_date + modality_code | 去重检查数、去重患者数 | 401 |
| ads_patient_imaging_profile | 每位患者的检查概览 | patient_key | 患者属性与检查汇总 | 108 |

*以上是基于源数据、当前规则和选定粒度计算的下一阶段验收目标，不是已经建成/加载的表。若实际隔离规则改变，应附原因重新对账，不能强迫结果匹配目标数。

补充规则：

- `patient_key / encounter_key / study_key` 由源系统命名空间和源业务标识生成稳定替代键，不用行号/随机 SERIAL 作为跨批业务身份。具体生成函数留给实现阶段；须检测键冲突。
- DIM 覆盖所有有效患者，不限于影像队列。DWS 从患者维表 LEFT JOIN 检查汇总，12 位无影像者 exam_count=0、latest_exam_at=NULL。
- 一次检查的多个模态由 bridge 表表达，不用拼接字符串作为维度键，也不任意选择一个模态。
- 代码/UID 一律作为字符串；不得转成数字。实例 UID 在源系统命名空间内唯一，重复且属性相同可去重，属性冲突隔离。
- UTC 时间保存为 TIMESTAMPTZ；源字符串保留在 ODS。示例按 UTC 日期聚合，不根据电脑所在时区偷偷改变统计日。
- 出生日期等 DATE 不当成带时区事件时间；下游分析维度只保留所需信息，原始字段留在受限区。哈希替代键属于假名化，不等于完全匿名。
- 首轮快照模型不声称 SCD2 已实现；患者历史版本需求另行设计。

## 6. 数量对账与指标合同

| 指标 | 口径/公式 | 样本目标 |
| --- | --- | ---: |
| 输入与 ODS | 三张表分别按行与源文件对账 | 108 / 5,571 / 478 |
| 患者覆盖 | 有影像患者 + 无影像患者 | 96 + 12 = 108 |
| 就诊覆盖 | 有影像就诊 + 无影像就诊 | 379 + 5,192 = 5,571 |
| 检查/实例转换 | 按每检查实例数加权 | 348 × 1 + 65 × 2 = 478 |
| 患者汇总检查数 | SUM(exam_count) = 检查事实行数 | 413 |
| DIM/DWS/ADS 覆盖 | 每位有效患者一行 | 各 108 |
| 按模态检查数 | 每个模态内 COUNT(DISTINCT study_key) | 本样本合计 413 |
| 一般多模态情况 | 模态分组之和可能大于去重检查总数 | 不作强制等式 |

逐行质量处置按同粒度互斥类别计算“输入=有效+隔离+去重”；不同粒度单独列转换关系。患者数按日期/模态的分组结果也不能简单相加当总患者数。

## 7. 一条真实来源的追溯例子

不展示患者姓名，采用报告内哈希简称；这些简称只用于文档，并非最终仓库键生成协议。

- 患者源记录序号：14。
- 就诊源记录序号：524。
- 影像源记录序号：35、36。
- 两条影像记录属于同一检查、同一序列，模态为 OP；它们是两个不同实例。
- 明细应产生 1 个检查、1 个序列、2 个实例，患者检查次数增加 1。

记录序号从 1 开始，不含 CSV 表头；若字段含换行，记录序号不等于文本编辑器物理行号。追溯必须同时带源文件 SHA-256，不能只记“第 35 行”。机器证据见 `profile.json` 的 `lineage_example`。

## 8. 与旧项目的差异，留待 C 阶段实施

| 旧设计/探索状态 | B 阶段建议 |
| --- | --- |
| 只有六列 EMR；缺就诊键和检查源键 | 保留三个源表，明确主外键和完整时间 |
| 一张检查明细没有影像层级 | 拆检查、序列、实例，防止按检查键误删实例 |
| 探索版患者维度只含 96 位影像患者 | 全量有效患者维度 108 位，显式保留零检查患者 |
| 拼接模态和部位作 exam_type | 代码和描述分列，模态关系用独立 bridge |
| 原主流程按阶段 truncate/insert，跨多次连接 | 快照 run_id 隔离、验证后原子发布，失败不切换 |
| 数据库建表未填充 DWS/ADS | 将汇总生成及数量核对纳入闭环 |
| 影像检查输出 Observation | 在 FHIR 实施前确定版本及 ImagingStudy 映射，不混称 |

## 9. 本阶段验收与下一步

已完成：源包标识、三表与 56 列盘点、主外键与时间检查、实际粒度验证、字段映射字典、表模型草案、指标口径、真实源行追溯案例、独立 HTML 报告。

后续 C 阶段已经进入实现：独立数据库建成、核心模型落地、空输入修复、22 个测试及 17 项样本核验通过。ODS 采用按源表隔离的统一 JSONB 存储。当前暂停；下一步为 FHIR 专项，尚未开展规模测试。

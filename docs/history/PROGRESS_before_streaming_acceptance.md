> 最新验收（2026-09-20）：万人完整文件 ETL 已通过：11,476 患者、677,836 就诊、19,435 检查、1,036,348 实例；进程树采样 RSS 峰值 280.76 MiB，低于 500 MiB，墙钟 392.618 秒。 详见[流式验收报告](stage-e/STREAMING.md)和[第十一章](html/11-streaming.html)。下文早期状态保留为历史；“尚未验证”和“RUNNING”不代表本轮结果。

> 最新交接（2026-09-20）：请优先阅读 [万人 ETL 500 MiB 优化交接](HANDOFF_MEMORY_500M.md)。流式代码已落盘但尚未测试，万人完整 ETL 和 500 MiB 目标均未验证；此前45项通过不代表最新代码。用户要求保存记录后新开会话。历史 p10000-r1 清单已为 FAILED。

# 项目进度与恢复记录

更新日期：2026-09-20。

**当前状态：用户已要求继续。C核心闭环、FHIR R4B摘要导出与D本轮13项可靠性扩展完成，E首轮分级实测完成，万人生成成功但ETL被内存保护终止；已尝试移除重复分组结构，万人复测仍受内存限制；下一步必须改为真正的分块/流式读取，并处理日期时区和中断状态。**

## 阶段状态

| 阶段 | 状态 | 已完成/剩余 |
| --- | --- | --- |
| A 项目复习 | 材料已交付 | 五章 HTML 已按 C 阶段事实更新 |
| B 数据剖析与建模 | 完成 | 三表 56 字段、关联/时间检查、模型契约、源行追溯 |
| C 小样本闭环 | 核心完成 | CSV→文件分层→PostgreSQL→API 已验证；FHIR R4B三类资源摘要已完成；完整series需源检查UID |
| D 可靠性 | 本轮扩展完成 | 新增13项；修复重复表头、缺必需哈希两问题；强杀/断电/并发压测仍未覆盖 |
| E 规模测试 | 首轮完成，万人ETL未通过 | 100/1000档各3次ETL成功；10000档生成成功，ETL内存保护终止；数据库/API规模未测 |
| F 组件与复盘 | 部分完成 | 数据库/API 已验证；Spark、Airflow、FHIR服务集成待验证 |

## 当前可信数字

官方 Synthea 合成 CSV 样本：18 个文件，压缩包 5,960,866 字节。当前主线读取其中患者、就诊、影像三表。

| 结果 | 当前成功批次数量 |
| --- | ---: |
| ODS 原始记录（按 source_table 隔离） | 6,157 |
| DIM 患者 | 108 |
| DWD 就诊 | 5,571 |
| DWD 检查 | 413 |
| DWD 序列 | 413 |
| DWD 实例 | 478 |
| 检查-模态关联 | 413 |
| DWS 患者汇总 | 108 |
| DWS 日×模态 | 401 |
| ADS 患者概览 | 108 |

- 96 位有影像、12 位无影像；后者保留，检查数为 0。
- 348 次检查各 1 个实例，65 次各 2 个实例，共 478 个实例。
- 最新完整测试：45 passed（本轮新增13项可靠性测试），2 条依赖弃用警告；旧空输入失败已修复。
- 完整样本核验：17 项通过。API 使用 ASGI TestClient 连接真实 PostgreSQL；未做 HTTP 吞吐测试。
- 当前发布 run_id：`stage-c-verified`。文件 ETL 约 0.63 秒，不含数据库/API，不是性能基准。

## 环境与连接

- 项目根目录：`F:\project\medical-etl-fhir-platform`。D 盘旧路径不可用。
- 新环境：`.venv`（Python 3.13.9）；核心依赖锁定 `requirements-core.lock.txt`。旧 `venv` 留作历史，不作为日常入口。
- 新建项目数据库：PostgreSQL 18.4，`127.0.0.1:55432`，数据库 `medical_etl`，schema `synthea_v1`。
- 用户已授权因忘记旧密码而新建实例；原系统实例及其密码未修改。
- 连接配置：`output/local-postgres/connection.json`，已 Git 忽略。不要读取并输出密码、提交配置或复制到文档。
- 新实例未注册开机服务；9月20日已启动并再次通过17项数据库/API核验。电脑重启后需要 start。
- 没有常驻 HTTP API 服务；需要时自行启动 uvicorn。

## 下次从这里继续

已恢复，以下FHIR事项完成了摘要范围，详见[第八章](html/08-fhir.html)。下一步修复E阶段发现的内存与时区问题；不重复新建数据库或三表建模。

1. 只读检查 Git 工作区、项目数据库状态和当前已发布批次；不要默认重建数据库或重跑全部 ETL。
2. 已统一 FHIR R4B 4.3.0，显式使用 R4B 模型。
3. 已导出 Patient 108 / Encounter 5571 / ImagingStudy摘要413，共6092个；9项导出核验通过。没有官方HL7 Validator/术语服务验证；不宣称完整FHIR服务。
4. 将影像专属语义与 Observation 区分；只有接入 observations.csv 后再设计观察结果资源。
5. D本轮已完成；固定Synthea版本、seed、参数，先小档验证容量，再做千/万患者规模实测。

必要命令：

```powershell
.\.venv\Scripts\python.exe -m scripts.local_postgres status
.\.venv\Scripts\python.exe -m scripts.local_postgres start
.\.venv\Scripts\python.exe -m scripts.verify_stage_c
```

## 关键入口与证据

- [当前项目复习指南](../PROJECT_REVIEW_GUIDE.md)
- [HTML 第一章](html/01-overview.html) / [C 阶段报告](html/07-stage-c.html)
- [B 阶段模型](stage-b/DATA_MODEL.md) / [56 字段字典](stage-b/FIELD_DICTIONARY.md)
- [C 阶段运行说明](stage-c/README.md)
- [22 个测试结果](stage-c/test-results.txt) / [17 项样本核验](stage-c/verification.json)
- 文件运行清单：`output/synthea/runs/stage-c-verified/manifest.json`
- 主入口：`run_etl.py`；处理：`src/synthea_pipeline.py`；事务存储：`src/database/synthea_store.py`；接口：`api/synthea.py`。

## 工作区与限制

代码、文档与测试均保存在 F 盘工作区，尚未提交 Git。没有推送或部署。`resume.tex` 是用户原有文件，不属于本任务，不改动。原始归档、运行产物和数据库配置在 Git 忽略目录，Git 提交本身不会备份它们。

原 EMR 入口只在 `--legacy-emr` 下运行，仍保留旧配置与局限；当前验证范围是新的 Synthea 主线。文件与数据库分别发布，尚无分布式事务；权限与接口能力为本机开发级别。DICOM像素、Spark集群、Airflow调度、大规模数据库/API不得称为已验证；文件ETL规模结果见E报告。

## 上轮FHIR交付

新增 `src/fhir/synthea_export.py`、10项测试与[第八章HTML](html/08-fhir.html)，证据位于 `docs/stage-c-fhir/`。产物在 `output/fhir/stage-c-verified-r4b-20260920/`；原数据库/文件发布批次不变。输出清单包含源清单、导出代码与产物哈希。

CSV缺少DICOM检查UID，不能凭业务UUID补造。FHIR只导出检查摘要，完整序列实例留在数仓。后续取得可信UID映射后再扩展series；不把当前模型校验说成标准认证。

## 最新D阶段交付

新增13项测试，完整45项通过；修复CSV重复列名静默覆盖与加载清单遗漏必需哈希的两个实际问题。后段事务失败、读取可见性、旧批次重放、文件成功数据库失败后补加载均已验证。

[第九章](html/09-reliability.html) / [运行说明与矩阵](stage-d/README.md) / [回归证据](stage-d/verification.json)。官方样本另跑到output/stage-d-validation/runs/stage-d-verified，数量保持一致，主库仍stage-c-verified。全部测试使用隔离schema，没有物理断电/强杀/多发布者压测。

## 最新E阶段首轮结果

Synthea v4.0.0，固定seed/reference/end，100/1000/10000目标人数。千人档实际1159患者、70229就诊、89354实例，2239检查产出；3次文件ETL约10.7秒，后两次JSON哈希与首跑一致，采样RSS峰值约661MiB。

万人档生成成功：11476患者、677836就诊、1036348实例，共1725660行，CSV约563.69MiB，生成418.229秒；文件ETL在5.742秒因RSS约4.23GiB超过3.5GiB保护阈值被终止。没有万人分层成功产物或规模数据库结果。失败目录清单仍RUNNING，仅控制器记ETL_FAILED；不能将孤立RUNNING视为仍运行。

数据：data/generated/scale-v4-p10000/source.zip；失败文件批次output/scale-v4/runs/p10000-r1；证据docs/stage-e/；[第十章](html/10-scale.html)。主发布仍stage-c-verified。另发现本机生日默认UTC+08日期与就诊UTC时间不一致，千人档45条隔离在UTC+08比较下均不早于出生，属于需处理的时区契约问题。

下次：先读E报告，设计流式/分块读取，修正/明确生成和日期比较时区，完善外部中断状态，再复用已有万人ZIP复测。不要重新下载JAR、重复生成万人或单纯提高内存阈值。当前测量未跑大规模数据库/FHIR/Spark/Airflow。


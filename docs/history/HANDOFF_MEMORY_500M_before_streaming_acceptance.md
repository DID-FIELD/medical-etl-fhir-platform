> 最新验收（2026-09-20）：万人完整文件 ETL 已通过：11,476 患者、677,836 就诊、19,435 检查、1,036,348 实例；进程树采样 RSS 峰值 280.76 MiB，低于 500 MiB，墙钟 392.618 秒。 详见[流式验收报告](stage-e/STREAMING.md)和[第十一章](html/11-streaming.html)。下文早期状态保留为历史；“尚未验证”和“RUNNING”不代表本轮结果。

# 新会话交接：万人 ETL 内存优化

更新：2026-09-20。用户要求记录当前任务，准备新开会话。本轮交接不继续压测。

## 目标
完善医疗 ETL/FHIR 项目，用 Synthea 的真实实测支持面试中的规模、流转、质量治理和故障恢复回答。当前优先任务是复用万人归档，完成文件 ETL，将整个 ETL 进程树采样 RSS 峰值控制在 500 MiB 以内。500 MiB 尚未达成；降低报警阈值本身不能降低实际内存。用户余额有限，避免重复生成数据和无关扩展。
自动驾驶只作面试经验迁移讨论；不新建项目。不要修改 resume.tex。

## 已验证成果
- A/B：复习 HTML、三表字段剖析、模型契约。
- C：CSV → 分层文件 → PostgreSQL 事务发布 → FastAPI。正式样本 108 患者、5571 就诊、413 检查、413 序列、478 实例，保留 12 位无影像患者。
- FHIR R4B 4.3.0 摘要：108 Patient、5571 Encounter、413 ImagingStudy，共6092资源。缺可信 StudyInstanceUID，不导出完整 series；无官方 HL7 Validator 或服务器认证。
- D：此前45项测试和17项样本核验通过。这是最新流式修改之前的结果，不能当作当前代码回归结果。
- E：千人实际1159患者、70229就诊、89354实例，原文件 ETL 约10.7秒、RSS峰值约661 MiB。
- 万人原始数据已生成：11476患者、677836就诊、1036348实例，共1725660源行，CSV约563.69 MiB。旧ETL峰值约4.23 GiB被终止，移除重复分组后仍约4.04 GiB。尚无万人完整成功结果。
- 旧暂存原型约109 MiB只完成SQLite入暂存，未输出完整数仓且清理失败，不能作为完整ETL达标证据。

## 已落盘但未经验证的本轮修改
1. src/stream_io.py：分块文件哈希、逐行JSON数组输出。
2. src/streaming_pipeline.py：替换旧原型，CSV分批入磁盘SQLite；按患者及冲突关联连通分组；批内复用共享质量规则；结果入SQLite后输出九张JSON/Parquet、行处置账本、警告和manifest。包含源哈希、对账、Parquet回读、成功后current更新、关闭数据库后清理临时目录。
3. src/synthea_pipeline.py：transform新增birth_date_offset（默认+00:00）；修正先重复后冲突应全组隔离、跨检查冲突series应两边隔离的逻辑。
上述代码尚未跑新增测试、千人回归或万人压测。run_etl.py尚未接入流式模式，500 MiB外部监控尚未实现。
实现边界：默认每批10000行，参数上限20000；单关联组还有16 MiB限制，超限主动失败。需实测检查限制，不能拆散关联组规避冲突检测。

## 下一会话执行顺序
1. 检查代码，补测试：新旧结果一致、源行号映射、多患者跨批汇总、晚到冲突、跨患者series/instance冲突、UTC+08出生日期边界、空影像、超大组拒绝、失败保留current、临时目录清理。
2. 运行原有及新增测试，修复失败。pytest使用 --tb=short，避免敏感连接信息出现在失败上下文。数据库测试用隔离schema。
3. 增加500 MiB完整子进程树RSS监控和外部终止后的FAILED记录。scripts/benchmark_synthea.py可复用，但旧保护阈值为3.5 GiB。明确采样间隔，采样保护不等于系统硬限额。
4. 先跑已有千人ZIP，以默认UTC口径对照旧九张表、源哈希及质量处置，验证优化未改变结果。
5. 明确生日时区契约：本机生成生日是UTC+08日历日期，事件是UTC时间。千人旧45条早于出生隔离在+08:00比较下消失。通过边界测试后显式传参，保持原事件UTC时间不变，记录口径差异。
6. 使用独立输出根目录与唯一run_id，在保护下复用万人ZIP完整实测。记录输入输出数量、质量、对账、耗时、峰值；不达500 MiB继续定位，不提高阈值冒充完成。
7. 更新PROGRESS、TASK_MEMORY、README和复习指南，新增第十一章HTML说明流式实现与实测证据，保留第十章失败历史；酌情接入主CLI。
大规模数据库发布、FHIR、Spark同口径与Airflow仍未完成。文件ETL达标不代表全量读取的数据库加载器也满足500 MiB。

## 环境与关键路径
- 实际根目录 F:\project\medical-etl-fhir-platform；D盘不可用，所有命令显式指定F工作目录。
- Python .venv\Scripts\python.exe；pandas、pyarrow、pytest、psutil已装。
- 万人 data/generated/scale-v4-p10000/source.zip；千人 data/generated/scale-v4-p1000/source.zip。不要重新生成或下载。
- 千人旧对照 output/scale-v4/runs/p1000-r1/。
- 新默认 output/scale-stream/；建议新验证独立用 output/scale-stream-final/。
- 正式 output/synthea/current.json 指向 stage-c-verified，不覆盖。
- PostgreSQL 127.0.0.1:55432 / medical_etl / synthea_v1。凭据 output/local-postgres/connection.json，不打印或提交。原5432数据库不动。
- 证据 docs/stage-e/；章节 docs/html/10-scale.html。
- 本轮交接核实 p10000-r1/manifest.json 已为FAILED，旧PROGRESS中仍RUNNING的描述过时。
- 工作区有大量未提交修改，不reset、不覆盖。Git不备份被忽略的归档、运行产物和凭据。

```powershell
Set-Location F:\project\medical-etl-fhir-platform
.\.venv\Scripts\python.exe -m pytest -q --tb=short
.\.venv\Scripts\python.exe -m scripts.local_postgres status
```
新CLI（未验证）：python -m src.streaming_pipeline --archive ... --output-root ... --run-id ... --birth-date-offset +08:00 --batch-rows 10000。万人执行先加内存监控。
HTML核验入口 output/html-review/check.cjs，Node为 E:\node\node.exe。
工具注意：默认cwd可能仍为失效D盘；所有exec指定F盘。PowerShell管道传中文补丁到Python可能出现编码错误，需显式UTF-8，避免损坏文档。

## 新会话提示
请读取 F:\project\medical-etl-fhir-platform\docs\HANDOFF_MEMORY_500M.md，继续万人ETL的500 MiB内存优化。流式代码已写入但尚未验证；先补测试修复，再复用已有ZIP受控实测，最后更新文档。不要重新生成数据，不覆盖stage-c-verified，节约额度。

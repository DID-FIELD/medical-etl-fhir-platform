> 最新验收（2026-09-20）：万人完整文件 ETL 已通过：11,476 患者、677,836 就诊、19,435 检查、1,036,348 实例；进程树采样 RSS 峰值 280.76 MiB，低于 500 MiB，墙钟 392.618 秒。 详见[流式验收报告](stage-e/STREAMING.md)和[第十一章](html/11-streaming.html)。下文早期状态保留为历史；“尚未验证”和“RUNNING”不代表本轮结果。

> 最新交接（2026-09-20）：请优先阅读 [万人 ETL 500 MiB 优化交接](HANDOFF_MEMORY_500M.md)。流式代码已落盘但尚未测试，万人完整 ETL 和 500 MiB 目标均未验证；此前45项通过不代表最新代码。用户要求保存记录后新开会话。历史 p10000-r1 清单已为 FAILED。

# 项目任务记忆

更新：2026-09-20。

## 当前指令

用户已明确要求继续，撤销此前暂停。已完成FHIR摘要和D可靠性扩展（第九章）；E首测已完成并发现万人ETL内存瓶颈，下一步有界内存读取和时区契约。没有安排自动任务。

最新完整恢复入口：[PROGRESS.md](PROGRESS.md)。本文件覆盖之前的阶段性指令；早期过程保存在 [历史记忆](history/TASK_MEMORY_before_pause.md)，不可把历史未完成状态当成当前状态。

## 用户目标与范围

完善医疗 ETL / FHIR 项目，用真实可复现的合成数据实测支撑面试回答：输入规模、输出数量、数据流转、质量处理、实际问题和重跑恢复。数据源确定为 Synthea。

自动驾驶仅是面试中如何迁移既有经验的讨论，不是另一个必做项目。

## 已完成

- A：项目复习 HTML 与架构材料。
- B：原始三表 56 字段剖析、主外键/时间核验、模型与指标契约。
- C 核心：Synthea CSV→分层文件→PostgreSQL 事务发布→API 查询。
- 新建本机独立 PostgreSQL（用户授权）；原数据库不改。
- 108 患者、5,571 就诊、413 检查、413 序列、478 实例、401 日模态组；患者 DIM/DWS/ADS 各108，保留12位无影像患者。
- 45 测试通过，17 项完整样本验证通过；旧空输入错误已修复。
- 基础重复、冲突、孤儿关系、多模态、失败回滚与重跑测试已覆盖。

## 环境与文件

根目录 F:\project\medical-etl-fhir-platform；使用 .venv，不使用 D 盘旧路径。依赖锁定 requirements-core.lock.txt。

数据库 127.0.0.1:55432 / medical_etl / synthea_v1；连接配置 output/local-postgres/connection.json 已忽略，不打印或提交密码。数据库最后检查运行正常，没有开机自启；HTTP API 未常驻。

当前发布批次 stage-c-verified；证据在 docs/stage-c/ 与 output/synthea/runs/stage-c-verified/。代码和文档未提交 Git；resume.tex 为原有用户文件，不动。

## 未完成与续做起点

1. 已统一FHIR R4B 4.3.0：108 Patient、5571 Encounter、413 ImagingStudy摘要，共6092资源，9项导出核验通过。完整series因缺StudyInstanceUID暂未导出；取得可信关联后再扩展。没有官方HL7 Validator、术语服务或FHIR服务器验证。证据docs/stage-c-fhir/；产物output/fhir/stage-c-verified-r4b-20260920/。
2. D本轮13项测试已完成，修复重复CSV表头和缺必需哈希；新增tests/test_synthea_reliability.py，证据docs/stage-d/。强杀、断电、多发布者并发仍未验证。
3. E首测：100/1000档生成及3次ETL成功；万人生成11476患者、1725660源行，ETL超过内存阈值终止。数据已保存data/generated/scale-v4-p10000/source.zip，证据docs/stage-e/，第十章已生成。下一步优化读取内存、日期时区与外部终止状态；不重新生成。
4. Spark 同口径结果、Airflow 编排与完整项目复盘。

9月20日已启动原项目数据库，17项样本核验再次通过。下次从E内存瓶颈与时区契约修复继续；不重复新建数据库、三表建模或本次FHIR摘要导出。


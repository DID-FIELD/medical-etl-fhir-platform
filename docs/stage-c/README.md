# C 阶段：三表小样本闭环与新本地数据库

日期：2026-09-18。

> 历史阶段报告（2026-09-18）：下文的“尚未完成”和“下一步”仅指当时状态。当前 Stage M 万人完整链路已验收，见[最新交接](../HANDOFF_STAGE_M.md)。首次使用请从[项目首页](../../README.md)进入；本文发布命令会切换配置中的当前批次。

## 当前阶段

A 项目复习材料和 B 数据剖析/模型已交付。C 阶段的 **Synthea CSV → 质量处置 → DIM/DWD/DWS/ADS → PostgreSQL 原子发布 → API 查询**核心闭环已跑通。

这不表示整个项目完成。Synthea FHIR 版本与 ImagingStudy/Encounter 映射、Spark 同口径对照、Airflow 编排、大规模生成与性能测试尚未完成；D 阶段的基础异常/重跑测试已提前覆盖一部分，仍需扩展。

## 新建数据库，而非重置旧库

用户忘记原密码并授权重新配置或新建。已使用本机 PostgreSQL 18.4 软件，在项目内部创建独立实例：

| 配置 | 值 |
| --- | --- |
| 主机 | 127.0.0.1（只监听本机） |
| 端口 | 55432 |
| 数据库 | medical_etl |
| 本地开发用户 | medical_local |
| 业务 schema | synthea_v1 |
| 数据目录 | output/local-postgres/data |
| 连接配置 | output/local-postgres/connection.json |

密码为随机生成，连接配置已更新；不要提交配置文件或数据目录。已验证它们被 Git 忽略。项目 Synthea 存储模块自动读取配置，不需要在命令行写密码。该账号是新实例的本地开发管理员，不是生产环境最小权限账号。

原端口的 PostgreSQL 系统服务、账号和数据未修改。新实例未注册 Windows 自动启动服务；电脑重启后使用下方 start 命令启动。数据库文件保留，不会因为 Python 程序退出而清空。

## 可复现命令

在 F:\project\medical-etl-fhir-platform 下执行：

```powershell
# 核心依赖（新机器先创建 .venv）
.\.venv\Scripts\python.exe -m pip install -r requirements-core.lock.txt

# 首次新建：仅在没有本地配置时需要；会生成新密码
.\.venv\Scripts\python.exe -m scripts.local_postgres init

# 日常启动与检查
.\.venv\Scripts\python.exe -m scripts.local_postgres start
.\.venv\Scripts\python.exe -m scripts.local_postgres status

# 生成新快照并加载数据库；省略 run-id 自动生成新批次
.\.venv\Scripts\python.exe run_etl.py --publish-db

# 校验当前发布的官方小样本；此脚本的期望数量仅用于当前样本
.\.venv\Scripts\python.exe -m scripts.verify_stage_c

# 启动 API（当前报告验证用 ASGI TestClient，没有常驻 HTTP 服务）
.\.venv\Scripts\python.exe -m uvicorn api.main:app --host 127.0.0.1 --port 8000

# 含真实数据库的完整测试
$env:RUN_DATABASE_TESTS = '1'
.\.venv\Scripts\python.exe -m pytest -q --tb=short
Remove-Item Env:RUN_DATABASE_TESTS

# 需要时停止新实例，不影响旧系统服务
.\.venv\Scripts\python.exe -m scripts.local_postgres stop
```

Windows 受限沙箱可能阻止 PostgreSQL 进程启动，届时需要允许启动该本机后台进程。本轮已在授权后启动成功。

原 EMR 示例保留在 `run_etl.py --legacy-emr`，沿用原配置。新主入口默认使用 Synthea；新增 API 使用 `/api/synthea` 前缀，原 API 未迁移到新模型，不应将其当作本轮已验证接口。

## 已落地的模型与实数

| 模型 | 本次落地行数 |
| --- | ---: |
| DIM 患者 | 108 |
| DWD 就诊 | 5,571 |
| DWD 检查 | 413 |
| DWD 序列 | 413 |
| DWD 实例 | 478 |
| 检查-模态关系 | 413 |
| DWS 患者检查汇总 | 108 |
| DWS 日×模态统计 | 401 |
| ADS 患者检查概览 | 108 |

12 位没有影像记录的患者保留，exam_count=0、latest_exam_at=NULL。患者检查次数总和为 413。

三张 ODS 在文件中分别保存为原始 JSON，在 PostgreSQL 的 `source_records` 中按 source_table 隔离存储，保留完整原始记录 JSONB、文件校验值和源记录序号；总计 6,157 条。这是对 B 草案“三张独立 ODS 物理表”的实现调整：保留三张逻辑源表和原始内容，采用统一来源存储，未丢弃原字段。

原始数据均为 Synthea 合成样本。运行和产物见 `output/synthea/runs/stage-c-verified/`，核验结果见 [verification.json](verification.json)。文件 ETL 单次约 0.63 秒，仅包含文件流程，不包含数据库加载和 API 验证，不作为稳定性能指标。

## 质量、追溯和发布

- 关键列缺失/CSV 宽度错误导致批次失败。
- 相同业务标识且内容相同：保留一条，其余标记重复；相同标识内容冲突：隔离。
- 患者不合法时，就诊/检查关联失败会级联隔离。
- 检查中有无效实例或父级/序列属性冲突时，整项检查隔离，避免发布不完整检查。
- 影像时间超出就诊范围先记 warning，避免未经业务确认直接丢弃；出生时间和就诊先后等已实施明确规则。
- 所有事件时间转 UTC；患者出生日期按 DATE 处理。
- 替代键采用源系统命名空间 + UUID5，保证重跑身份稳定；这属于假名化，不保证完全匿名。
- 每个实例保留源 SHA-256 与记录序号，可由检查追溯到所有源实例。
- 全空患者快照不发布；无影像但有有效患者的快照允许发布。
- 文件批次目录不可覆盖；成功后用原子文件替换更新 current 指针。数据库单事务加载、检查和切换当前批次，失败整体回滚。
- 文件与数据库分别维护发布指针，不是跨文件系统与数据库的分布式事务；数据库失败可重试加载已有成功文件快照，不需要重算数据。
- 同一 run_id、相同 manifest 重复加载返回 ALREADY_LOADED；同名不同内容拒绝。新 run_id 可以保留相同数据的重跑历史，API 仅查询当前快照，不叠加全部历史。
- 数据库以批次复合主键/外键约束父子关系，写入后重新统计行数和检查总数。

## API 已验证范围

- `GET /api/synthea/status`：当前发布批次及计数。
- `GET /api/synthea/patients/{patient_key}`：患者概览，含无影像患者；未知患者返回 404。
- `GET /api/synthea/patients/{patient_key}/studies`：该患者的检查列表。
- `GET /api/synthea/studies/{study_key}/lineage`：实例源文件校验值和源记录位置，不返回原始患者姓名。

对实际 108 位患者逐个验证概览和检查列表；来自源影像第 35、36 条记录的同一检查，通过 API 确认返回两个实例来源。测试使用 ASGI TestClient 加真实数据库，不代表网络吞吐量或生产访问控制已经测试。当前接口为本机开发用途，没有生产鉴权与分页优化。

## 测试结果与真实问题

- 22 个测试通过，包括真实 PostgreSQL 集成测试；2 条依赖弃用警告，不影响当前结果。
- 17 项完整样本核验通过：9 张表计数、零检查患者数、检查汇总、ODS 数量、状态与患者/检查 API、来源追溯、404 行为。
- 旧探索链路的空输入异常已修复，原来的失败回归测试通过。
- 新测试覆盖多序列/多模态、合法多实例、重复和冲突、孤儿关联、UTC 规范化、零影像输入、无效空快照、同名批次保护。
- 数据库测试在随机独立 schema 中注入写入失败，验证当前批次保持不变且没有半批数据；修复后同一文件快照可重新加载。测试完成删除该专用 schema，不触碰正式样本 schema。
- Windows 的后台进程启动与输出句柄导致启动等待，已改为文件输出和数据库连接探测，并实际验证服务可连接。

## 下一步

优先补齐 FHIR 版本和资源映射设计/验证，再扩展 D 阶段失败恢复与数据异常覆盖，随后进入规模测试。大规模 Synthea 数据仍未生成；Spark、Airflow 仍未跑通。本阶段报告不能替代这些验证。

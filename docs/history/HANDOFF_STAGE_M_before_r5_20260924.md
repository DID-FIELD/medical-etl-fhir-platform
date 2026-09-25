> 最新续做（2026-09-24 12:44）：r4 已确认中断，数据库首次 1800 秒超时且未提交；FHIR 遗留 RUNNING，WSL 无验收进程；主机 01:58 关机。正式数据库指针复核仍 stage-c-verified。[中断证据](stage-m/full-consumers-p10000-r4-interrupted/README.md)。r5 正在启动，继续使用 2 GiB Spark 堆；Stage M 尚未通过。以下 r4 执行中描述均为历史。

> 最新核查（2026-09-23 23:50）：r3 已结束并归档 FAILED。生产四任务 dag.test 成功，但真实 scheduler 的 Spark 两次 Java heap space；新 r4 已显式配置并实采确认 2 GiB 堆；Spark 首次尝试 SUCCESS（26 项检查、14 组对账通过），生产数据库消费执行中。独立 FHIR 修复验证保持成功；下方运行中描述为历史。[r3 失败证据](stage-m/full-consumers-p10000-r3/README.md)。

> 续做更新（2026-09-23）：已开始 FHIR 超时诊断，局部 SQLite 写入对照显示 DrvFS 21.05 秒、Linux 临时目录 2.43 秒（各 10 万条）。导出器已改用系统临时目录，36 项针对性测试及另一批 4 项数据库测试通过。复用 r2 Spark 的隔离万人导出已 SUCCESS（742.634 秒），708,747 资源逐条等价及成功重试复用通过；r3 完整 DAG 首次 Spark 尝试发生 Java heap space，当前由 Airflow 自动进行第二次尝试，总结果待完成；本段之后的 11:20 状态为历史核查。详见 [诊断记录](stage-m/fhir-timeout-diagnosis-20260923-r1/README.md)。

# Stage M 当前交接：万人 FHIR 超时待定位

更新：2026-09-23 11:20（Asia/Shanghai）。用户最新要求“写交接文档”；本次收尾只整理现场，不启动新验收。下一任务首先按本文核对状态。

## 1. 结论先看

- **Stage L 千人四任务完整 DAG 已通过；Stage M 万人完整 DAG 尚未通过。**
- r2 已完成生产 dag.test 中的输入校验、完整 Spark ETL 和数据库消费。数据库 manifest 为 SUCCESS，九表、source_records、row_dispositions 共 11 组逐行内容核对全部零差异。
- **r2 的 FHIR 第一次尝试明确在 1800 秒超时**；第二次尝试留下 RUNNING 回执，没有完成记录。不能将全部问题只归因于关机。
- r2 总报告仍 RUNNING，但本次检查 Windows/WSL 未发现验收 Python、Java、Airflow 进程。Windows 记录 03:24 关机、10:12 日志服务启动，当前不是后台仍在执行。
- 生产 dag.test 未整体成功，未进入真实 scheduler 的最终验收；没有本轮双下游成功重试、708,747 资源等价及 132 次 API 的完整通过结论。
- 当前正式文件指针只读核查仍 stage-c-verified。正式数据库在 r2 启动前核查同样为 stage-c-verified；本次 11:20 未重新连接数据库，不把启动前检查说成此时已复核。

## 2. 实际目录、环境和保护范围

实际根目录 **F:\project\medical-etl-fhir-platform**；D: 旧路径不可用。所有 shell 调用显式传 F: workdir。

- Windows Python：`.venv\Scripts\python.exe`；PowerShell，中文日志用 PYTHONIOENCODING=utf-8。
- WSL 程序：`C:\Program Files\WSL\wsl.exe`；发行版 MedicalETL-Airflow，root。
- Airflow Python：`/opt/medical-etl-airflow/runtime/airflow-venv/bin/python`；Airflow 2.11.2。
- Spark/FHIR worker：`/opt/medical-etl-airflow/runtime/worker-venv/bin/python`；PySpark 4.0.1、JDK21、local[2]。
- Windows DB worker：`/mnt/f/project/medical-etl-fhir-platform/.venv/Scripts/python.exe`；cwd 同 Windows 项目的 /mnt/f 路径，移除 Linux PYTHONPATH。
- 只读源码挂载：`/opt/medical-etl-airflow/project`；可写输出：`/mnt/f/project/medical-etl-fhir-platform/output`。WSL 重启后挂载可能失效，执行前检查；不要通过只读源码挂载写产物。
- 项目独立 PostgreSQL：127.0.0.1:55432 / medical_etl。配置在 Git 忽略的 `output/local-postgres/connection.json`，不要打印凭据。现时是否已启动需重新检查；系统其他 postgres 进程不代表这个端口可用。
- `scripts/local_postgres.py start` 可恢复既有项目实例；此前 sandbox 下 pg_ctl 报 restricted token error 87，获准在宿主环境运行后恢复成功。没有修改认证或监听范围。
- 当前 main 分支大量 modified/untracked；不 reset/clean，不擅自提交，不改 resume.tex 或 docs/resume，不切换正式发布，不删除失败现场。
- 不需要重新生成或下载 Synthea 数据，不自动创建新任务或自动化。

## 3. r2 现场与时间线

session：`full-consumers-p10000-r2`。
生产 run_id：`manual__2026-09-22T16:27:17.643745+00:00`。
结果目录哈希：`f6014750f4b58739ab104662f39d554277bde2583e167ac58ea2963286b9f66b`。
数据库隔离 schema：`synthea_airflow_f6014750f4b58739ab104662f39d5542`。

| 本地时间（09-23） | 观察结果 |
| --- | --- |
| 00:27:43 | verify_inputs SUCCESS |
| 00:36:56 / 00:37:05 | Spark manifest / Airflow Spark task SUCCESS |
| 00:37:06 | export_fhir try=1 开始 |
| 01:07:15 | FHIR worker TimeoutExpired，1800 秒；任务 UP_FOR_RETRY |
| 01:07:17 | load_database 开始 |
| 01:25:10 / 01:25:11 | 数据库 manifest / Airflow task SUCCESS，11 组内容一致 |
| 01:25:11 | export_fhir try=2 开始；消费者及内部导出 manifest 遗留 RUNNING |
| 03:24:45 / 03:24:54 | Windows 1074 关机事件 / 6006 日志服务停止 |
| 10:12:45 | Windows 6005 日志服务启动 |
| 11:20 左右 | 无验收进程；没有总 SUCCESS、没有 scheduler_run 结果 |

不能仅凭第二次尝试遗留状态推断其未完成的具体根因；应继续读日志/临时索引/导出代码，不将关机当成第一次超时的原因。

现场路径：

- Windows：`output/full-consumers-p10000-r2/acceptance.json`、`results/<上述哈希>/`。
- WSL：`/opt/medical-etl-airflow/runs/full-consumers-p10000-r2/`，含 Airflow metadata.db、cli.log；主日志为同级 `full-consumers-p10000-r2.log`。
- 本次快照：[r2 核查](stage-m/full-consumers-p10000-r2-interrupted/inspection.json)、[总报告原件](stage-m/full-consumers-p10000-r2-interrupted/acceptance.json)、[主日志](stage-m/full-consumers-p10000-r2-interrupted/acceptance.log)。各 manifest 一并复制，未改写原状态，未复制全量数据。
- 原执行工具 session 曾为 2461；它不是恢复入口，不假设跨任务仍有效，以操作系统进程和日志为准。

## 4. r1 必须保留

r1 session `full-consumers-p10000-r1` 的 Spark 成功，数据库 RUNNING；09-23 恢复项目 PostgreSQL 时服务日志显示异常关闭并自动恢复。只读确认隔离 schema `synthea_airflow_3dac2edb120ad71dab3da76e630295dc` 已提交 run，但不能证明消费者全部核验完成。

09-22 19:31:25 Windows 1074 记录开始菜单发起关机，19:31:34 日志服务停止、19:33:34 启动。该轮和 r2 是不同现场，不混用结论。

证据：[r1 核查](stage-m/full-consumers-p10000-r1-interrupted/inspection.json)、[数据库只读回执](stage-m/interruption-check-20260923.json)、[系统事件](stage-m/full-consumers-p10000-r1-interrupted/system-events.json)。原始 manifest 均未改成 FAILED 或 SUCCESS。

## 5. 下一任务先做什么

1. 先读 r2 核查与日志，确认有无其他任务接手或活动进程；仅 RUNNING 文件不能作为存活证据。不立即启动另一轮完整 ETL。
2. **先定位 FHIR 超时。** 入口 `src/airflow_tasks.py:execute_consumer` 固定 worker timeout=1800；导出在 `src/fhir/streaming_export.py:export_streaming`，公共目录读取在 `src/snapshot_reader.py`。导出 SQLite 临时索引建在 output 内，r2 位于 WSL 的 /mnt/f；跨文件系统随机 I/O 是可调查方向，尚未证实。不要直接提高超时并宣称问题解决。
3. 检查 r2 两次 FHIR 尝试留下的临时 SQLite、manifest 和输出数量；保留现场。诊断/修复尽量复用已经 SUCCESS 的 r2 Spark 快照，另用新尝试号或全新隔离诊断目录，并遵守同 run 绑定；不要伪造 run_id、覆盖已有尝试或改成功 manifest。
4. 若更改消费者/适配器，补针对性回归，重新核验代码哈希绑定；旧成功结果可能无法按新代码复用，不绕过校验。
5. 修复经独立导出验证后，安排全新 r3（先检查名称未存在）完整验收。它必须包括生产 dag.test、真实 SequentialExecutor scheduler、两消费者首次持久化成功后注入故障、try=2 成功复用、11 组内容、FHIR 708,747 资源等价、132 次串行 ASGI、正式指针不变。
6. 真正 SUCCESS 后才调用收集器归档和更新成功结论。原收集器拒绝 RUNNING；本次 interrupted 快照不冒充收集器完成的成功验收。

输入固定为：

- ZIP：`data/generated/scale-v4-p10000/source.zip`，SHA256 `cde7f394763863dae3123799a54fd21dbc90683c650ceb17fe7db85f79a28559`。
- Python 基准：`output/scale-stream-final/runs/p10000-stream-local-r1`，manifest SHA256 `69a9f6652041c7475bca33fa81ece23d5430f8a912721b5d08853ce266b0a11c`。
- FHIR 参考：`output/fhir/p10000-stream-r2`；manifest SHA256 `f4ff8aac8668df4524576f2c8e9c4bc0497fbe31260960859cf6047cfea91a5a`。
- 11,476 Patient + 677,836 Encounter + 19,435 ImagingStudy = 708,747；源行 1,725,660。
- 生日口径 +08:00，事件 UTC；不改变基准或参考内容。

新完整验收示例（**先解决 FHIR 超时，且 session 必须尚不存在**）：

```bash
mountpoint -q /opt/medical-etl-airflow/project || mount --bind /mnt/f/project/medical-etl-fhir-platform /opt/medical-etl-airflow/project
mount -o remount,bind,ro /opt/medical-etl-airflow/project
export PYTHONDONTWRITEBYTECODE=1
/opt/medical-etl-airflow/runtime/airflow-venv/bin/python \
  /opt/medical-etl-airflow/project/scripts/airflow/full_consumers_acceptance.py \
  --session full-consumers-p10000-r3 --population 10000 \
  --archive /opt/medical-etl-airflow/project/data/generated/scale-v4-p10000/source.zip \
  --baseline /opt/medical-etl-airflow/project/output/scale-stream-final/runs/p10000-stream-local-r1 \
  --fhir-reference /opt/medical-etl-airflow/project/output/fhir/p10000-stream-r2 \
  --scheduler-timeout 7200
```

7200 秒只控制 scheduler 等待，并不解除 consumer worker 的 1800 秒限制。运行时保留主日志，避免宿主机关机中断。

## 6. 本轮代码、测试和教材状态

本次没有修改业务转换/消费者实现；恢复项目数据库、启动 r2、核查并整理证据和文档。已有参数化验收入口是接手前工作。

- 再验参数/参考绑定测试：7 passed / 2.55 秒；[日志](stage-m/full-consumers-scale-tests-20260923-r1.log)。这是独立测试批次，不合并旧批次数字。
- PROGRESS.md / TASK_MEMORY.md 重新整理，原长篇历史保存在 docs/history/*before_stage_m_20260923.md。
- 第 15 章源文件 `docs/html/chapters/15-consumers.html` 已新增“事务已提交，为什么验收仍未完成？”r1 案例。**尚未重新构建生成页面或进行本轮浏览器 QA**，要先按最终实际结果继续补充 r2 超时，再构建。
- `output/finalize-stage-m-html-r2.py` 和 `output/verify-stage-m-evidence-r2.py` 只是准备文件，**未运行**，内部假设 r2 SUCCESS；当前不能运行、更不能当作成功证据。未来若 r3 成功要重新审阅并改目标，不批量套用错误结论。
- 本轮新 QA 目录 `output/html-stage-m-qa-20260923/` 已准备 check_links.py / check.cjs，但尚未执行；旧 Stage L QA 不能算本轮完成。
- HTML 正确修改入口为 chapters/*.html，然后 `.venv\Scripts\python.exe docs/html/build_chapters.py`；链接/锚点、重复构建字节一致性、Edge 1366/390 两档 30 个视图、关键截图均需复核。
- Edge 检查 Node：`E:\node\node.exe`；Playwright 路径见准备好的 check.cjs。检查后浏览器退出，保留截图与 JSON。

## 7. 成功后收尾及边界

SUCCESS 后在 WSL 调用 `scripts/collect_consumer_airflow_evidence.py --session <成功的新 session> --stage stage-m`，输出目录必须不存在。独立验证 manifest/代码哈希、回执相同、无第二份消费者产物、11 组与 708,747 资源及 API；同步 Stage M、PROGRESS、TASK_MEMORY、当前交接、README、PROJECT_REVIEW_GUIDE 和教材，完成布局 QA。

本轮没有新增 RSS 测量，不将历史 500 MiB 套给 Spark/PostgreSQL；没有多机集群、HTTP 并发、官方 FHIR Validator/Server 或 CDC 结论。Stage L 千人成功与 Stage I 万人 Spark 转换仍有效，但不能拼接成 Stage M 全链路成功。

[Stage L 历史交接](history/HANDOFF_SPARK_FULL_ETL_before_stage_m_20260923.md) 保留更详细环境说明；当前状态以本文、Stage M 原始证据及后续实测为准。

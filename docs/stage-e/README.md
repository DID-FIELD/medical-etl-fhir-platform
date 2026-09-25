# E 阶段：Synthea分级规模实测

日期：2026-09-20。使用官方Synthea v4.0.0本机生成新数据，按100、1000、10000目标人数分档，再执行既有三表文件ETL。结论：100与1000档文件ETL成功且各3次结果一致；10000档生成成功，但文件ETL触发内存阈值而被终止。第十章由证据生成。

## 固定条件

- JAR版本v4.0.0，SHA-256 `ed43c20ad40ba5c3bc724503a5af032715fe3c491620b766148e7c2361e6ecc1`，与发布资产digest一致。
- population seed / clinician seed：20260920；referenceDate / endDate：20260920；Massachusetts。
- 最近10年历史；只导出patients、encounters、imaging_studies三张CSV；原生FHIR输出关闭。
- Java25.0.1，堆上限2GiB，2线程；Windows约15.69GiB物理内存，开始时约5GiB可用，F盘约222GiB空闲。
- 这轮生成未覆盖JVM默认时区；主机China Standard Time（UTC+08）。已发现日期与时间戳跨时区问题，见下文，不能忽略这项复现条件。

## 测量定义

生成计时：Java命令启动至结束，含启动/模拟/CSV导出。

文件ETL计时：Python子进程启动至结束，含导入依赖、读ZIP/CSV、质量处置、分层、JSON与Parquet写入、读回核验。压缩打包独立计时。manifest.elapsed_seconds是ETL内部时间，不与进程墙钟混用。

复跑：复用同一ZIP，使用新run_id输出，每档最多3次ETL；比较数量及不含run_id的JSON产物哈希。Parquet含run_id，因此不要求跨批次文件哈希相同。各档生成仅一次，不能给生成耗时置信区间。

内存：每250ms采样启动器及后代进程RSS之和，报告采样最大值。共享内存可能重复计算；不是独占内存、分配总量或精确瞬时峰值。初版100/1000生成与ETL首跑只统计启动器，已将字段明确改为launcher_peak_working_set_bytes_not_workload并排除结论，ETL复跑修正为整个进程树。早期内存数据缺失不能补造。

保护阈值：开始前15GiB空闲盘、3GiB可用内存；运行时可用内存低于700MiB或进程树RSS超过3.5GiB或超过30分钟时停止被测进程树并记录原因。阈值退出不是ETL正常完成；要检查status、exit_code、stop_reason和清单。

## 两个重要的数据解释

1. `-p`与患者文件行数不同。本次100档110条患者，1000档1159条；生成日志分别记录目标存活人数和额外死亡患者。规模报告使用实际CSV行数。
2. 实例增长不按患者数简单同比。千人档89354实例对应2239检查，一次检查最多500实例；不得用100档的影像量简单乘10预测所有档位。

## 质量问题：日期时区跨日

100档3条、1000档45条就诊被原规则标为encounter_before_birth。千人档45条在转成UTC+08日期后，均不早于患者生日。

已检查Synthea v4.0.0 ExportHelper：DATE_FORMAT使用默认时区，而ISO_DATE_FORMAT使用UTC；CSVExporter分别用这两种格式导出生日和就诊时间。例如生日2018-08-21，对应就诊2018-08-20T17:56:58Z，在UTC+08已经是8月21日。

这些是与时区边界一致的疑似误隔离，不能直接当作源病历错误。这轮不静默改规则或重写已生成文件；保留隔离账本、源哈希与诊断结果。下一轮应明确日期的时区契约，例如将生成器固定UTC后重新生成独立批次，并重新验收边界规则。原官方样本与新增样本的质量结果分别报告。

## 运行与文件

```powershell
.\.venv\Scripts\python.exe -m pip install -r docs/stage-e/requirements-benchmark.txt
.\.venv\Scripts\python.exe -m scripts.benchmark_synthea --population 1000
.\.venv\Scripts\python.exe -m scripts.benchmark_repeat_etl --population 1000 --repeat 2
.\.venv\Scripts\python.exe -m scripts.profile_scale_quality --population 1000
.\.venv\Scripts\python.exe docs/stage-e/render_report.py
```

上述生成命令只在目标目录未存在时运行。数据保存在 `data/generated/scale-v4-p{n}/`，文件数仓在 `output/scale-v4/runs/p{n}-r{repeat}/`，均Git忽略；证据JSON与HTML在docs内。主库与主文件发布批次仍为stage-c-verified，不被本轮规模数据覆盖。工具仅适用于当前Windows测量环境。

## 验证边界

这是单机数据生成与文件ETL规模试验，不等于分布式大数据平台验证。没有对这些规模数据做数据库加载、HTTP并发、FHIR输出性能或长期负载测试。当前运行条件包含普通桌面系统背景负载，少量复跑仅提供观察值，不承诺稳定吞吐或线性扩展。

[第十章HTML](../html/10-scale.html) · [生成器证据](generator.json) · [环境](environment.json) · [100档](p100.json) · [1000档](p1000.json) · [10000档](p10000.json)

依据：[官方运行说明](https://github.com/synthetichealth/synthea/wiki/Basic-Setup-and-Running)、[v4.0.0发行版](https://github.com/synthetichealth/synthea/releases/tag/v4.0.0)、[ExportHelper日期逻辑](https://github.com/synthetichealth/synthea/blob/v4.0.0/src/main/java/org/mitre/synthea/export/ExportHelper.java)。

## 本轮实测结论

| 请求人数 | 实际患者 | 就诊输入 | 实例输入 | 检查产出 | 文件ETL |
| --- | ---: | ---: | ---: | ---: | --- |
| 100 | 110 | 5,419 | 184 | 124 | 3次成功 |
| 1,000 | 1,159 | 70,229 | 89,354 | 2,239 | 3次成功 |
| 10,000 | 11,476 | 677,836 | 1,036,348 | 无成功产出 | 内存保护终止 |

万人档共1,725,660源记录，591,076,792字节CSV（563.69MiB），ZIP为38,603,995字节。生成418.229秒，进程树采样峰值1,730,809,856字节（约1.61GiB）。文件ETL在5.742秒后因进程树RSS超过3.5GiB终止；观测峰值4,543,516,672字节（4.23GiB），采样间隔导致阈值越界后才终止。这不是成功耗时，也不是机器物理内存上限结论。

千人档3次文件ETL墙钟时间10.790、10.684、10.659秒。后两次测得进程树RSS峰值约661、633MiB，输出计数和JSON哈希与首跑相同。100档复跑2.867、1.576秒，首跑8.020秒，启动/缓存/背景负载使小档时间差异明显。

被终止的p10000-r1和p10000-r2目录均无ODS/DWD产出；控制器现已将清单补记为FAILED，error_type=ExternalProcessTermination，exit_code=15、stop_reason=process_tree_rss_above_3.5GiB。没有残留被测进程、没有将失败批次发布到主库；不要把RUNNING孤立清单看成仍在运行。本轮产生了一个受控的进程终止案例，与D阶段仅异常注入有所区别。

优化尝试：去掉按主键分组的重复列表后，45项回归测试仍通过；万人档复测依然在约5.5秒触发内存保护，说明全量源行与全量结果同时驻留才是主瓶颈。下一轮优先：分块/流式输入降低Python字典常驻内存；明确日期时区契约；完善外部中断的状态核对。复用已生成万人ZIP进行优化前后对照，不重复昂贵的生成步骤。待文件ETL完成后，再测数据库/FHIR等后续阶段。E阶段端到端万人验收尚未完成。

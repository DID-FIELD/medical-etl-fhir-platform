"""Render database/API scale evidence without rerunning the measured workload."""
import html
import json
from pathlib import Path
import re

ROOT=Path(__file__).resolve().parents[1]


def main():
    folder=ROOT/'docs/stage-f'
    runs={name:json.loads((folder/(name+'.json')).read_text(encoding='utf-8')) for name in ['p1000-db-r1','p1000-db-r2','p10000-db-r1','p10000-db-r2']}
    big=runs['p10000-db-r2'];loaded=runs['p10000-db-r1'];small=runs['p1000-db-r1'];retry=runs['p1000-db-r2']
    if big['status']!='SUCCESS' or retry['status']!='SUCCESS':
        raise ValueError('Missing successful scale/API evidence')
    worker=big['worker'];client=big['measurement'];server=big['postgres_server'];counts=loaded['worker']['load']['counts']
    summary=f"万人数据库加载与 API 抽样验收通过：{counts['dim_patient']:,} 患者、{counts['dwd_encounter']:,} 就诊、{counts['dwd_imaging_study']:,} 检查、{counts['dwd_imaging_instance']:,} 实例。加载 {loaded['worker']['load_wall_seconds']:.3f} 秒；首次加载流程客户端采样峰值 {loaded['measurement']['sampled_peak_tree_rss_bytes']/1024**2:.2f} MiB。"
    table='\n'.join(f"| {label} | {value['measurement']['seconds']:.3f} 秒 | {value['measurement']['sampled_peak_tree_rss_bytes']/1024**2:.2f} MiB | {value['postgres_server']['sampled_peak_tree_rss_bytes']/1024**2:.2f} MiB |" for label,value in runs.items())
    latencies='\n'.join(f"| {name} | {value['requests']} | {value['median_ms']:.3f} | {value['p95_ms']:.3f} | {value['max_ms']:.3f} |" for name,value in worker['api_latency'].items())
    body=f'''# 数据库分块加载与万人 API 验收

更新：2026-09-20。{summary}

## 实测范围

复用上一轮文件快照，没有重新生成患者或重跑文件 ETL。隔离 schema 为 `{big['schema']}`；正式 `synthea_v1` 和文件指针仍为 `stage-c-verified`。隔离 schema 保留千人和万人两个成功批次，当前指向 `p10000-stream-local-r1`。

| 运行证据 | 客户端完整墙钟 | 客户端树 RSS 峰值 | PostgreSQL 树 RSS 峰值 |
| --- | ---: | ---: | ---: |
{table}

千人首跑加载成功（{small['worker']['load_wall_seconds']:.3f}秒），但 API 的本地时区格式不符导致总体 FAILED；修复后 r2 是 ALREADY_LOADED 幂等重放和 API 复验，不能将其时间称为重新加载耗时。万人 r1 完整加载已提交并通过SQL对账，但后续API验证遇到OperationalError；r2复用已提交批次，幂等重放与API抽样通过。未重写万人数据，未把复验时间当作重新加载时间。初次OperationalError的具体根因未确认；不宣称首次端到端一次通过或多次稳定性实测。

## 分块实现与事务边界

标准库 JSONDecoder 逐块读取数组，只接受对象记录并检查截断、尾随内容和非法分隔符；单条最多 4 Mi 字符。数据库批次最多 1,000 条或 2 MiB 编码输入，单条大记录独立一批。哈希计算使用 1 MiB 文件块。

哈希校验后，一个事务内完成 ODS、处置账本、九张模型表、ANALYZE、SQL 对账和 current_snapshot 切换。分批执行不等于分批提交；失败回滚整个批次。SQL 对账覆盖表数量、三表输入与质量处置平衡、患者/就诊一致性、汇总覆盖及总量、accepted行追溯及源行唯一性；主外键由数据库约束执行。

每张 ODS 源表按一基行号落地；源哈希来自清单。模型表沿用同一个 run_id。相同 run_id、相同清单返回 ALREADY_LOADED，旧批次重放不会切换当前指针；不同内容复用同一 run_id 拒绝。

新增检查查询索引 `(run_id,patient_key,started_at,study_key)` 与实例追溯索引 `(run_id,study_key,source_row)`。SQL work_mem 在加载事务内设为16 MiB；这不是 PostgreSQL 总内存上限。

## 修复的真实问题

千人数据库字段存储的是正确时刻，但 PostgreSQL 默认会话时区将 UTC 时间返回为 +08:00。例如文件 02:34:52+00:00，API 返回 10:34:52+08:00。API 现显式 SET LOCAL TIME ZONE UTC，使接口时间格式与文件契约一致；没有移动事件发生时刻，也没有修改历史数据。

千人 r1 保留为“加载成功、API格式核验失败”的证据；控制器分别记录数据库发布状态和整体验证状态，避免把 API 失败误报为事务回滚。

## API 验收

状态/数量、50位有检查患者、10位零检查患者、相应检查列表、10项检查实例追溯以及未知患者404，合计132次串行请求。预期值直接从文件快照分块读取。样本按文件顺序选择，不是随机统计抽样；并非对全部患者逐一请求。

| 请求类型 | 次数 | 中位数 ms | 经验P95 ms | 最大 ms |
| --- | ---: | ---: | ---: | ---: |
{latencies}

这是 FastAPI ASGI TestClient 连接真实 PostgreSQL 的功能与本机串行延迟观察。每个请求建立数据库连接。没有启动HTTP服务，没有并发负载、网络吞吐或SLA承诺；小样本P95不能推广为生产性能。

## 内存测量含义

客户端进程树每250ms采样并受500 MiB保护；包括解析、加载、文件对照和API测试。PostgreSQL是独立服务，不在客户端树内，另起采样器测服务进程树。服务采样峰值为 {server['sampled_peak_tree_rss_bytes']/1024**2:.2f} MiB，共 {server['sample_count']} 次采样、{server['errors']} 次采样错误。

两边均为各进程RSS相加，共享映射可能重复计数、短暂峰值可能漏采。不得称“整套数据库系统低于500 MiB”，也不得把两个各自时点的峰值相加作为同步峰值。保护是采样检查，不是OS硬限额。服务会缓存页、产生WAL并占用磁盘。

## 测试与恢复

实际万人加载期间另一个TestClient请求返回200，仍可见千人旧批次（0.415秒）；见p10000-reader-during-load.json。

完整回归73项通过，2条依赖弃用警告；新增17项覆盖跨块Unicode与嵌套JSON、非法输入、大小预算、第二批写入失败、事务回滚及旧快照可见、幂等重放、重算哈希后错误数量/行号/汇总和格式异常拦截。

万人尚未做物理断电、强制终止、多个发布者竞争或重复大规模加载；现有故障注入使用小样本隔离schema。控制器结束后重新读取数据库指针，区别“未提交”与“已提交但后续验证失败”；不能仅凭进程退出码推断回滚。

## 复现与后续

```powershell
.\\.venv\\Scripts\\python.exe -m scripts.benchmark_database --run-dir output/scale-stream-final/runs/p10000-stream-local-r1 --schema synthea_scale_db_new --name p10000-db-new
```

使用新证据名和新的隔离schema可重新测完整加载；复用本轮schema会触发幂等重放。不要重建数据库、重生万人数据或覆盖正式发布。

下一步是 FHIR 规模导出的分块实现与核验，再推进 Spark 同口径对照和 Airflow 调度。完整 FHIR series 仍缺可信 StudyInstanceUID，官方 Validator 和 FHIR 服务未验收。

证据：[千人首跑](p1000-db-r1.json)、[千人复验](p1000-db-r2.json)、[万人加载](p10000-db-r1.json)、[万人API复验](p10000-db-r2.json)。上一轮文件ETL见[第十一章](../html/11-streaming.html)。
'''
    (folder/'README.md').write_text(body,encoding='utf-8')
    prior=(ROOT/'docs/html/11-streaming.html').read_text(encoding='utf-8')
    style=re.search(r'<style>(.*?)</style>',prior,re.S).group(1)
    sections=[]
    for part in body.split('\n## ')[1:]:
        title,text=part.split('\n',1); rendered=[]
        for paragraph in text.strip().split('\n\n'):
            if paragraph.startswith('```'):
                rendered.append('<pre>'+html.escape('\n'.join(paragraph.splitlines()[1:-1]))+'</pre>')
            elif paragraph.startswith('|'):
                lines=paragraph.splitlines(); rows=[]
                for i,line in enumerate(lines):
                    if i==1: continue
                    tag='th' if i==0 else 'td'
                    rows.append('<tr>'+''.join(f'<{tag}>'+html.escape(cell.strip())+f'</{tag}>' for cell in line.strip('|').split('|'))+'</tr>')
                rendered.append('<div class="table-wrap"><table>'+''.join(rows)+'</table></div>')
            else:
                escaped=html.escape(paragraph).replace('\n','<br>')
                escaped=re.sub(r'`([^`]+)`',r'<code>\1</code>',escaped)
                escaped=re.sub(r'\[([^]]+)\]\(([^)]+\.json)\)',r'<a href="../stage-f/\2">\1</a>',escaped)
                rendered.append('<p>'+escaped+'</p>')
        sections.append('<section><h2>'+html.escape(title)+'</h2>'+''.join(rendered)+'</section>')
    page=f'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>第十二章 · 数据库分块加载与API</title><style>{style}</style></head><body><header><div class="eyebrow">第十二章 · 2026-09-20</div><h1>从万人文件快照到数据库查询</h1><p>{summary}</p></header><nav><a href="01-overview.html">项目全貌</a><a href="11-streaming.html">文件ETL</a><a href="12-database-scale.html" aria-current="page">数据库/API</a><a href="../stage-f/README.md">证据与复现</a></nav><main>{''.join(sections)}</main></body></html>'''
    (ROOT/'docs/html/12-database-scale.html').write_text(page,encoding='utf-8')
    for path in (ROOT/'docs/html').glob('*.html'):
        content=path.read_text(encoding='utf-8')
        if '12-database-scale.html' not in content:
            path.write_text(content.replace('</nav>','<a href="12-database-scale.html">12 数据库/API</a></nav>'),encoding='utf-8')


if __name__=='__main__': main()

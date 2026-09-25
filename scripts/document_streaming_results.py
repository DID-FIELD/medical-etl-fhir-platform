"""Build the streaming handoff and chapter from saved measurement evidence."""
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


def main():
    evidence = ROOT / 'docs/stage-e'
    names = ['p1000-stream-utc-r2', 'p1000-stream-local-r1', 'p10000-stream-local-r1']
    results = [json.loads((evidence / (name+'.json')).read_text(encoding='utf-8')) for name in names]
    if not all(r['passed'] for r in results):
        raise ValueError('Cannot document success without successful evidence')
    big = results[-1]; m = big['manifest']; counts = m['counts']
    rows = []
    for name, result in zip(names, results):
        measurement = result['measurement']
        rows.append(f"| {name} | {measurement['seconds']:.3f} 秒 | {measurement['sampled_peak_tree_rss_bytes']/1024**2:.2f} MiB | 通过 |")
    summary = f"万人完整文件 ETL 已通过：{counts['dim_patient']:,} 患者、{counts['dwd_encounter']:,} 就诊、{counts['dwd_imaging_study']:,} 检查、{counts['dwd_imaging_instance']:,} 实例；进程树采样 RSS 峰值 {big['measurement']['sampled_peak_tree_rss_bytes']/1024**2:.2f} MiB，低于 500 MiB，墙钟 {big['measurement']['seconds']:.3f} 秒。"
    body = f'''# 流式 ETL：500 MiB 目标验收

更新：2026-09-20。{summary}

## 实测结果

| 独立批次 | 完整进程墙钟 | 采样 RSS 峰值 | 结果 |
| --- | ---: | ---: | --- |
{chr(10).join(rows)}

复用原 ZIP，未重新生成。输入为 11,476 患者、677,836 就诊、1,036,348 影像实例，共 1,725,660 行。输出包含三张 ODS JSON、九张模型表的 JSON/Parquet、逐行处置账本、警告和带哈希的运行清单。全部对账与 Parquet 回读检查通过。

## 内存为何下降

ZIP 中 CSV 分批写入磁盘 SQLite；关联患者及共享业务键的冲突记录保持在同一连通组；每批复用原质量规则，结果写回磁盘；最终按业务键输出 JSON 和 Parquet，并分批读回检查。哈希计算也分块进行。

真实数据单患者最多 29,846 行，原型 10,000 行批次在千人档主动拒绝。现在每批最多 40,000 行、原始 JSON 32 MiB；超大关联组仍拒绝，不能拆散规避冲突检测。500 MiB 外部保护值没有提高。Windows SQLite 显式关闭后再清理临时目录。

这里不是任意规模的恒定内存保证：患者连通关系索引随患者数增长；极大患者组会失败。当前结论仅适用于保存的本机数据和参数。

## 正确性与时区

千人 UTC 对照的九张模型表、完整处置账本、源清单、输出数量和质量计数全部与旧版一致。JSON 空白格式不同，不以文件字节相等代替语义比较。

本机生成器的生日是 UTC+08 日历日期，事件时间是 UTC。显式 `--birth-date-offset +08:00` 仅改变出生日期边界比较；事件时间及日×模态统计仍采用 UTC。千人 45 条误隔离就诊恢复，70,229 条就诊全部有效；默认参数仍是 +00:00，不对未知来源推测时区。

新增测试覆盖跨批日汇总、源行映射、晚到重复冲突、跨患者 series/instance 冲突、生日边界、空影像、超大组拒绝、失败保留 current、临时文件清理、内存保护和外部终止状态。含隔离 schema 数据库测试共 56 项通过，2 条依赖弃用警告。

## 测量与边界

每 250ms 采样 ETL 启动器及其后代 RSS 之和；共享页可能重复计算，短暂峰值可能漏采。这是采样保护，不是操作系统硬内存上限。500 MiB、系统可用内存 700 MiB 和 30 分钟超时均可触发终止。控制器将非正常退出记录为 FAILED，保留已存在的具体异常。

计时包含导入、读 ZIP、SQLite 暂存及索引、质量规则、各层输出、读回、哈希和清理，不含源数据生成。新方案以磁盘 I/O 换取内存；千人原版约 10.7 秒，新版约 24 秒。万人旧版被终止，其失败耗时不能用于吞吐比较。首次万人运行期间有短暂测试后台负载，不视为独占机器基准。

正式文件发布仍是 `output/synthea/current.json → stage-c-verified`，数据库主发布不变。独立验证目录是 `output/scale-stream-final/`。主 `run_etl.py` 保持原入口，流式入口使用下面的命令。

大规模 PostgreSQL 加载/API、FHIR 导出、Spark 同口径、Airflow 均待验证；当前全量数据库加载器不具备已验证的 500 MiB 保证。外部强制终止可能留下磁盘暂存文件；本轮未自动删除历史失败目录。

## 复现

```powershell
.\\.venv\\Scripts\\python.exe -m scripts.benchmark_streaming --population 10000 --run-id p10000-stream-local-new --birth-date-offset +08:00
```

必须使用新的 run_id；已有产物和证据不会覆盖。不要重新生成万人数据，不要覆盖正式发布。

证据：{', '.join('['+n+']('+n+'.json)' for n in names)}。旧失败历史仍保存在第十章。

下一步：让数据库加载也支持分块消费，先在隔离 schema 验证流式快照加载，再做万人数据库/API验收；随后推进 FHIR规模、Spark 和 Airflow。
'''
    (evidence/'STREAMING.md').write_text(body, encoding='utf-8')
    old = (ROOT/'docs/html/10-scale.html').read_text(encoding='utf-8')
    style = re.search(r'<style>(.*?)</style>', old, re.S).group(1)
    import html
    table = ''.join(f'<tr><td>{html.escape(n)}</td><td>{r["measurement"]["seconds"]:.3f} 秒</td><td>{r["measurement"]["sampled_peak_tree_rss_bytes"]/1024**2:.2f} MiB</td></tr>' for n,r in zip(names,results))
    sections = []
    for part in body.split('\n## ')[2:]:
        title, text = part.split('\n',1)
        paragraphs = ''
        for paragraph in text.strip().split('\n\n'):
            if paragraph.startswith('```'):
                paragraphs += '<pre>' + html.escape('\n'.join(paragraph.splitlines()[1:-1])) + '</pre>'
            else:
                escaped = html.escape(paragraph).replace('\n', '<br>')
                escaped = re.sub(r'`([^`]+)`', r'<code>\1</code>', escaped)
                escaped = re.sub(r'\[([^]]+)\]\(([^)]+\.json)\)', r'<a href="../stage-e/\2">\1</a>', escaped)
                paragraphs += '<p>' + escaped + '</p>'
        sections.append('<section><h2>'+html.escape(title)+'</h2>'+paragraphs+'</section>')
    page = f'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>第十一章 · 流式 ETL 与内存验收</title><style>{style}td{{overflow-wrap:anywhere}}</style></head><body><header><div class="eyebrow">第十一章 · 2026-09-20</div><h1>让万人 ETL 完整跑通</h1><p>{summary}</p></header><nav><a href="01-overview.html">项目全貌</a><a href="10-scale.html">规模失败历史</a><a href="11-streaming.html" aria-current="page">流式验收</a><a href="../stage-e/STREAMING.md">证据与复现</a></nav><main><section><h2>完整文件 ETL 实测</h2><div class="table-wrap"><table><tr><th>批次</th><th>墙钟</th><th>进程树采样峰值</th></tr>{table}</table></div><p>九张模型表、三张 ODS、处置账本、Parquet 回读及哈希全部完成。500 MiB 是采样目标，不是操作系统硬限额。</p></section>{''.join(sections)}</main></body></html>'''
    (ROOT/'docs/html/11-streaming.html').write_text(page,encoding='utf-8')
    (evidence/'streaming-test-results.txt').write_text('2026-09-20\nRUN_DATABASE_TESTS=1 .venv/Scripts/python.exe -m pytest -q --tb=short\n56 passed, 2 warnings in 11.23s\nWarnings: Starlette/httpx and anyio BlockingPortal deprecations.\n',encoding='utf-8')


if __name__ == '__main__':
    main()

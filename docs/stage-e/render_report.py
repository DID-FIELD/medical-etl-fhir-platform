from pathlib import Path
import json,re
from html import escape
ROOT=Path(__file__).resolve().parents[2]
css=re.search(r'<style>(.*?)</style>',(ROOT/'docs/html/01-overview.html').read_text(encoding='utf-8'),re.S).group(1)
rows=[];perf=[]
for n in [100,1000,10000]:
 p=ROOT/f'docs/stage-e/p{n}.json'
 if not p.exists():continue
 r=json.loads(p.read_text());source=r.get('source',{});m=r.get('etl_manifest',{});c=m.get('counts',{})
 def count(table):return f"{source[table]['rows']:,}" if table in source else '待完成'
 rows.append(f"<tr><td>{n:,}</td><td>{escape(r['status'])}</td><td>{count('patients')}</td><td>{count('encounters')}</td><td>{count('imaging_studies')}</td><td>{c.get('dwd_imaging_study','未产出')}</td></tr>")
 gen=r.get('measurements',{}).get('generation',{});etl=r.get('measurements',{}).get('file_etl',{})
 memory=gen.get('sampled_peak_tree_rss_bytes');memory=f'{memory/2**20:,.0f} MiB' if memory else '启动器漏计，未采用'
 samples=[];memories=[]
 for repeat in [2,3]:
  p=ROOT/f'docs/stage-e/p{n}-r{repeat}.json'
  if p.exists():
   rr=json.loads(p.read_text());samples.append(str(rr['measurement']['seconds']));memories.append(rr['measurement']['sampled_peak_tree_rss_bytes'])
 memory_etl=f'{max(memories)/2**20:,.0f} MiB' if memories else ('未测得' if 'sampled_peak_tree_rss_bytes' not in etl else f"{etl['sampled_peak_tree_rss_bytes']/2**20:,.0f} MiB")
 size=sum(s['bytes'] for s in source.values())/2**20
 perf.append(f"<tr><td>{n:,}</td><td>{size:,.2f} MiB</td><td>{gen.get('seconds','待完成')} s</td><td>{etl.get('seconds','待完成')} s</td><td>{' / '.join(samples) or '待完成'} s</td><td>{memory}</td><td>{memory_etl}</td></tr>")
content='''<section><span class="tag">E 阶段 · Synthea本机分级实测</span><h2>可以生成大批数据，但要分别数人、就诊、检查和实例</h2><p class="lead">固定Synthea 4.0.0，按100 → 1,000 → 10,000目标存活人数分档生成，再用同一条ETL处理。以下数字来自各档运行记录，不是复制小样本放大。</p><p>本轮范围为患者、就诊、影像三张CSV与文件数仓；不包含大规模数据库加载、API吞吐或FHIR导出性能。现有主库批次保持不变。</p></section>
<section><h2>数据实际有多大？</h2><div class="table-wrap"><table><thead><tr><th>请求人数</th><th>运行状态</th><th>患者行数</th><th>就诊行数</th><th>影像实例行数</th><th>检查产出数</th></tr></thead><tbody>'''+''.join(rows)+'''</tbody></table></div><p>Synthea默认运行过程中可能记录死亡患者，再补足目标存活人数。因此-p不是最终患者文件行数。100人档实际110人；千人档实际1159人。</p><p>千人档有89,354个影像实例，但只有2,239次检查；样本中最大一次检查有500个实例。不能根据100人档简单乘10估算所有记录量。</p></section>
<section><h2>生成和处理分别花多久？</h2><div class="table-wrap"><table><thead><tr><th>请求人数</th><th>三表CSV大小</th><th>生成</th><th>ETL首跑</th><th>ETL复跑2 / 3</th><th>生成进程树RSS峰值</th><th>ETL复跑RSS峰值</th></tr></thead><tbody>'''+''.join(perf)+'''</tbody></table></div><p>计时为子进程启动到退出的墙钟时间；ETL包含Python启动、解压解析、分层、JSON/Parquet写入与回读，不含生成、压缩打包或数据库。内部manifest.elapsed_seconds更短，不能混用。</p><p>内存每250毫秒采样一次进程树RSS之和，可能重复计算共享页，并非精确瞬时峰值。100/1000档生成时初版监测只量到Windows启动器，因此其内存值不采用；后续ETL复跑使用修正后的子进程监测。</p><div class="note">单台日常使用电脑的小次数测量，受系统负载、缓存和进程启动影响；不是生产SLA或分布式“大数据平台”吞吐结论。</div></section>
<section><h2>结论：生成成功，但万人ETL触及本机内存边界</h2><p>万人档生成11,476位患者、677,836条就诊、1,036,348条影像实例，共1,725,660条源记录，CSV合计563.69 MiB。生成耗时418.229秒，进程树采样峰值约1.61 GiB。</p><p>文件ETL首跑在5.742秒时被内存保护终止，采样RSS约4.23 GiB；去掉重复分组结构后复测在5.482秒时仍以约4.04 GiB终止，超过3.5 GiB阈值。采样有间隔，因此实际记录可超过阈值。这个5.742秒是<strong>停止前用时，不是成功处理时间</strong>。没有可用的万人DWD或FHIR产出。</p><p>失败目录只有RUNNING清单，没有分层文件；结合源码全量read/list读取方式，瓶颈位于输入读入阶段。外部终止来不及执行异常处理，清单仍是RUNNING；应结合控制器ETL_FAILED和stop_reason识别中断，不能按RUNNING字面继续等待或当作成功。</p><p>千人档三次ETL为10.790、10.684、10.659秒，修正后的复跑RSS峰值约661 MiB，两次复跑的数量与JSON哈希均一致。这次只证明千人档文件ETL可重复完成，没有证明数据库同规模性能。</p><div class="note">下一步应优先改为分块或流式读取，并处理外部中断状态和日期时区契约，再复测已有万人归档。无需重新生成，更不宜单纯调高内存阈值。</div></section><section><h2>可复现的配置</h2><ul><li>Synthea v4.0.0，JAR约192 MiB，SHA-256与GitHub发布资产digest核对。</li><li>随机种子和临床人员种子均20260920；reference/end均20260920；地区Massachusetts。</li><li>导出最近10年历史，只生成患者、就诊、影像三张CSV；关闭原生FHIR导出。</li><li>Java堆上限2 GiB，2个生成线程。16 GiB级本机，开始时可用内存约5 GiB，F盘空闲约222 GiB。</li><li>开始前检查可用内存和磁盘；运行中可用内存低于700 MiB或被测进程树RSS超过3.5 GiB时停止本次被测进程。</li></ul><p>每档使用独立目录，记录配置、命令、JAR及CSV哈希、生成日志和ETL清单。双线程导出行序不保证跨运行完全相同；固定种子并不等于CSV字节顺序必然一致。</p></section>
<section><h2>质量问题也随数据规模出现</h2><p>100人档有3条、千人档有45条就诊被现有“早于出生日期”规则隔离。例子中，出生日期为2018-08-21，而就诊时间是2018-08-20T17:56:58Z，两者可能落在不同的日期时区。</p><p>已核对v4.0.0源码：出生日期导出使用默认时区，就诊时间强制UTC。本机Windows默认时区为UTC+08，这意味着跨日边界可能触发当前规则的误隔离。报告保留处置数量，不把它直接称为病历数据本身错误，也不静默放宽规则。</p><p>诊断比较发现，千人档45条记录换算到UTC+08日期后均不早于生日。后续应显式固定生成器时区，或在数据契约中明确出生日期所属时区，再比较不同粒度的日期。质量问题和性能数据必须一起解释。</p></section>
<section><h2>面试怎么讲更准确</h2><div class="block"><p>“我不仅用了官网下载的小样本，还固定版本、seed和模拟日期，分档生成数据。患者数、就诊数和影像实例数增长并不一致，所以我记录实际行数、文件大小、检查去重数和质量隔离数。”</p><p>“我分别测生成和ETL，记录进程树内存；发现Windows启动器会导致漏计后，修正采样方式并补测。日期字段的默认时区与UTC时间也会影响质量规则，需要写入数据契约。”</p></div></section>
<section><h2>复现入口与证据</h2><pre>.\\.venv\\Scripts\\python.exe -m pip install -r docs/stage-e/requirements-benchmark.txt
.\\.venv\\Scripts\\python.exe -m scripts.benchmark_synthea --population 1000
.\\.venv\\Scripts\\python.exe -m scripts.benchmark_repeat_etl --population 1000 --repeat 2</pre><p>每个输出目录只允许创建一次；已有结果不会被覆盖。重跑时需要显式安排新批次目录，不要直接删除已有证据。</p><p><a href="../stage-e/README.md">测试范围与结论</a> · <a href="../stage-e/p100.json">100档</a> · <a href="../stage-e/p1000.json">1000档</a> · <a href="../stage-e/p10000.json">10000档</a> · <a href="../stage-e/generator.json">生成器版本及哈希</a></p><p class="source">来源：<a href="https://github.com/synthetichealth/synthea/wiki/Basic-Setup-and-Running">Synthea官方运行说明</a>、<a href="https://github.com/synthetichealth/synthea/blob/v4.0.0/src/main/java/org/mitre/synthea/export/ExportHelper.java">v4.0.0日期导出源码</a>。</p></section>'''
nav='<a href="01-overview.html">项目全貌</a><a href="09-reliability.html">异常恢复</a><a href="10-scale.html" aria-current="page">规模实测</a>'
(ROOT/'docs/html/10-scale.html').write_text(f'<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>第十章 · Synthea分级规模实测</title><style>{css}</style></head><body><header><div class="eyebrow">STAGE E / SCALE MEASUREMENTS · 2026.09.20</div><h1>Synthea放大之后，数据和资源如何变化</h1><p>以实际生成、分层产出和进程资源记录回答规模问题。</p></header><nav>{nav}</nav><main>{content}<footer class="footer"><a href="09-reliability.html">← 异常与恢复</a><a href="01-overview.html">项目全貌</a></footer></main></body></html>',encoding='utf-8')
print('Stage E HTML rendered')

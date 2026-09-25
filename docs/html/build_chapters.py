"""Build all offline teaching pages from chapter sources; safe from any cwd."""
from pathlib import Path
from html import escape

ROOT = Path(__file__).resolve().parent
CHAPTERS = [
 ('01-overview.html','项目全貌','先看问题、两条执行路径和当前证据'),
 ('02-data-flow.html','数据逐层流转','用一位患者、两次检查讲清每一层'),
 ('03-reconciliation.html','数据量与对账','区分粒度、记录守恒与内容等价'),
 ('04-roadmap.html','实施进度与验收','按证据理解已完成工作和剩余边界'),
 ('05-interview.html','面试复习','把数据、问题、处理和验证连起来讲'),
 ('06-stage-b.html','源数据与建模','历史 B 阶段：56 个字段和粒度契约'),
 ('07-stage-c.html','小样本闭环','历史 C 阶段：文件、数据库与 API 的第一条闭环'),
 ('08-fhir.html','FHIR 资源映射','从早期样本到万人摘要导出和 Spark 消费'),
 ('09-reliability.html','异常与恢复','从事务回滚到调度重试和源文件复核'),
 ('10-scale.html','早期规模失败复盘','保留失败原因，连接后续流式修复'),
 ('11-streaming.html','流式 ETL','磁盘暂存如何换来可控内存'),
 ('12-database-scale.html','数据库与 API','分批加载、事务边界和串行接口验收'),
 ('13-spark.html','完整 Spark ETL','严格读入、质量治理与双向多重集对账'),
 ('14-airflow.html','Airflow 与重试','成功产物和任务成功为什么是两件事'),
 ('15-consumers.html','Spark 产物消费','Parquet 目录如何接入 FHIR、数据库和 API'),
]

def build():
 css=(ROOT/'chapters.css').read_text(encoding='utf-8')
 for i,(filename,title,subtitle) in enumerate(CHAPTERS):
  content=(ROOT/'chapters'/filename).read_text(encoding='utf-8')
  nav=''.join(f'<a href="{f}"'+(' aria-current="page"' if j==i else '')+f'>{j+1:02d} · {escape(t)}</a>' for j,(f,t,_) in enumerate(CHAPTERS))
  previous=f'<a href="{CHAPTERS[i-1][0]}">← {escape(CHAPTERS[i-1][1])}</a>' if i else '<span>从业务问题开始，再看数据与实现。</span>'
  following=f'<a href="{CHAPTERS[i+1][0]}">{escape(CHAPTERS[i+1][1])} →</a>' if i+1<len(CHAPTERS) else '<a href="01-overview.html">返回总览 →</a>'
  notice='<p class="reading">教材更新于 2026-09-25 · 实测证据截至 2026-09-24 · <a href="01-overview.html#status">当前完成度</a> · <a href="04-roadmap.html#next">剩余工作</a></p>'
  doc=f'''<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{i+1:02d} · {escape(title)} | 医疗数据项目教学</title><style>{css}</style></head>
<body><header><div class="eyebrow">MEDICAL DATA ENGINEERING / SYNTHEA</div><h1>{i+1:02d} / {escape(title)}</h1><p>{escape(subtitle)}</p></header><nav aria-label="章节目录">{nav}</nav><main>{notice}
{content}
<footer class="footer">{previous}{following}</footer><p class="small">离线阅读，无外部脚本或字体依赖。历史数字按原阶段标注，不能与新批次混算。</p></main></body></html>
'''
  (ROOT/filename).write_text(doc,encoding='utf-8')
 print(f'Built {len(CHAPTERS)} offline chapters.')

if __name__=='__main__':
 build()

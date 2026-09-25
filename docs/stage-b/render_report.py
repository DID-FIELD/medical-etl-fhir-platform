"""Render the stage-B evidence and proposed contract as offline documentation."""
import json
import re
from pathlib import Path
from html import escape as esc

root=Path('docs/stage-b')
r=json.loads((root/'profile.json').read_text(encoding='utf-8'))
# Target descriptions are design decisions, not claims of implemented columns.
common={
'patients':{
 'Id':('string','必需；唯一','dim_patient.patient_key 的源业务键；原值仅受限映射'),
 'BIRTHDATE':('date','必需；可解析','ODS 保留精确值；dim_patient.birth_year'),
 'DEATHDATE':('date','可空；非空须可解析','ODS 保留；首轮不进入检查概览'),
 'GENDER':('string/code','必需；值域待固定','dim_patient.gender；FHIR 另做代码映射'),
},
'encounters':{
 'Id':('string','必需；唯一','dwd_encounter.encounter_key 的源业务键'),
 'PATIENT':('string','必需；患者外键','dwd_encounter.patient_key'),
 'START':('UTC timestamp','必需；可解析','dwd_encounter.start_at'),
 'STOP':('UTC timestamp','允许未结束就诊为空；非空不早于 START','dwd_encounter.stop_at'),
 'ENCOUNTERCLASS':('string/code','保留源代码；值域监测','dwd_encounter.encounter_class'),
 'CODE':('string/code','保留为字符串','dwd_encounter.code'),
 'DESCRIPTION':('string','可空；不作为主键','dwd_encounter.description'),
 'ORGANIZATION':('string','首轮不检查未接入组织表外键','ODS；后续组织维度候选'),
 'PROVIDER':('string','首轮不检查未接入医生表外键','ODS；后续医生维度候选'),
 'PAYER':('string','首轮不检查未接入支付方表外键','ODS；本期不接入支付模型'),
 'BASE_ENCOUNTER_COST':('decimal candidate','首轮保留原串；未做金额校验','ODS；本期不聚合费用'),
 'TOTAL_CLAIM_COST':('decimal candidate','首轮保留原串；未做金额校验','ODS；本期不聚合费用'),
 'PAYER_COVERAGE':('decimal candidate','首轮保留原串；未做金额校验','ODS；本期不聚合费用'),
 'REASONCODE':('string/code','可空','ODS；可选扩展就诊原因'),
 'REASONDESCRIPTION':('string','可空','ODS；可选扩展就诊原因'),
},
'imaging_studies':{
 'Id':('string','必需；在实例源表中可重复','dwd_imaging_study.study_key 的源业务键'),
 'DATE':('UTC timestamp','必需；同检查一致','dwd_imaging_study.started_at；源串留 ODS'),
 'PATIENT':('string','必需；与关联就诊患者一致','dwd_imaging_study.patient_key'),
 'ENCOUNTER':('string','必需；就诊外键','dwd_imaging_study.encounter_key'),
 'SERIES_UID':('UID string','必需；同序列父检查一致','dwd_imaging_series.series_uid'),
 'INSTANCE_UID':('UID string','必需；实例业务键','dwd_imaging_instance.instance_uid'),
 'MODALITY_CODE':('string/code','序列内一致；不可转数字','dwd_imaging_series.modality_code；派生 bridge'),
 'MODALITY_DESCRIPTION':('string','可空；不作为键','ODS；可选模态描述字段'),
 'BODYSITE_CODE':('string/code','序列内一致','dwd_imaging_series.body_site_code'),
 'BODYSITE_DESCRIPTION':('string','序列内一致；描述不作键','dwd_imaging_series.body_site_description'),
 'SOP_CODE':('string/code','首轮保留；标准值域另验','dwd_imaging_instance.sop_code'),
 'SOP_DESCRIPTION':('string','可空；描述不作键','ODS；可选实例描述字段'),
 'PROCEDURE_CODE':('string/code','可能与实例/检查关联；不任取 first','ODS；后续按实际关系设计关联表'),
}}
restricted={'SSN','DRIVERS','PASSPORT','FIRST','MIDDLE','LAST','PREFIX','SUFFIX','MAIDEN','ADDRESS','ZIP','LAT','LON','BIRTHPLACE'}
for c in r['tables']['patients']['fields']:
 if c not in common['patients']:
  common['patients'][c]=('string (raw)','可空；首轮不作业务校验', '仅受限 ODS，首轮不导出到分析/服务层' if c in restricted else 'ODS 保留，首轮不用于检查主题模型')
md=['# Synthea 字段字典与目标映射（B 阶段）','',
'本机源文件共 56 列。空值统计为本次剖析实测，目标字段与必填/处置规则为设计提案，尚未写入数据库。','',
'所有源列在受限 ODS 保留原字符串；下表的类型是目标规范化类型。基数仅统计非空值，不展示姓名等原始值。','']
html_tables=[]
for name,t in r['tables'].items():
 md += [f'## {name}.csv','',f"{t['rows']:,} 条记录 · {t['columns']} 列 · {t['bytes']:,} 字节。",'',
 '| 源字段 | 非空基数 | 空值数 | 目标类型 | 规则提案 | 去向提案 |','| --- | ---: | ---: | --- | --- | --- |']
 rows=[]
 for c,f in t['fields'].items():
  typ,rule,target=common[name][c]
  md.append(f"| {c} | {f['distinct_nonempty']} | {f['missing']} | {typ} | {rule} | {target} |")
  rows.append('<tr>'+''.join(f'<td>{esc(str(v))}</td>' for v in [c,f['missing'],f['distinct_nonempty'],typ,target])+'</tr>')
 md.append('')
 html_tables.append(f'<details><summary>{name}.csv · {t["columns"]} 列（点击展开）</summary><div class="table-wrap"><table><thead><tr><th>源字段</th><th>空值</th><th>非空基数</th><th>目标类型</th><th>去向提案</th></tr></thead><tbody>'+''.join(rows)+'</tbody></table></div></details>')
(root/'FIELD_DICTIONARY.md').write_text('\n'.join(md),encoding='utf-8')
css=re.search(r'<style>(.*?)</style>',Path('docs/html/01-overview.html').read_text(encoding='utf-8'),re.S).group(1)
modalities=''.join(f'<tr><td>{esc(k)}</td><td>{v}</td><td>{r["distributions"]["instance_rows_by_modality"][k]}</td></tr>' for k,v in r['distributions']['studies_by_modality'].items())
tables=''.join(f'<tr><td>{k}.csv</td><td>{v["rows"]:,}</td><td>{v["columns"]}</td><td>{v["bytes"]:,}</td></tr>' for k,v in r['tables'].items())
content='''<section><span class="tag">阶段 B · 已完成源数据剖析</span><h2>这次从原始三表出发，确定如何建模</h2><p class="lead">108 位患者 → 5,571 次就诊；其中 379 次就诊发生了 413 次影像检查，共 478 个实例。</p><p>本轮实际读取 Synthea ZIP、检查字段和关联，生成证据与模型草案。<strong>没有执行数据库建表、迁移或修改 ETL 主流程。</strong>下文分别标注实测和模型目标。</p><div class="table-wrap"><table><thead><tr><th>源文件</th><th>记录数</th><th>列数</th><th>CSV 字节数</th></tr></thead><tbody>'''+tables+'''</tbody></table></div></section>
<section><h2>01 / 数据粒度，已经被样本验证</h2><div class="grid"><div class="block"><span class="metric">348</span>次检查，每次 1 个实例</div><div class="block"><span class="metric">65</span>次检查，每次 2 个实例</div><div class="block"><span class="metric">478</span>不同实例，共 413 次检查</div></div><div class="equation">348 × 1 + 65 × 2 = 478</div><p>本样本每次检查恰好一个序列、一个模态。但数仓仍需支持一项检查多个序列/模态，不能把一次观察到的关系固化为业务限制。</p><div class="note good">65 条差额已解释为合法的检查内实例展开。检查表应保留 413 次检查，实例表应保留 478 个实例，两者并存。</div></section>
<section><h2>02 / 完整患者和就诊不能被影像筛选掉</h2><pre>患者：108 = 96 有影像 + 12 无影像
就诊：5,571 = 379 有影像 + 5,192 无影像
检查：413 次，属于上述 379 次就诊
实例：478 个，属于上述 413 次检查</pre><p><strong>建模选择：</strong>患者 DIM 保留全部有效患者。DWS 从 DIM 左连接检查汇总，没有影像者检查次数为 0、最近检查时间为 NULL。</p><p>之前探索版仅构建了 96 位影像患者的维度。下一阶段按本方案，患者维度、患者汇总和 ADS 的样本目标都变成 108 行。这是覆盖范围改变，不是凭空增加数据。</p></section>
<section><h2>03 / 主外键和时间，检查到了什么？</h2><div class="grid"><div class="block"><strong>键检查通过</strong>患者/就诊键无空值或重复；实例 UID 无重复；三表无完全重复行。</div><div class="block"><strong>关联检查通过</strong>未知患者、未知就诊、检查与就诊患者不一致均为 0。</div><div class="block"><strong>时间检查通过</strong>所查非空时间均可解析；就诊倒序与影像超出就诊区间均为 0。</div></div><p>影像关联就诊后仍为 478 行；就诊关联患者后仍为 5,571 行，符合预期的多对一关联。</p><div class="note">必填规则不能一刀切：DEATHDATE 缺失 99 条，就诊原因代码缺失 2,044 条。它们在首轮设计中允许为空，不应直接作为脏数据隔离。费用、术语值域、其他维度外键和 FHIR 符合性尚未验证。</div></section>
<section><span class="tag plan">B 阶段模型契约 · 历史设计</span><h2>04 / 源数据应该落到哪些表？</h2><div class="flow"><div><strong>ODS：三张源表各自保存</strong><span>108 患者 / 5,571 就诊 / 478 影像行，携带批次与来源。</span></div><div class="arrow">↓</div><div><strong>DIM：患者维度 · 目标 108 行</strong><span>全部有效患者。跨表统一替代键；直接标识信息留在受限区。</span></div><div class="arrow">↓</div><div><strong>DWD：就诊 + 检查 + 序列 + 实例</strong><span>目标 5,571 / 413 / 413 / 478 行。每张表粒度独立，保留父子关联。</span></div><div class="arrow">↓</div><div><strong>Bridge：检查与模态的去重关系</strong><span>支持一项检查多个模态，本样本目标 413 行。</span></div><div class="arrow">↓</div><div><strong>DWS / ADS：统计与患者概览</strong><span>每患者各 108 行；SUM(exam_count) = 413。零检查患者保留。</span></div></div><p class="small">以上为 B 阶段制定的验收目标，已在 C 阶段实际落库核对；若质量规则改变，应记录原因重新对账。所有表按 run_id 组成同一快照，查询只读成功发布批次。</p><details><summary>为什么不用“患者 + 日期 + 检查类型”作为检查键？</summary><p>本样本未出现该简化组合的重复，但同日同类型检查在业务上不能被假定只发生一次。使用源检查 Id，并加源系统命名空间，才能表达稳定身份。</p></details><details><summary>为什么保留完整时间，而不是只取日期？</summary><p>日期截断会丢失就诊顺序与检查时间。ODS 保留原串；事件时间规范为 UTC，日汇总从完整时间派生，明确统计时区。</p></details></section>
<section><h2>05 / 模态计数也要分清粒度</h2><div class="table-wrap"><table><thead><tr><th>源模态代码</th><th>检查数</th><th>实例数</th></tr></thead><tbody>'''+modalities+'''</tbody></table></div><p>OP 的 45 次检查对应 90 个实例，OPT 的 20 次检查对应 40 个实例。按实例统计“检查量”就会放大这些类别。</p><p>本样本各模态检查数恰好合计 413；未来一项检查涉及多个模态时，各组加总可能超过去重总检查数，不能硬性要求相等。</p></section>
<section><span class="tag">真实源记录追溯 · 不展示姓名</span><h2>06 / 找到一条确切的流转样例</h2><pre>patients.csv 第 14 条数据记录
  ↓ 患者引用
encounters.csv 第 524 条数据记录
  ↓ 就诊引用
imaging_studies.csv 第 35、36 条数据记录
  ↓ 同一检查、同一序列、两个不同实例（OP）
目标结果：1 个检查 + 1 个序列 + 2 个实例
患者检查次数应增加 1，而不是 2</pre><p>序号从 1 开始、不含表头，指 CSV 记录位置，不一定是物理文本行号。配合源文件校验值才能准确回查。这些是 B 阶段的源记录证据；C 阶段已在仓库和 API 中验证该检查返回两个实例来源。</p></section>
<section><h2>07 / 全部 56 个字段的落点</h2><p>空值与基数为实测；类型与去向为建模提案。未进入第一期业务模型的字段仍在 ODS 保留。</p>'''+''.join(html_tables)+'''</section>
<section><h2>阶段结论与下一步</h2><p>B 阶段已经形成数据剖析、关系验证、字段字典、模型契约、指标口径和真实追溯例子。后续 C 阶段已按此契约实现核心闭环，新数据库和空输入问题已处理，22 个测试与17项核验通过。FHIR专项已完成摘要导出，最新32项测试通过，见第八章。</p><p><a href="../stage-b/DATA_MODEL.md">完整模型草案</a> · <a href="../stage-b/FIELD_DICTIONARY.md">字段字典</a> · <a href="../stage-b/profile.json">机器可读证据</a> · <a href="../stage-b/profile_synthea.py">复现脚本</a></p><p class="source">来源：本机 Synthea 原始 CSV 剖析；设计参考 <a href="https://github.com/synthetichealth/synthea/wiki/CSV-File-Data-Dictionary">官方 CSV 字典</a>。本轮不属于数据库或大规模性能测试。</p></section>'''
nav=''.join(f'<a href="{f}">{text}</a>' for f,text in [('01-overview.html','项目全貌'),('02-data-flow.html','数据流转'),('03-reconciliation.html','数量对账'),('04-roadmap.html','实施路线'),('05-interview.html','面试复习'),('06-stage-b.html','B 阶段实测'),('07-stage-c.html','C 阶段闭环')])
doc=f'<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>B 阶段 · Synthea 数据剖析与建模</title><style>{css}</style></head><body><header><div class="eyebrow">STAGE B / DATA PROFILING & MODEL DESIGN · 2026.09.18</div><h1>把源数据，变成明确的模型契约</h1><p>实际剖析三张表、56 个字段。先解释清楚关系与数量，再进入小样本实现。</p></header><nav aria-label="章节目录">{nav}</nav><main><div class="note good">本页保留 B 阶段剖析与设计过程；C 阶段已实现核心模型并通过核验。<a href="07-stage-c.html">查看当前结果</a> · <a href="../PROGRESS.md">最新进度记录</a>。</div>{content}<footer class="footer"><a href="01-overview.html">← 返回项目全貌</a><a href="04-roadmap.html">查看后续实施路线 →</a></footer></main></body></html>'
Path('docs/html/06-stage-b.html').write_text(doc,encoding='utf-8')
print('Wrote FIELD_DICTIONARY.md and 06-stage-b.html')

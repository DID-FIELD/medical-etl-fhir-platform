import json,re
from pathlib import Path
from html import escape
r=json.loads(Path('docs/stage-c/verification.json').read_text())
css=re.search(r'<style>(.*?)</style>',Path('docs/html/01-overview.html').read_text(encoding='utf-8'),re.S).group(1)
rows=''.join(f'<tr><td><code>{escape(k)}</code></td><td>{v:,}</td></tr>' for k,v in r['counts'].items())
content='''<section><span class="tag">C 阶段核心闭环完成</span><h2>现在已经从源文件跑到了数据库和 API</h2><p class="lead">Synthea 三表 → 质量处置 → 分层结果 → PostgreSQL 发布 → API 查询。</p><p>这次不是模型推算：下方数量已经实际落库，并与文件结果及查询接口核对。本页记录9月18日核心闭环实测；9月20日新增FHIR R4B摘要导出与10项测试，详见第八章。</p><div class="grid"><div class="block"><span class="metric">22</span>测试通过（含真实 PostgreSQL）</div><div class="block"><span class="metric">17</span>完整样本核验通过</div><div class="block"><span class="metric">12</span>零影像患者完整保留</div></div></section>
<section><h2>阶段进度</h2><div class="table-wrap"><table><thead><tr><th>阶段</th><th>目前状态</th></tr></thead><tbody><tr><td>A · 项目复习</td><td>分章节材料已交付，可继续随实现更新</td></tr><tr><td>B · 数据剖析与建模</td><td>已完成三表 56 字段剖析和模型契约</td></tr><tr><td>C · 小样本闭环</td><td>三表到数据库/API 核心链路已通过；FHIR摘要导出已补齐（第八章）；完整series仍待源检查UID</td></tr><tr><td>D · 可靠性</td><td>已覆盖基础异常、重跑和事务回滚；待扩展</td></tr><tr><td>E · 规模测试</td><td>未开始千/万患者生成与测试</td></tr><tr><td>F · 组件与复盘</td><td>数据库/API 已验证；Spark、Airflow、完整FHIR服务尚未整体验证</td></tr></tbody></table></div></section>
<section><h2>实际落地结果</h2><div class="table-wrap"><table><thead><tr><th>表</th><th>当前批次记录数</th></tr></thead><tbody>'''+rows+'''</tbody></table></div><p>三张逻辑 ODS 统一保存到来源表，共 6,157 条原始记录。患者汇总检查数加总为 413；没有影像的患者检查数为 0，最近检查时间为空。</p><p class="small">这次患者维度从影像队列的 96 位扩展为完整的 108 位；没有复制或伪造患者。检查与实例仍分别统计。</p></section>
<section><h2>数据库重新配置完成</h2><p>新建了项目专用 PostgreSQL 18.4，原有系统数据库和账号保留。新实例只监听本机，数据文件保存在项目 output 目录。</p><pre>主机：127.0.0.1
端口：55432
数据库：medical_etl
schema：synthea_v1
连接配置：output/local-postgres/connection.json</pre><p>密码由本机配置自动读取，无需记忆或粘贴。配置已排除 Git 跟踪。新实例不随 Windows 自动启动，重启电脑后运行启动命令。</p><details><summary>启动数据库、运行 ETL、启动 API</summary><pre>.\\.venv\\Scripts\\python.exe -m scripts.local_postgres start
.\\.venv\\Scripts\\python.exe run_etl.py --publish-db
.\\.venv\\Scripts\\python.exe -m uvicorn api.main:app --host 127.0.0.1 --port 8000</pre><p>在项目根目录执行。当前只保持数据库运行，未启动常驻 HTTP 服务。</p></details></section>
<section><h2>已经验证的可靠性行为</h2><details open><summary>同一批次重复加载</summary><p>相同 run_id 和相同内容直接返回已加载，不插入第二份；同名不同内容拒绝。新批次可以保存重跑历史，API 只读当前成功批次，不累计所有历史。</p></details><details open><summary>写入一半失败</summary><p>在测试专用 schema 中注入实例表写入失败，验证整批事务回滚、当前批次保持不变；随后重新加载同一文件快照成功。</p></details><details><summary>同一检查多实例、多序列、多模态</summary><p>检查数按源检查键统计。多模态检查可进入多个模态分组，分组总数不强制等于去重检查总数。</p></details><details><summary>空输入、重复、冲突和孤儿关联</summary><p>原探索版空输入错误已修复。新链路拒绝发布无患者快照；允许有患者但无影像。完全重复与键冲突分开处置，错误父级引用会级联隔离；不完整检查整项隔离。</p></details><div class="note">文件和数据库分别发布，不是分布式事务。文件成功但数据库失败时，可以重试加载已有快照。当前仅为本机开发验证，没有生产权限控制或并发负载测试。</div></section>
<section><h2>API 已查询到了什么？</h2><ul><li>全部 108 位患者的概览与检查列表逐个核对。</li><li>12 位无影像患者仍可查询，次数为 0。</li><li>来自源影像第 35、36 条记录的同一检查，可追溯到两个实例。</li><li>不存在的患者返回 404。</li></ul><pre>/api/synthea/status
/api/synthea/patients/{patient_key}
/api/synthea/patients/{patient_key}/studies
/api/synthea/studies/{study_key}/lineage</pre><p>测试使用 ASGI TestClient 连接真实 PostgreSQL，未做网络压测。旧接口仍属于原 EMR 演示，不应混用。</p></section>
<section><h2>已恢复 · FHIR摘要完成</h2><p>2026-09-20已恢复，完成FHIR R4B三类资源摘要导出，新增10项测试，总计32项通过。<a href="08-fhir.html">查看第八章</a>。D阶段本轮已完成13项扩展，总计45项测试通过，<a href="09-reliability.html">见第九章</a>；下一步进入分级规模测试。</p><p><a href="../PROGRESS.md">最新进度与恢复记录</a> · <a href="../stage-c/README.md">完整运行说明</a> · <a href="../stage-c/verification.json">17 项核验结果</a> · <a href="../../output/synthea/runs/stage-c-verified/manifest.json">当前样本运行清单</a></p></section>'''
nav='<a href="01-overview.html">项目全貌</a><a href="06-stage-b.html">B 阶段剖析</a><a href="07-stage-c.html" aria-current="page">C 阶段实测</a><a href="08-fhir.html">FHIR资源流转</a>'
Path('docs/html/07-stage-c.html').write_text(f'<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>C 阶段 · 小样本闭环与数据库</title><style>{css}</style></head><body><header><div class="eyebrow">STAGE C / VERIFIED SNAPSHOT · 2026.09.18</div><h1>数据已落库，查询可以对账</h1><p>当前进展、真实验证结果和新的本机数据库使用说明。</p></header><nav aria-label="章节目录">{nav}</nav><main>{content}<footer class="footer"><a href="06-stage-b.html">← 回顾模型设计</a><a href="01-overview.html">返回项目全貌</a></footer></main></body></html>',encoding='utf-8')
print('Stage C HTML report updated')

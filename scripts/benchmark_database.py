"""Measure a bounded loader/API client and separately sample the PostgreSQL server."""
import argparse
import json
from pathlib import Path
import re
import shutil
import sys
import threading

import psutil
from psycopg2 import sql

from scripts.benchmark_synthea import ROOT, available, measured, save
from src.database import synthea_store as store


def snapshot(schema):
    conn=store.get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute('SELECT to_regclass(%s)',(schema+'.current_snapshot',))
                if cur.fetchone()[0] is None: return None
                cur.execute(sql.SQL('SELECT run_id FROM {} WHERE singleton=1').format(sql.Identifier(schema,'current_snapshot')))
                row=cur.fetchone()
                return row[0] if row else None
    finally: conn.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir',type=Path,required=True)
    parser.add_argument('--schema',required=True)
    parser.add_argument('--name',required=True)
    args=parser.parse_args()
    if not re.fullmatch(r'synthea_scale_[a-z0-9_]{1,40}',args.schema): parser.error('Use an isolated synthea_scale_ schema')
    if not re.fullmatch('[A-Za-z0-9_-]{1,100}',args.name): parser.error('Invalid evidence name')
    if shutil.disk_usage(ROOT).free<15*1024**3 or available()<2*1024**3:
        raise RuntimeError('Require 15 GiB disk and 2 GiB available RAM')
    folder=ROOT/'docs/stage-f';folder.mkdir(exist_ok=True)
    target=ROOT/'output/database-scale'/args.name;target.mkdir(parents=True,exist_ok=False)
    evidence=folder/(args.name+'.json')
    if evidence.exists(): raise FileExistsError(args.name)
    report={'status':'RUNNING','schema':args.schema,'run_dir':str(args.run_dir),
            'official_before':snapshot('synthea_v1'),'isolated_before':snapshot(args.schema),
            'client_rss_limit_bytes':500*1024**2}
    save(evidence,report)
    server={'sampled_peak_tree_rss_bytes':0,'sample_count':0,'errors':0}
    stop=threading.Event()
    server_pid=int((ROOT/'output/local-postgres/data/postmaster.pid').read_text().splitlines()[0])
    def sample_server():
        parent=psutil.Process(server_pid)
        while not stop.is_set():
            rss=0
            try:
                for process in [parent,*parent.children(recursive=True)]:
                    try: rss+=process.memory_info().rss
                    except psutil.NoSuchProcess: pass
                server['sampled_peak_tree_rss_bytes']=max(server['sampled_peak_tree_rss_bytes'],rss)
                server['sample_count']+=1
            except psutil.Error: server['errors']+=1
            stop.wait(.25)
    thread=threading.Thread(target=sample_server,daemon=True);thread.start()
    try:
        measurement=measured([sys.executable,'-m','scripts.verify_database_scale','--run-dir',str(args.run_dir.resolve()),
                              '--schema',args.schema,'--result',str(target/'result.json')],target/'worker.log',
                              limit_seconds=1800,rss_limit_bytes=500*1024**2)
    finally:
        stop.set();thread.join(timeout=5)
    worker=json.loads((target/'result.json').read_text(encoding='utf-8')) if (target/'result.json').exists() else {}
    after=snapshot(args.schema);official_after=snapshot('synthea_v1')
    report.update(measurement=measurement,worker=worker,postgres_server=server,
                  isolated_after=after,official_after=official_after,
                  memory_scope='500 MiB guards the loader/API client tree only. Independent PostgreSQL server sampled separately; shared mappings counted per process, not exclusive RAM.',
                  status='SUCCESS' if measurement['exit_code']==0 and worker.get('status')=='SUCCESS' and after==args.run_dir.name and official_after==report['official_before'] else 'FAILED')
    if measurement['stop_reason'] or measurement['sampled_peak_tree_rss_bytes'] > report['client_rss_limit_bytes']:
        report['status']='FAILED'
    report['database_publication_observed']=after==args.run_dir.name
    save(evidence,report)
    print(json.dumps({k:report[k] for k in ['status','measurement','postgres_server','isolated_after','official_after']},indent=2))
    if report['status']!='SUCCESS': raise SystemExit(1)


if __name__=='__main__': main()

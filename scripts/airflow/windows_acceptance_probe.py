"""Windows-only database/API acceptance probe; never emits connection credentials."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
from src.database import synthea_store as store
from src.consumer_workflow import database_schema
from src.stream_io import file_hash


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--manifest',type=Path)
    parser.add_argument('--run-id')
    args=parser.parse_args()
    if args.output.exists():raise FileExistsError('Probe report already exists')
    report={'status':'RUNNING'}
    try:
        conn=store.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute('SELECT run_id FROM synthea_v1.current_snapshot')
                report['formal_database_run_id']=cur.fetchone()[0]
        finally:conn.close()
        if args.manifest:
            if json.loads(args.manifest.read_text(encoding='utf-8'))['run_id'] != args.run_id:
                raise ValueError('API input run mismatch')
            api_report=args.output.with_suffix('.api.json')
            with args.output.with_suffix('.api.log').open('x',encoding='utf-8') as log:
                subprocess.run([sys.executable,'-m','scripts.verify_database_scale','--run-dir',str(args.manifest.parent),
                    '--schema',database_schema(args.run_id),'--result',str(api_report)],check=True,
                    stdout=log,stderr=subprocess.STDOUT,timeout=600)
            api=json.loads(api_report.read_text(encoding='utf-8'))
            if api['status']!='SUCCESS' or not all(api['api_checks'].values()):
                raise ValueError('API checks failed')
            report.update(api_checks=api['api_checks'],api_requests=sum(v['requests'] for v in api['api_latency'].values()),
                          source_manifest_sha256=file_hash(args.manifest),schema=database_schema(args.run_id))
        report['status']='SUCCESS'
    except Exception as exc:
        report.update(status='FAILED',error_type=type(exc).__name__)
    args.output.write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(report['status'])
    if report['status']!='SUCCESS':raise SystemExit(1)


if __name__=='__main__':main()

"""Load an isolated snapshot, then verify API results against its file tables."""
import argparse
import json
from pathlib import Path
import re
import time

from fastapi.testclient import TestClient
from psycopg2 import sql

from api.main import app
from src.database import synthea_store as store
from src.snapshot_reader import SnapshotReader
from src.stream_io import file_hash


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir',type=Path,required=True)
    parser.add_argument('--schema',required=True)
    parser.add_argument('--result',type=Path,required=True)
    args=parser.parse_args()
    if not re.fullmatch(r'(?:synthea_scale_[a-z0-9_]{1,40}|synthea_airflow_[a-f0-9]{32})',args.schema):
        parser.error('Use a new isolated synthea_scale_ schema')
    store.SCHEMA=args.schema
    report={'status':'RUNNING','phase':'load','schema':args.schema,'run_id':json.loads((args.run_dir/'manifest.json').read_text(encoding='utf-8'))['run_id'],
            'code_sha256':{p:file_hash(p) for p in ['src/database/streaming_load.py','src/database/synthea_store.py','src/json_stream.py','api/synthea.py','scripts/verify_database_scale.py']}}
    def save():
        args.result.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    save()
    try:
        started=time.perf_counter()
        report['load']=store.load_run(args.run_dir)
        report['load_wall_seconds']=round(time.perf_counter()-started,3)
        report['status']='LOADED';report['phase']='file_reference_scan';save()
        reader=SnapshotReader(args.run_dir)
        checks={}; latencies={}; sample=[]; zero=[]
        for row in reader.rows('ads_patient_imaging_profile'):
            if row['exam_count'] and len(sample)<50: sample.append(row)
            if not row['exam_count'] and len(zero)<10: zero.append(row)
        sample+=zero
        wanted={r['patient_key'] for r in sample}
        expected_studies={k:[] for k in wanted}
        first_studies=[]
        for row in reader.rows('dwd_imaging_study'):
            if len(first_studies)<10: first_studies.append(row['study_key'])
            if row['patient_key'] in wanted:
                expected_studies[row['patient_key']].append({k:row[k] for k in ['study_key','encounter_key','started_at']})
        expected_lineage={k:[] for k in first_studies}
        for row in reader.rows('dwd_imaging_instance'):
            if row['study_key'] in expected_lineage:
                expected_lineage[row['study_key']].append({k:row[k] for k in ['instance_uid','series_uid','source_sha256','source_row']})
        manifest=json.loads((args.run_dir/'manifest.json').read_text(encoding='utf-8'))
        with TestClient(app) as client:
            def get(url, group):
                report['phase']='api_'+group
                began=time.perf_counter();response=client.get(url)
                latencies.setdefault(group,[]).append((time.perf_counter()-began)*1000)
                return response
            status=get('/api/synthea/status','status')
            checks['api_status']=status.status_code==200 and status.json()['run_id']==manifest['run_id'] and status.json()['counts']==manifest['counts']
            checks['patient_profiles']=True;checks['patient_studies']=True;checks['lineage']=True
            for expected in sample:
                patient_key=expected['patient_key']
                response=get('/api/synthea/patients/'+patient_key,'patient')
                actual=response.json()
                checks['patient_profiles'] &= response.status_code==200 and actual['run_id']==manifest['run_id'] and all(actual[k]==v for k,v in expected.items())
                response=get('/api/synthea/patients/'+patient_key+'/studies','studies')
                expected_rows=sorted(expected_studies[patient_key],key=lambda r:(r['started_at'],r['study_key']))
                checks['patient_studies'] &= response.status_code==200 and response.json()['studies']==expected_rows and response.json()['total']==expected['exam_count']
            for study,rows in expected_lineage.items():
                response=get('/api/synthea/studies/'+study+'/lineage','lineage')
                checks['lineage'] &= response.status_code==200 and response.json()['instances']==sorted(rows,key=lambda r:r['source_row'])
            checks['unknown_patient_404']=get('/api/synthea/patients/unknown','missing').status_code==404
        def summary(values):
            values=sorted(values)
            return {'requests':len(values),'min_ms':round(values[0],3),'median_ms':round(values[len(values)//2],3),
                    'p95_ms':round(values[min(len(values)-1,int(len(values)*.95))],3),'max_ms':round(values[-1],3)}
        report.update(api_checks=checks,api_latency={k:summary(v) for k,v in latencies.items()},
                      sampled_patients=len(sample),sampled_zero_exam_patients=len(zero),sampled_studies=len(first_studies),
                      api_method='Serial ASGI TestClient with real PostgreSQL; not HTTP concurrency or throughput',
                      status='SUCCESS' if all(checks.values()) else 'FAILED')
        save()
        if report['status']!='SUCCESS': raise ValueError('API verification failed')
        print(json.dumps({'status':report['status'],'load_wall_seconds':report['load_wall_seconds'],
                          'counts':report['load'].get('counts'),'api_checks':checks},indent=2))
    except Exception as exc:
        report.update(status='FAILED',error_type=type(exc).__name__,sqlstate=getattr(exc,'pgcode',None))
        message=str(exc).lower()
        report['error_category']=next((label for phrase,label in [
            ('timeout expired','connection_timeout'),('server closed the connection','server_connection_closed'),
            ('connection refused','connection_refused'),('statement timeout','statement_timeout')
        ] if phrase in message),'unclassified')
        save()
        # Do not dump SQL parameters, credentials or source rows on failure.
        print(json.dumps({'status':'FAILED','error_type':type(exc).__name__}))
        raise SystemExit(1)


if __name__=='__main__': main()

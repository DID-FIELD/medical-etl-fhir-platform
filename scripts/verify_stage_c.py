"""Validate the actual published sample against PostgreSQL and the API."""
import json
from pathlib import Path
from fastapi.testclient import TestClient
from psycopg2 import sql
from api.main import app
from src.database.synthea_store import get_connection,qualified
from src.synthea_pipeline import key


def main():
    conn=get_connection();checks={};counts={}
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL('SELECT run_id FROM {} WHERE singleton=1').format(qualified('current_snapshot')))
                run_id=cur.fetchone()[0]
                for table,expected in {'dim_patient':108,'dwd_encounter':5571,'dwd_imaging_study':413,
                    'dwd_imaging_series':413,'dwd_imaging_instance':478,'bridge_study_modality':413,
                    'dws_patient_imaging_summary':108,'dws_imaging_daily_modality':401,'ads_patient_imaging_profile':108}.items():
                    cur.execute(sql.SQL('SELECT count(*) FROM {} WHERE run_id=%s').format(qualified(table)),(run_id,))
                    counts[table]=cur.fetchone()[0];checks[table+'.count']=counts[table]==expected
                cur.execute(sql.SQL('SELECT patient_key,exam_count FROM {} WHERE run_id=%s ORDER BY patient_key').format(qualified('ads_patient_imaging_profile')),(run_id,))
                patients=cur.fetchall()
                checks['zero_exam_patients']=sum(count==0 for _,count in patients)==12
                checks['sum_exam_count']=sum(count for _,count in patients)==413
                cur.execute(sql.SQL('SELECT record FROM {} WHERE run_id=%s AND source_table=%s AND source_row=35').format(qualified('source_records')),(run_id,'imaging_studies'))
                source=cur.fetchone()[0];sample_study=key('study',source['Id'])
                cur.execute(sql.SQL('SELECT count(*) FROM {} WHERE run_id=%s').format(qualified('source_records')),(run_id,))
                checks['ods_count']=cur.fetchone()[0]==108+5571+478
        with TestClient(app) as client:
            status=client.get('/api/synthea/status');checks['api_status']=status.status_code==200 and status.json()['run_id']==run_id
            checks['api_all_patient_profiles']=True;checks['api_all_patient_studies']=True
            for patient_key,exam_count in patients:
                response=client.get('/api/synthea/patients/'+patient_key)
                checks['api_all_patient_profiles'] &= response.status_code==200 and response.json()['exam_count']==exam_count
                response=client.get('/api/synthea/patients/'+patient_key+'/studies')
                checks['api_all_patient_studies'] &= response.status_code==200 and response.json()['total']==exam_count
            response=client.get('/api/synthea/studies/'+sample_study+'/lineage')
            trace=response.json();checks['api_sample_lineage']=response.status_code==200 and [r['source_row'] for r in trace['instances']]==[35,36]
            checks['api_unknown_patient_404']=client.get('/api/synthea/patients/unknown').status_code==404
        report={'stage':'C','database':'project local PostgreSQL 18.4','run_id':run_id,'counts':counts,
                'checks':checks,'api_validation':'ASGI TestClient with real PostgreSQL; not load testing',
                'sample_lineage':trace,'passed':all(checks.values()),
                'remaining':['Synthea FHIR resource mapping/version validation','Spark equivalence','Airflow orchestration','larger Synthea data generation/load tests']}
        Path('docs/stage-c/verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
        print(json.dumps({'run_id':run_id,'checks':len(checks),'passed':all(checks.values())}))
        if not all(checks.values()):raise RuntimeError('Stage C validation failed')
    finally:conn.close()


if __name__=='__main__':main()

"""Queries read one committed Synthea snapshot; raw source records are not exposed."""
from fastapi import APIRouter, HTTPException
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from src.database.synthea_store import get_connection, qualified

router=APIRouter(prefix='/api/synthea',tags=['Synthea snapshot'])


def query(statement,parameters=(),one=False):
    conn=get_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SET LOCAL TIME ZONE 'UTC'")
                cur.execute(sql.SQL('SELECT run_id FROM {} WHERE singleton=1').format(qualified('current_snapshot')))
                row=cur.fetchone()
                if not row:raise HTTPException(503,'No published snapshot')
                run_id=row['run_id']
                cur.execute(statement,(run_id,*parameters))
                result=cur.fetchone() if one else cur.fetchall()
                return run_id,result
    finally:conn.close()


@router.get('/status')
def status():
    run_id,row=query(sql.SQL('SELECT manifest FROM {} WHERE run_id=%s').format(qualified('pipeline_runs')),one=True)
    return {'run_id':run_id,'status':'PUBLISHED','counts':row['manifest']['counts']}


@router.get('/patients/{patient_key}')
def patient(patient_key:str):
    run_id,row=query(sql.SQL('SELECT patient_key,gender,birth_year,exam_count,latest_exam_at,modality_count FROM {} WHERE run_id=%s AND patient_key=%s').format(qualified('ads_patient_imaging_profile')),(patient_key,),one=True)
    if row is None:raise HTTPException(404,'Patient not found')
    return {'run_id':run_id,**row}


@router.get('/patients/{patient_key}/studies')
def studies(patient_key:str):
    run_id,rows=query(sql.SQL('SELECT study_key,encounter_key,started_at FROM {} WHERE run_id=%s AND patient_key=%s ORDER BY started_at,study_key').format(qualified('dwd_imaging_study')),(patient_key,))
    return {'run_id':run_id,'patient_key':patient_key,'total':len(rows),'studies':rows}


@router.get('/studies/{study_key}/lineage')
def lineage(study_key:str):
    run_id,rows=query(sql.SQL('SELECT instance_uid,series_uid,source_sha256,source_row FROM {} WHERE run_id=%s AND study_key=%s ORDER BY source_row').format(qualified('dwd_imaging_instance')),(study_key,))
    if not rows:raise HTTPException(404,'Study not found')
    return {'run_id':run_id,'study_key':study_key,'source_table':'imaging_studies','record_position':'one-based, excluding header','instances':rows}

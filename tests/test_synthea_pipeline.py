import csv
import io
import json
import os
from pathlib import Path
from uuid import uuid4
import zipfile

import pytest
from fastapi.testclient import TestClient
from psycopg2 import sql

from src.synthea_pipeline import REQUIRED, key, run, transform, reconcile
from src.database import synthea_store as store


def fixture_sources():
    p=[dict(Id='p1',BIRTHDATE='1980-01-01',DEATHDATE='',GENDER='M'),
       dict(Id='p2',BIRTHDATE='1981-01-01',DEATHDATE='',GENDER='F')]
    e=[dict(Id='e1',PATIENT='p1',START='2024-01-01T00:00:00Z',STOP='2024-01-02T00:00:00Z',ENCOUNTERCLASS='ambulatory',CODE='001',DESCRIPTION='Test')]
    def image(instance,series,modality):
        return dict(Id='s1',PATIENT='p1',ENCOUNTER='e1',DATE='2024-01-01T01:00:00Z',SERIES_UID=series,INSTANCE_UID=instance,
                    MODALITY_CODE=modality,BODYSITE_CODE='123',BODYSITE_DESCRIPTION='Body',SOP_CODE='1.2.3')
    return {'patients':p,'encounters':e,'imaging_studies':[image('i1','se1','CT'),image('i2','se1','CT'),image('i3','se2','MR')]}


def archive(tmp_path,sources,name='source.zip'):
    path=tmp_path/name
    with zipfile.ZipFile(path,'w') as z:
        for table,rows in sources.items():
            body=io.StringIO();w=csv.DictWriter(body,fieldnames=REQUIRED[table]);w.writeheader();w.writerows(rows)
            z.writestr(table+'.csv',body.getvalue())
    return path


def convert(sources):
    inventory={name:{'rows':len(rows),'sha256':'a'*64} for name,rows in sources.items()}
    tables,ledger,warnings=transform(sources,inventory)
    reconcile(tables,ledger,inventory)
    return tables,ledger,warnings


def test_multiseries_and_multimodality_preserve_grain_and_zero_patient():
    tables,_,_=convert(fixture_sources())
    assert len(tables['dwd_imaging_study'])==1
    assert len(tables['dwd_imaging_series'])==2
    assert len(tables['dwd_imaging_instance'])==3
    assert len(tables['bridge_study_modality'])==2
    assert sum(r['exam_count'] for r in tables['dws_imaging_daily_modality'])==2
    assert sum(r['exam_count'] for r in tables['dws_patient_imaging_summary'])==1
    zero=next(r for r in tables['ads_patient_imaging_profile'] if r['patient_key']==key('patient','p2'))
    assert zero['exam_count']==0 and zero['latest_exam_at'] is None


def test_exact_duplicate_is_distinct_from_valid_instances():
    sources=fixture_sources();sources['imaging_studies'].append(sources['imaging_studies'][0].copy())
    tables,ledger,_=convert(sources)
    assert len(tables['dwd_imaging_instance'])==3
    assert sum(e['disposition']=='duplicate' for e in ledger)==1


def test_conflicting_instance_quarantines_whole_study():
    sources=fixture_sources();other={**sources['imaging_studies'][0],'MODALITY_CODE':'US'};sources['imaging_studies'].append(other)
    tables,ledger,_=convert(sources)
    assert tables['dwd_imaging_study']==[]
    assert all(e['disposition']=='quarantined' for e in ledger if e['source_table']=='imaging_studies')


def test_orphan_encounter_and_patient_mismatch_are_quarantined():
    sources=fixture_sources();sources['encounters'][0]['PATIENT']='unknown'
    tables,ledger,_=convert(sources)
    assert not tables['dwd_encounter'] and not tables['dwd_imaging_study']
    sources=fixture_sources();sources['imaging_studies'][0]['PATIENT']='p2'
    tables,ledger,_=convert(sources)
    assert not tables['dwd_imaging_study']
    assert any('patient_encounter_mismatch' in r['reasons'] for r in ledger)


def test_timezone_normalization_and_missing_deathdate():
    sources=fixture_sources()
    for row in sources['imaging_studies']:row['DATE']='2024-01-01T09:00:00+08:00'
    tables,_,_=convert(sources)
    assert tables['dwd_imaging_study'][0]['started_at']=='2024-01-01T01:00:00+00:00'
    assert len(tables['dim_patient'])==2


def test_snapshot_repeatability_failure_and_no_overwrite(tmp_path):
    source=archive(tmp_path,fixture_sources());root=tmp_path/'warehouse'
    one=run(source,root,'one');two=run(source,root,'two')
    assert one['counts']==two['counts']
    for name in store.COLUMNS:assert one['artifacts'][name+'.json']==two['artifacts'][name+'.json']
    with pytest.raises(FileExistsError):run(source,root,'two')
    bad=fixture_sources();bad['patients']=[]
    bad_source=archive(tmp_path,bad,'empty.zip')
    with pytest.raises(ValueError,match='Empty patient'):run(bad_source,root,'bad')
    assert json.loads((root/'current.json').read_text())['run_id']=='two'
    assert json.loads((root/'runs/bad/manifest.json').read_text())['status']=='FAILED'


def test_no_imaging_is_a_valid_snapshot(tmp_path):
    sources=fixture_sources();sources['imaging_studies']=[]
    result=run(archive(tmp_path,sources),tmp_path/'warehouse','empty-imaging')
    assert result['zero_exam_patients']==2
    assert result['counts']['dwd_imaging_study']==0


def test_missing_columns_fail_before_publication(tmp_path):
    path=tmp_path/'invalid.zip'
    with zipfile.ZipFile(path,'w') as z:z.writestr('patients.csv','Id\np1\n')
    with pytest.raises(ValueError,match='missing required columns'):run(path,tmp_path/'warehouse','bad-schema')
    assert not (tmp_path/'warehouse/current.json').exists()


@pytest.mark.skipif(os.getenv('RUN_DATABASE_TESTS')!='1',reason='Requires project PostgreSQL; opt in with RUN_DATABASE_TESTS=1')
def test_postgres_api_rollback_and_idempotent_load(tmp_path,monkeypatch):
    from api.main import app
    schema='synthea_test_'+uuid4().hex[:12]
    monkeypatch.setattr(store,'SCHEMA',schema)
    source=archive(tmp_path,fixture_sources());root=tmp_path/'warehouse'
    run(source,root,'good');run(source,root,'retry')
    try:
        assert store.load_run(root/'runs/good')['status']=='PUBLISHED'
        assert store.load_run(root/'runs/good')['status']=='ALREADY_LOADED'
        with TestClient(app) as client:
            assert client.get('/api/synthea/status').json()['run_id']=='good'
            response=client.get('/api/synthea/patients/'+key('patient','p2'))
            assert response.status_code==200 and response.json()['exam_count']==0
            assert client.get('/api/synthea/patients/missing').status_code==404
            assert client.get('/api/synthea/patients/'+key('patient','p1')+'/studies').json()['total']==1
            assert len(client.get('/api/synthea/studies/'+key('study','s1')+'/lineage').json()['instances'])==3
        original=store.execute_values
        def failing(cur,statement,*args,**kwargs):
            if 'dwd_imaging_instance' in statement.as_string(cur):raise RuntimeError('injected_write_failure')
            return original(cur,statement,*args,**kwargs)
        monkeypatch.setattr(store,'execute_values',failing)
        with pytest.raises(RuntimeError,match='injected_write_failure'):store.load_run(root/'runs/retry')
        with store.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL('SELECT run_id FROM {}.current_snapshot').format(sql.Identifier(schema)))
                assert cur.fetchone()[0]=='good'
                cur.execute(sql.SQL('SELECT count(*) FROM {}.pipeline_runs').format(sql.Identifier(schema)))
                assert cur.fetchone()[0]==1
        monkeypatch.setattr(store,'execute_values',original)
        assert store.load_run(root/'runs/retry')['status']=='PUBLISHED'
        assert TestClient(app).get('/api/synthea/status').json()['run_id']=='retry'
    finally:
        conn=store.get_connection()
        try:
            with conn:
                with conn.cursor() as cur:cur.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))
        finally:conn.close()

import json
import os
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from psycopg2 import sql

from src.json_stream import records, batches
from src.stream_io import file_hash
from src.streaming_pipeline import run
from src.database import synthea_store as store
from test_synthea_pipeline import archive, fixture_sources


@pytest.mark.parametrize('chunk', [1, 7, 65536])
def test_json_objects_across_chunks(tmp_path, chunk):
    rows = [{'text':'璺ㄥ潡\\\"\n', 'nested':[None, True, {'x':3.1}]}, {}, {'long':'x'*100}]
    path = tmp_path/'array.json'
    path.write_text('\ufeff'+json.dumps(rows, ensure_ascii=False)+' \n', encoding='utf-8')
    assert list(records(path, chunk_chars=chunk)) == rows
    path.write_text('[ \n ]', encoding='utf-8')
    assert list(records(path, chunk_chars=chunk)) == []


@pytest.mark.parametrize('body', ['[{},]', '[{} {}]', '[{"x":', '[{}', '[1]', '{}', '[] {}', '[{"x":NaN}]'])
def test_bad_json_is_rejected(tmp_path, body):
    path = tmp_path/'array.json'; path.write_text(body)
    with pytest.raises(ValueError):
        list(records(path, chunk_chars=3))


def test_json_and_batch_size_budgets(tmp_path):
    path = tmp_path/'array.json'; path.write_text('[{"x":"'+'a'*100+'"}]')
    with pytest.raises(ValueError, match='size budget'):
        list(records(path, chunk_chars=8, max_record_chars=32))
    rows = [{'x':'a'*10} for _ in range(7)]
    groups = list(batches(iter(rows), max_rows=3, max_bytes=40))
    assert [len(b) for b in groups] == [2,2,2,1]
    assert [r for b in groups for r in b] == rows


@pytest.fixture
def isolated(monkeypatch):
    if os.getenv('RUN_DATABASE_TESTS') != '1':
        pytest.skip('Requires project PostgreSQL')
    schema = 'synthea_test_stream_' + uuid4().hex[:12]
    monkeypatch.setattr(store,'SCHEMA',schema)
    yield schema
    conn=store.get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))
    finally:
        conn.close()


def test_streamed_load_batch_rollback_and_replay(tmp_path, monkeypatch, isolated):
    from api.main import app
    source=archive(tmp_path,fixture_sources()); root=tmp_path/'files'
    for name in ['old','new']:
        run(source,root,name)
    assert store.load_run(root/'runs/old',batch_rows=2)['status']=='PUBLISHED'
    profile = next(records(root/'runs/old'/'ads_patient_imaging_profile.json'))
    with TestClient(app) as client:
        for profile in records(root/'runs/old'/'ads_patient_imaging_profile.json'):
            actual = client.get('/api/synthea/patients/'+profile['patient_key']).json()
            assert actual['latest_exam_at'] == profile['latest_exam_at']

    original=store.execute_values; batch_sizes=[]; writes=0
    def fail(cur,statement,values,**kwargs):
        nonlocal writes
        batch_sizes.append(len(values))
        result=original(cur,statement,values,**kwargs)
        if 'dwd_imaging_instance' in statement.as_string(cur):
            writes+=1
            if writes==2:
                assert TestClient(app).get('/api/synthea/status').json()['run_id']=='old'
                raise RuntimeError('injected_second_batch')
        return result
    with monkeypatch.context() as patch:
        patch.setattr(store,'execute_values',fail)
        with pytest.raises(RuntimeError,match='injected_second_batch'):
            store.load_run(root/'runs/new',batch_rows=2)
    assert max(batch_sizes)<=2 and writes==2
    assert TestClient(app).get('/api/synthea/status').json()['run_id']=='old'
    conn=store.get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                for name in ['pipeline_runs','source_records','row_dispositions',*store.COLUMNS]:
                    cur.execute(sql.SQL('SELECT count(*) FROM {} WHERE run_id=%s').format(store.qualified(name)),('new',))
                    assert cur.fetchone()[0]==0
    finally:
        conn.close()
    result=store.load_run(root/'runs/new',batch_rows=2)
    assert result['status']=='PUBLISHED' and all(result['checks'].values())
    assert store.load_run(root/'runs/old',batch_rows=2)['status']=='ALREADY_LOADED'
    assert TestClient(app).get('/api/synthea/status').json()['run_id']=='new'


@pytest.mark.parametrize('problem',['count','source_row','aggregate','bad_json'])
def test_rehashed_invalid_snapshot_rolls_back(tmp_path, isolated, problem):
    source=archive(tmp_path,fixture_sources()); root=tmp_path/'files'
    for name in ['good','bad']:
        run(source,root,name)
    store.load_run(root/'runs/good',batch_rows=2)
    target=root/'runs/bad'
    manifest=json.loads((target/'manifest.json').read_text())
    if problem=='count':
        manifest['counts']['dim_patient']+=1
    else:
        name={'source_row':'dwd_encounter','aggregate':'ads_patient_imaging_profile','bad_json':'dispositions'}[problem]+'.json'
        rows=json.loads((target/name).read_text())
        if problem=='source_row': rows[0]['source_row']=999
        elif problem=='aggregate': rows[0]['exam_count']+=1
        (target/name).write_text(json.dumps(rows)+(' garbage' if problem=='bad_json' else ''))
        manifest['artifacts'][name]=file_hash(target/name)
    (target/'manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        store.load_run(target,batch_rows=2)
    conn=store.get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL('SELECT run_id FROM {}').format(store.qualified('current_snapshot')))
                assert cur.fetchone()[0]=='good'
                cur.execute(sql.SQL('SELECT count(*) FROM {} WHERE run_id=%s').format(store.qualified('source_records')),('bad',))
                assert cur.fetchone()[0]==0
    finally:
        conn.close()

"""Fault injection at input, file publication and real PostgreSQL boundaries."""
import json
import os
from uuid import uuid4
import zipfile

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from psycopg2 import sql

from src import synthea_pipeline as pipeline
from src.database import synthea_store as store
from test_synthea_pipeline import archive, fixture_sources, convert


def read(path):
    return json.loads(path.read_text(encoding='utf-8'))


@pytest.mark.parametrize('problem', ['duplicate_header', 'row_width', 'ambiguous_file'])
def test_malformed_csv_keeps_previous_snapshot(tmp_path, problem):
    source = archive(tmp_path, fixture_sources())
    root = tmp_path / 'warehouse'
    pipeline.run(source, root, 'good')
    with zipfile.ZipFile(source) as z:
        files = {name: z.read(name) for name in z.namelist()}
    body = files['patients.csv'].decode()
    lines = body.splitlines()
    if problem == 'duplicate_header':
        lines = [lines[0] + ',GENDER'] + [line + ',F' for line in lines[1:]]
        files['patients.csv'] = ('\n'.join(lines) + '\n').encode()
    elif problem == 'row_width':
        files['patients.csv'] = (body + 'bad,too,few\n').encode()
    else:
        files['nested/patients.csv'] = files['patients.csv']
    bad = tmp_path / 'bad.zip'
    with zipfile.ZipFile(bad, 'w') as z:
        for name, data in files.items(): z.writestr(name, data)
    with pytest.raises(ValueError): pipeline.run(bad, root, 'bad')
    assert read(root / 'current.json')['run_id'] == 'good'
    assert read(root / 'runs/bad/manifest.json')['status'] == 'FAILED'


@pytest.mark.parametrize('boundary', ['parquet_write', 'file_pointer'])
def test_file_failure_preserves_previous_and_new_run_recovers(tmp_path, monkeypatch, boundary):
    source = archive(tmp_path, fixture_sources())
    root = tmp_path / 'warehouse'
    good = pipeline.run(source, root, 'good')
    with monkeypatch.context() as patch:
        if boundary == 'parquet_write':
            original = pd.DataFrame.to_parquet
            def fail(frame, path, *args, **kwargs):
                if path.name == 'dwd_imaging_instance.parquet':
                    raise OSError('injected_disk_write_failure')
                return original(frame, path, *args, **kwargs)
            patch.setattr(pd.DataFrame, 'to_parquet', fail)
        else:
            def fail(*args, **kwargs): raise OSError('injected_pointer_failure')
            patch.setattr(pipeline.os, 'replace', fail)
        with pytest.raises(OSError, match='injected_'): pipeline.run(source, root, 'broken')
    assert read(root / 'current.json')['run_id'] == 'good'
    assert read(root / 'runs/broken/manifest.json')['status'] == 'FAILED'
    assert read(root / 'runs/good/manifest.json') == good
    recovered = pipeline.run(source, root, 'recovered')
    assert recovered['counts'] == good['counts']
    assert read(root / 'current.json')['run_id'] == 'recovered'


@pytest.mark.parametrize('problem', ['invalid_birth', 'naive_time', 'reversed_time'])
def test_bad_parent_cascades_and_balances_source_rows(problem):
    sources = fixture_sources()
    if problem == 'invalid_birth': sources['patients'][0]['BIRTHDATE'] = 'invalid'
    elif problem == 'naive_time': sources['encounters'][0]['START'] = '2024-01-01T00:00:00'
    else: sources['encounters'][0]['STOP'] = '2023-01-01T00:00:00Z'
    tables, ledger, _ = convert(sources)
    assert not tables['dwd_encounter'] and not tables['dwd_imaging_study']
    assert len(ledger) == sum(len(rows) for rows in sources.values())
    assert all(r['disposition'] == 'quarantined' for r in ledger if r['source_table'] == 'imaging_studies')
    assert len(tables['dim_patient']) == (1 if problem == 'invalid_birth' else 2)


@pytest.mark.parametrize('problem', ['tamper', 'missing_hash', 'failed_manifest'])
def test_loader_rejects_invalid_snapshot_before_connecting(tmp_path, problem):
    root = tmp_path / 'warehouse'
    pipeline.run(archive(tmp_path, fixture_sources()), root, 'source')
    snapshot = root / 'runs/source'
    manifest = read(snapshot / 'manifest.json')
    if problem == 'tamper': (snapshot / 'dim_patient.json').write_text('[]', encoding='utf-8')
    elif problem == 'missing_hash': del manifest['artifacts']['dim_patient.json']
    else: manifest['status'] = 'FAILED'
    (snapshot / 'manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
    def never_connect(): raise AssertionError('Must reject before opening database connection')
    with pytest.raises(ValueError): store.load_run(snapshot, connection_factory=never_connect)


@pytest.fixture
def isolated_database(monkeypatch):
    if os.getenv('RUN_DATABASE_TESTS') != '1': pytest.skip('Requires project PostgreSQL')
    schema = 'synthea_test_d_' + uuid4().hex[:12]
    monkeypatch.setattr(store, 'SCHEMA', schema)
    try: yield schema
    finally:
        conn = store.get_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))
        finally: conn.close()


def test_database_failure_reader_visibility_retry_and_old_replay(tmp_path, monkeypatch, isolated_database):
    from api.main import app
    source = archive(tmp_path, fixture_sources())
    root = tmp_path / 'warehouse'
    for name in ['old', 'new']: pipeline.run(source, root, name)
    store.load_run(root / 'runs/old')
    original = store.execute_values
    observations = []
    def observe_and_fail(cur, statement, *args, **kwargs):
        if 'ads_patient_imaging_profile' in statement.as_string(cur):
            with TestClient(app) as client:
                observations.append(client.get('/api/synthea/status').json()['run_id'])
            raise RuntimeError('injected_late_transaction_failure')
        return original(cur, statement, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(store, 'execute_values', observe_and_fail)
        with pytest.raises(RuntimeError, match='injected_late'): store.load_run(root / 'runs/new')
    assert observations == ['old']
    with TestClient(app) as client: assert client.get('/api/synthea/status').json()['run_id'] == 'old'
    conn = store.get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                for table in ['pipeline_runs', 'source_records', 'row_dispositions', *store.COLUMNS]:
                    cur.execute(sql.SQL('SELECT count(*) FROM {} WHERE run_id=%s').format(store.qualified(table)), ('new',))
                    assert cur.fetchone()[0] == 0, table
    finally: conn.close()
    assert store.load_run(root / 'runs/new')['status'] == 'PUBLISHED'
    assert store.load_run(root / 'runs/old')['status'] == 'ALREADY_LOADED'
    with TestClient(app) as client: assert client.get('/api/synthea/status').json()['run_id'] == 'new'
    # Same run id with changed content must not masquerade as an idempotent retry.
    manifest = read(root / 'runs/new/manifest.json')
    manifest['elapsed_seconds'] += 1
    (root / 'runs/new/manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
    with pytest.raises(ValueError, match='different content'): store.load_run(root / 'runs/new')
    with TestClient(app) as client: assert client.get('/api/synthea/status').json()['run_id'] == 'new'


def test_file_success_database_unavailable_then_load_existing_snapshot(tmp_path, isolated_database):
    import psycopg2
    from api.main import app
    root = tmp_path / 'warehouse'
    source = archive(tmp_path, fixture_sources())
    pipeline.run(source, root, 'old')
    store.load_run(root / 'runs/old')
    pipeline.run(source, root, 'file-ready')
    snapshot = root / 'runs/file-ready'
    before = {p.name: pipeline.sha(p.read_bytes()) for p in snapshot.iterdir()}
    def unavailable(): raise psycopg2.OperationalError('injected_database_unavailable')
    with pytest.raises(psycopg2.OperationalError, match='injected_database'):
        store.load_run(snapshot, connection_factory=unavailable)
    assert read(root / 'current.json')['run_id'] == 'file-ready'
    with TestClient(app) as client: assert client.get('/api/synthea/status').json()['run_id'] == 'old'
    assert store.load_run(snapshot)['status'] == 'PUBLISHED'
    assert {p.name: pipeline.sha(p.read_bytes()) for p in snapshot.iterdir()} == before
    with TestClient(app) as client: assert client.get('/api/synthea/status').json()['run_id'] == 'file-ready'

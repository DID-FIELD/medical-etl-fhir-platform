"""Consumer format compatibility, shuffled partition lineage and fail-closed checks."""
import json
from pathlib import Path
from collections import Counter
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from src.snapshot_reader import SnapshotReader
from src.stream_io import file_hash
from src.synthea_pipeline import run, COLUMNS, REQUIRED
from test_synthea_pipeline import fixture_sources, archive
from test_streaming_database import isolated


@pytest.fixture
def snapshots(tmp_path):
    run(archive(tmp_path, fixture_sources()), tmp_path / 'legacy', 'legacy')
    legacy = tmp_path / 'legacy/runs/legacy'
    target = tmp_path / 'spark'
    target.mkdir()
    report = json.loads((legacy / 'manifest.json').read_text())
    report.update(run_id='spark', backend='spark-full-csv-etl', artifacts={})
    for name in [*COLUMNS, *('ods_' + n for n in REQUIRED), 'dispositions']:
        rows = json.loads((legacy / (name + '.json')).read_text())
        if name in COLUMNS:
            rows = [dict(run_id='spark', **r) for r in rows]
        elif name.startswith('ods_'):
            rows = [dict(source_row=i, raw=r) for i,r in enumerate(rows,1)]
        # Reverse physical order to catch invented sequential source_row values.
        rows.reverse()
        table = pa.Table.from_pylist(rows)
        if name.startswith('ods_'):
            table = pa.Table.from_pylist(rows, schema=pa.schema([('source_row', pa.int64()), ('raw', pa.map_(pa.string(), pa.string()))]))
        directory = target / (name + '.parquet')
        directory.mkdir()
        for i, piece in enumerate([table.slice(0,1), table.slice(1)]):
            path = directory / f'part-{i:05d}.parquet'
            pq.write_table(piece, path)
            report['artifacts'][path.relative_to(target).as_posix()] = file_hash(path)
    (target / 'manifest.json').write_text(json.dumps(report))
    return legacy, target


def canonical(rows):
    return Counter(json.dumps(r,sort_keys=True) for r in rows)


def test_reader_preserves_models_and_shuffled_ods(snapshots):
    legacy, spark = map(SnapshotReader, snapshots)
    spark.verify([*COLUMNS, 'ods_patients'])
    for name in COLUMNS:
        assert canonical(spark.rows(name)) == canonical(legacy.rows(name))
    assert dict(spark.source_rows('patients')) == dict(legacy.source_rows('patients'))
    assert list(spark.source_rows('patients'))[0][0] == 2


def test_fhir_semantic_equivalence(snapshots, tmp_path):
    from src.fhir.streaming_export import export_streaming
    for n, source in enumerate(snapshots):
        result = export_streaming(source, tmp_path / str(n))
        assert result['status'] == 'SUCCESS'
    for kind in ('Patient', 'Encounter', 'ImagingStudy'):
        outputs = [canonical(json.loads(line) for line in (tmp_path / str(n) / (kind + '.ndjson')).read_text().splitlines()) for n in (0,1)]
        assert outputs[0] == outputs[1]


@pytest.mark.parametrize('damage', ['missing', 'changed', 'extra', 'manifest', 'run_id', 'count'])
def test_parquet_damage_rejected(snapshots, damage):
    _, path = snapshots
    reader = SnapshotReader(path)
    part = reader.parts('dim_patient')[0]
    report = reader.manifest
    if damage == 'missing':
        part.unlink()
    elif damage == 'changed':
        part.write_bytes(b'bad')
    elif damage == 'extra':
        (path / 'unexpected').write_text('extra')
    elif damage == 'manifest':
        (path / 'manifest.json').write_text('{}')
    elif damage == 'run_id':
        rows = pq.read_table(part).to_pylist()
        rows[0]['run_id'] = 'wrong'
        pq.write_table(pa.Table.from_pylist(rows), part)
        report['artifacts'][part.relative_to(path).as_posix()] = file_hash(part)
        (path / 'manifest.json').write_text(json.dumps(report))
        reader = SnapshotReader(path)
    else:
        report['counts']['dim_patient'] += 1
        (path / 'manifest.json').write_text(json.dumps(report))
        reader = SnapshotReader(path)
    with pytest.raises((ValueError, FileNotFoundError)):
        reader.verify(['dim_patient'])
        list(reader.rows('dim_patient'))


def test_database_parquet_equivalence_and_replay(snapshots, isolated):
    from src.database import synthea_store as store
    from psycopg2 import sql
    for path in snapshots:
        assert store.load_run(path, batch_rows=1)['status'] == 'PUBLISHED'
    conn = store.get_connection()
    try:
        with conn.cursor() as cur:
            for name, cols in {**COLUMNS, 'source_records':['source_table','source_row','source_sha256','record'],
                               'row_dispositions':['source_table','source_row','disposition','reasons']}.items():
                columns=sql.SQL(',').join(map(sql.Identifier,cols))
                query=sql.SQL('SELECT {} FROM {} WHERE run_id=%s EXCEPT ALL SELECT {} FROM {} WHERE run_id=%s').format(columns,store.qualified(name),columns,store.qualified(name))
                for ids in [('legacy','spark'),('spark','legacy')]:
                    cur.execute(query,ids)
                    assert cur.fetchall() == [], name
    finally:
        conn.close()
    assert store.load_run(snapshots[1])['status'] == 'ALREADY_LOADED'


def test_database_changed_source_rolls_back(snapshots, isolated, monkeypatch):
    from src.database import synthea_store as store
    from psycopg2 import sql
    legacy, spark = snapshots
    store.load_run(legacy)
    original = store.execute_values
    mutated = False
    def mutate(cur, query, values, **kwargs):
        nonlocal mutated
        result = original(cur,query,values,**kwargs)
        if not mutated:
            (spark / 'unexpected').write_text('concurrent change')
            mutated = True
        return result
    monkeypatch.setattr(store,'execute_values',mutate)
    with pytest.raises(ValueError,match='inventory mismatch'):
        store.load_run(spark)
    conn = store.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL('SELECT run_id FROM {}').format(store.qualified('current_snapshot')))
            assert cur.fetchone()[0] == 'legacy'
            cur.execute(sql.SQL('SELECT count(*) FROM {} WHERE run_id=%s').format(store.qualified('pipeline_runs')),('spark',))
            assert cur.fetchone()[0] == 0
    finally:
        conn.close()

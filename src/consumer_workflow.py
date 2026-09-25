"""Run-bound, retry-safe consumers for full Spark snapshots; no formal publication."""
import argparse
from hashlib import sha256
import json
from pathlib import Path
import tempfile

from src.snapshot_reader import SnapshotReader
from src.spark_full_workflow import DATASETS, attempt_directory as spark_attempt
from src.stream_io import file_hash


def attempt_directory(root, run_id, component, number):
    if component not in ('fhir', 'database'):
        raise ValueError('Unknown consumer')
    return spark_attempt(root, run_id, number).parent.parent / component / f'attempt-{number:04d}'


def database_schema(run_id):
    spark_attempt('.', run_id, 1)
    return 'synthea_airflow_' + sha256(run_id.encode()).hexdigest()[:32]


def code_hashes():
    root = Path(__file__).parent
    return {p: file_hash(root / p) for p in ['consumer_workflow.py', 'snapshot_reader.py',
        'database/streaming_load.py', 'database/synthea_store.py', 'fhir/streaming_export.py', 'fhir/synthea_export.py']}


def verify_source(manifest, run_id):
    manifest = Path(manifest)
    if manifest.name != 'manifest.json':
        raise ValueError('Expected an upstream manifest')
    reader = SnapshotReader(manifest.parent)
    if not reader.spark or reader.manifest.get('run_id') != run_id:
        raise ValueError('Upstream snapshot does not belong to this DAG run')
    reader.verify(DATASETS)
    return reader


def canonical(row):
    from datetime import datetime, date, timezone
    row = dict(row)
    for key, value in row.items():
        if value is not None and key.endswith('_at'):
            if isinstance(value, str):
                value = datetime.fromisoformat(value.replace('Z', '+00:00'))
            row[key] = value.astimezone(timezone.utc).isoformat()
        elif isinstance(value, date):
            row[key] = value.isoformat()
    return json.dumps(row, sort_keys=True, separators=(',', ':'), ensure_ascii=True)


def verify_database(reader, schema):
    """Compare all 11 database tables to source, exactly, using a bounded disk index."""
    import sqlite3
    from psycopg2 import sql
    from src.database import synthea_store as store
    from src.database.streaming_load import verify
    from src.synthea_pipeline import COLUMNS
    if schema != database_schema(reader.manifest['run_id']):
        raise ValueError('Unexpected database schema')
    store.SCHEMA = schema
    run_id = reader.manifest['run_id']
    conn = store.get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                cur.execute("SET LOCAL TIME ZONE 'UTC'")
                cur.execute(sql.SQL('SELECT manifest FROM {} WHERE run_id=%s').format(store.qualified('pipeline_runs')), (run_id,))
                stored = cur.fetchone()
                if not stored or stored[0] != reader.manifest:
                    raise ValueError('Database source manifest mismatch')
                cur.execute(sql.SQL('SELECT run_id FROM {} WHERE singleton=1').format(store.qualified('current_snapshot')))
                if cur.fetchone() != (run_id,):
                    raise ValueError('Database current run mismatch')
                counts, checks = verify(cur, store, reader.manifest)
            def source_records():
                for name, meta in reader.manifest['source'].items():
                    for number, row in reader.source_rows(name):
                        yield dict(source_table=name, source_row=number, source_sha256=meta['sha256'], record=row)
            tables = {name:(columns, lambda n=name: reader.rows(n)) for name,columns in COLUMNS.items()}
            tables['source_records'] = (['source_table','source_row','source_sha256','record'], source_records)
            tables['row_dispositions'] = (['source_table','source_row','disposition','reasons'], lambda: (
                {k:r[k] for k in ('source_table','source_row','disposition','reasons')} for r in reader.rows('dispositions')))
            compared = {}
            with tempfile.TemporaryDirectory(prefix='synthea-compare-') as temp:
                db = sqlite3.connect(str(Path(temp) / 'rows.sqlite'))
                try:
                    db.execute('PRAGMA cache_size=-8192')
                    db.execute('CREATE TABLE expected(body TEXT PRIMARY KEY, n INTEGER NOT NULL)')
                    for name,(columns, rows) in tables.items():
                        db.execute('DELETE FROM expected')
                        expected_count = 0
                        for row in rows():
                            db.execute('INSERT INTO expected VALUES (?,1) ON CONFLICT(body) DO UPDATE SET n=n+1', (canonical(row),))
                            expected_count += 1
                        actual_count = 0
                        with conn.cursor(name='consumer_compare') as cur:
                            cur.itersize = 500
                            cur.execute(sql.SQL('SELECT {} FROM {} WHERE run_id=%s').format(
                                sql.SQL(',').join(map(sql.Identifier,columns)),store.qualified(name)), (run_id,))
                            for values in cur:
                                body = canonical(dict(zip(columns,values)))
                                if db.execute('UPDATE expected SET n=n-1 WHERE body=? AND n>0',(body,)).rowcount != 1:
                                    raise ValueError('Database content mismatch: ' + name)
                                actual_count += 1
                        if expected_count != actual_count or db.execute('SELECT 1 FROM expected WHERE n<>0 LIMIT 1').fetchone():
                            raise ValueError('Database missing rows: ' + name)
                        compared[name] = dict(rows=actual_count, missing=0, extra=0)
                finally:
                    db.close()
            reader.verify(DATASETS)
            return dict(counts=counts, checks=checks, comparisons=compared)
    finally:
        conn.close()


def inventory(directory):
    directory = Path(directory)
    result = {}
    for path in directory.rglob('*'):
        if path.is_symlink():
            raise ValueError('Consumer artifact symlink')
        if path.is_file() and path != directory / 'manifest.json':
            result[path.relative_to(directory).as_posix()] = file_hash(path)
    return result


def verify_success(directory, reader, run_id, component):
    directory = Path(directory)
    report = json.loads((directory / 'manifest.json').read_text(encoding='utf-8'))
    binding = dict(run_id=run_id, component=component, source_manifest_sha256=file_hash(reader.root / 'manifest.json'), code_sha256=code_hashes())
    if report.get('status') != 'SUCCESS' or any(report.get(k) != v for k,v in binding.items()):
        raise ValueError('Consumer result binding mismatch')
    if report.get('artifacts') != inventory(directory):
        raise ValueError('Consumer artifact hash mismatch')
    if component == 'fhir':
        exported = json.loads((directory / 'fhir/manifest.json').read_text(encoding='utf-8'))
        expected = {kind:reader.manifest['counts'][name] for kind,name in [('Patient','dim_patient'),('Encounter','dwd_encounter'),('ImagingStudy','dwd_imaging_study')]}
        if (exported.get('status') != 'SUCCESS' or exported.get('source_manifest_sha256') != binding['source_manifest_sha256']
                or exported.get('counts') != expected or set(exported.get('artifacts', {})) != {k+'.ndjson' for k in expected}
                or not exported.get('checks') or not all(exported['checks'].values())):
            raise ValueError('FHIR result validation failed')
        for name,digest in exported['artifacts'].items():
            if file_hash(directory / 'fhir' / name) != digest:
                raise ValueError('FHIR artifact hash mismatch')
    else:
        if report.get('schema') != database_schema(run_id):
            raise ValueError('Consumer schema mismatch')
        current = verify_database(reader, report['schema'])
        if report.get('verification') != current:
            raise ValueError('Database verification changed')
    return str(directory / 'manifest.json')


def run_attempt(manifest, root, run_id, component, try_number):
    target = attempt_directory(root, run_id, component, try_number)
    reader = verify_source(manifest, run_id)
    for number in range(try_number,0,-1):
        candidate = attempt_directory(root,run_id,component,number)
        receipt = candidate / 'manifest.json'
        if receipt.is_file():
            report = json.loads(receipt.read_text(encoding='utf-8'))
            if report.get('status') == 'SUCCESS':
                return verify_success(candidate, reader, run_id, component)
    target.mkdir(parents=True, exist_ok=False)
    report = dict(status='RUNNING',run_id=run_id,component=component,
        source_manifest_sha256=file_hash(manifest),code_sha256=code_hashes())
    def save():
        temporary = target / 'manifest.tmp'
        temporary.write_text(json.dumps(report,indent=2),encoding='utf-8')
        temporary.replace(target / 'manifest.json')
    save()
    try:
        if component == 'fhir':
            from src.fhir.streaming_export import export_streaming
            report['export'] = export_streaming(reader.root,target / 'fhir')
        else:
            from src.database import synthea_store as store
            store.SCHEMA = report['schema'] = database_schema(run_id)
            report['load'] = store.load_run(reader.root)
            report['verification'] = verify_database(reader,report['schema'])
        reader.verify(DATASETS)
        report['artifacts'] = inventory(target)
        report['status'] = 'SUCCESS'
        save()
        return (str(target / 'manifest.json') if component == 'database'
                else verify_success(target,reader,run_id,component))
    except BaseException as exc:
        report.update(status='FAILED',error_type=type(exc).__name__)
        save()
        raise


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest',required=True)
    parser.add_argument('--output-root',required=True)
    parser.add_argument('--run-id',required=True)
    parser.add_argument('--component',required=True,choices=['fhir','database'])
    parser.add_argument('--try-number',required=True,type=int)
    args=parser.parse_args()
    try:
        result=run_attempt(args.manifest,args.output_root,args.run_id,args.component,args.try_number)
    except Exception as exc:
        # Worker failures must not print SQL parameters, source records or credentials.
        print('CONSUMER_FAILURE=' + type(exc).__name__)
        raise SystemExit(1)
    print('WORKFLOW_RESULT='+json.dumps(result))

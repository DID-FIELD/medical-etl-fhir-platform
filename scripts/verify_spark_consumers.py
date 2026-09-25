"""Accept existing Spark datasets through FHIR, isolated PostgreSQL and serial API."""
import argparse
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
from psycopg2 import sql
from src.database import synthea_store as store
from src.fhir.streaming_export import export_streaming
from src.snapshot_reader import SnapshotReader
from src.stream_io import file_hash
from src.synthea_pipeline import COLUMNS


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--spark', type=Path, required=True)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--schema', required=True)
    args = parser.parse_args()
    if not re.fullmatch(r'synthea_scale_consumers_[a-z0-9_]{1,30}', args.schema):
        raise ValueError('A new isolated consumers schema is required')
    args.output.mkdir(parents=True, exist_ok=False)
    report = dict(status='RUNNING', schema=args.schema, phase='verify_sources')
    def save():
        (args.output / 'acceptance.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    save()
    try:
        readers = [SnapshotReader(p) for p in (args.baseline, args.spark)]
        for reader in readers:
            reader.verify([*COLUMNS, 'dispositions', 'ods_patients', 'ods_encounters', 'ods_imaging_studies'])
        if readers[1].manifest['baseline']['source_manifest_sha256'] != file_hash(args.baseline / 'manifest.json'):
            raise ValueError('Spark and baseline are not bound')
        report['sources'] = [dict(run_id=r.manifest['run_id'], manifest_sha256=file_hash(r.root / 'manifest.json')) for r in readers]
        formal = Path('output/synthea/current.json')
        formal_hash = file_hash(formal)
        with store.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=%s)', (args.schema,))
                if cur.fetchone()[0]:
                    raise ValueError('Acceptance schema already exists')
                cur.execute('SELECT run_id FROM synthea_v1.current_snapshot')
                formal_db = cur.fetchone()[0]
        report['phase'] = 'fhir'
        save()
        report['fhir_exports'] = []
        for label, reader in zip(('baseline', 'spark'), readers):
            result = export_streaming(reader.root, args.output / ('fhir-' + label))
            report['fhir_exports'].append(dict(source=label, counts=result['counts'], checks=result['checks']))
        db = sqlite3.connect(args.output / 'fhir-comparison.sqlite')
        try:
            db.execute('CREATE TABLE resources(side TEXT, kind TEXT, id TEXT, body TEXT, PRIMARY KEY(side,kind,id))')
            for side in ('baseline', 'spark'):
                for kind in ('Patient', 'Encounter', 'ImagingStudy'):
                    with (args.output / ('fhir-' + side) / (kind + '.ndjson')).open(encoding='utf-8') as stream:
                        for line in stream:
                            row = json.loads(line)
                            db.execute('INSERT INTO resources VALUES (?,?,?,?)', (side,kind,row['id'],json.dumps(row,sort_keys=True,separators=(',',':'))))
            report['fhir_differences'] = {}
            for a,b in [('baseline','spark'),('spark','baseline')]:
                count = db.execute('SELECT count(*) FROM (SELECT kind,id,body FROM resources WHERE side=? EXCEPT SELECT kind,id,body FROM resources WHERE side=?)',(a,b)).fetchone()[0]
                report['fhir_differences'][a + '_minus_' + b] = count
                if count:
                    raise ValueError('FHIR semantic comparison failed')
            db.commit()
        finally:
            db.close()
        report['phase'] = 'database'
        save()
        store.SCHEMA = args.schema
        report['loads'] = [store.load_run(reader.root) for reader in readers]
        report['database_differences'] = {}
        with store.get_connection() as conn:
            with conn.cursor() as cur:
                tables = {**COLUMNS, 'source_records':['source_table','source_row','source_sha256','record'],
                          'row_dispositions':['source_table','source_row','disposition','reasons']}
                for name, columns in tables.items():
                    columns = sql.SQL(',').join(map(sql.Identifier, columns))
                    query = sql.SQL('SELECT count(*) FROM (SELECT {} FROM {} WHERE run_id=%s EXCEPT ALL SELECT {} FROM {} WHERE run_id=%s) diff').format(columns,store.qualified(name),columns,store.qualified(name))
                    diffs = []
                    for ids in [(r.manifest['run_id'] for r in readers), (r.manifest['run_id'] for r in reversed(readers))]:
                        cur.execute(query,tuple(ids))
                        diffs.append(cur.fetchone()[0])
                    report['database_differences'][name] = diffs
                    if any(diffs):
                        raise ValueError('Database content differs: ' + name)
        report['replay'] = store.load_run(args.spark)
        if report['replay']['status'] != 'ALREADY_LOADED':
            raise ValueError('Database replay is not idempotent')
        report['phase'] = 'api'
        save()
        with (args.output / 'api.log').open('w', encoding='utf-8') as log:
            subprocess.run([sys.executable, '-m', 'scripts.verify_database_scale', '--run-dir', str(args.spark),
                '--schema', args.schema, '--result', str(args.output / 'api.json')], check=True,
                stdout=log, stderr=subprocess.STDOUT, timeout=300)
        api = json.loads((args.output / 'api.json').read_text())
        if api['status'] != 'SUCCESS':
            raise ValueError('API acceptance failed')
        report['api_checks'] = api['api_checks']
        report['api_requests'] = sum(v['requests'] for v in api['api_latency'].values())
        with store.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT run_id FROM synthea_v1.current_snapshot')
                if cur.fetchone()[0] != formal_db:
                    raise ValueError('Formal database publication changed')
        if file_hash(formal) != formal_hash:
            raise ValueError('Formal file publication changed')
        for reader in readers:
            reader.verify(list(COLUMNS))
        report.update(status='SUCCESS', phase='complete', formal_file_unchanged=True,
                      formal_database_unchanged=True, formal_database_run_id=formal_db,
                      code_sha256={p:file_hash(p) for p in ['src/snapshot_reader.py', 'src/fhir/streaming_export.py',
                          'src/database/streaming_load.py', 'scripts/verify_database_scale.py', __file__]})
        save()
        print(json.dumps(dict(status='SUCCESS', schema=args.schema, api_requests=report['api_requests'])))
    except Exception as exc:
        report.update(status='FAILED', error_type=type(exc).__name__)
        save()
        print(json.dumps(dict(status='FAILED', phase=report['phase'], error_type=type(exc).__name__)))
        raise SystemExit(1)


if __name__ == '__main__':
    main()

"""Bounded client-side loading with SQL reconciliation inside one transaction."""
import json
from pathlib import Path
import time

from psycopg2 import sql
from psycopg2.extras import Json

from src.json_stream import batches
from src.snapshot_reader import SnapshotReader
from src.stream_io import file_hash
from src.synthea_pipeline import COLUMNS, REQUIRED


def verify(cur, store, manifest):
    run_id = manifest['run_id']; checks = {}; counts = {}
    def scalar(query, *names, params=None):
        cur.execute(sql.SQL(query).format(*(store.qualified(n) for n in names)), params or (run_id,))
        return cur.fetchone()[0]
    for name in COLUMNS:
        counts[name] = scalar('SELECT count(*) FROM {} WHERE run_id=%s', name)
        checks[name+'.count'] = counts[name] == manifest['counts'][name]
    for source, meta in manifest['source'].items():
        params = (run_id, source)
        checks[source+'.ods_count'] = scalar('SELECT count(*) FROM {} WHERE run_id=%s AND source_table=%s', 'source_records', params=params) == meta['rows']
        cur.execute(sql.SQL('SELECT min(source_row),max(source_row) FROM {} WHERE run_id=%s AND source_table=%s').format(store.qualified('source_records')), params)
        first, last = cur.fetchone()
        checks[source+'.ods_lineage_range'] = ((first, last) == (1, meta['rows']) if meta['rows'] else first is None and last is None)
        checks[source+'.input_balance'] = scalar('SELECT count(*) FROM {} WHERE run_id=%s AND source_table=%s', 'row_dispositions', params=params) == meta['rows']
        cur.execute(sql.SQL('SELECT disposition,count(*) FROM {} WHERE run_id=%s AND source_table=%s GROUP BY disposition').format(store.qualified('row_dispositions')), params)
        actual = dict(cur.fetchall())
        checks[source+'.quality'] = all(actual.get(k,0) == manifest['quality'][source].get(k,0) for k in ('accepted','quarantined','duplicate'))
    checks['study_parent_consistency'] = scalar('''SELECT count(*) FROM {} s JOIN {} e
        ON e.run_id=s.run_id AND e.encounter_key=s.encounter_key
        WHERE s.run_id=%s AND s.patient_key<>e.patient_key''', 'dwd_imaging_study','dwd_encounter') == 0
    for name in ('dws_patient_imaging_summary','ads_patient_imaging_profile'):
        # FK plus unique patient key and equal count prove exact coverage.
        checks[name+'.patient_coverage'] = counts[name] == counts['dim_patient']
        checks[name+'.exam_balance'] = scalar('SELECT COALESCE(sum(exam_count),0) FROM {} WHERE run_id=%s', name) == counts['dwd_imaging_study']
    checks['daily_bridge_balance'] = scalar('SELECT COALESCE(sum(exam_count),0) FROM {} WHERE run_id=%s', 'dws_imaging_daily_modality') == counts['bridge_study_modality']
    for name, source in [('dim_patient','patients'),('dwd_encounter','encounters'),('dwd_imaging_instance','imaging_studies'),('dwd_imaging_study','imaging_studies')]:
        checks[name+'.source_lineage'] = scalar('''SELECT count(*) FROM {} t LEFT JOIN {} d
            ON d.run_id=t.run_id AND d.source_table=%s AND d.source_row=t.source_row
            WHERE t.run_id=%s AND (d.source_row IS NULL OR d.disposition<>'accepted' OR t.source_sha256<>%s)''',
            name, 'row_dispositions', params=(source,run_id,manifest['source'][source]['sha256'])) == 0
        if name != 'dwd_imaging_study':
            checks[name+'.accepted_coverage'] = counts[name] == manifest['quality'][source].get('accepted',0)
            checks[name+'.unique_source_row'] = scalar('SELECT count(DISTINCT source_row) FROM {} WHERE run_id=%s',name) == counts[name]
    if not all(checks.values()):
        raise ValueError('Database reconciliation failed: '+', '.join(k for k,v in checks.items() if not v))
    return counts, checks


def load_run(run_dir, *, store, connection_factory=None, batch_rows=1000):
    if not 1 <= batch_rows <= 5000:
        raise ValueError('batch_rows must be between 1 and 5000')
    started = time.perf_counter()
    run_dir = Path(run_dir)
    reader = SnapshotReader(run_dir)
    manifest = reader.manifest
    names = [*COLUMNS, *('ods_' + n for n in REQUIRED), 'dispositions']
    reader.verify(names)
    if not reader.spark:
        required = {f'{name}.{suffix}' for name in COLUMNS for suffix in ('json', 'parquet')}
        required.update(f'ods_{name}.json' for name in REQUIRED)
        required.add('dispositions.json')
        if not required.issubset(manifest.get('artifacts', {})):
            raise ValueError('Missing required artifact checksums')
        for name, expected in manifest['artifacts'].items():
            if Path(name).name != name:
                raise ValueError('Unsafe artifact filename')
            if file_hash(run_dir / name) != expected:
                raise ValueError('Artifact checksum mismatch: ' + name)
    if set(manifest.get('source', {})) != set(REQUIRED):
        raise ValueError('Unexpected source inventory')
    if set(manifest.get('counts',{})) != set(COLUMNS) or set(manifest.get('quality',{})) != set(REQUIRED):
        raise ValueError('Missing model counts or quality contract')
    run_id = manifest['run_id']; conn = (connection_factory or store.get_connection)()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL work_mem = '16MB'")
                cur.execute("SET LOCAL statement_timeout = '15min'")
                # Lock before DDL as well, avoiding same-schema concurrent initialization races.
                cur.execute('SELECT pg_advisory_xact_lock(9182026)')
                store.initialize(cur)
                cur.execute(sql.SQL('SELECT manifest FROM {} WHERE run_id=%s').format(store.qualified('pipeline_runs')),(run_id,))
                previous = cur.fetchone()
                if previous:
                    if previous[0] != manifest:
                        raise ValueError('run_id already exists with different content')
                    return {'run_id':run_id,'status':'ALREADY_LOADED'}
                cur.execute(sql.SQL('INSERT INTO {} VALUES (%s,%s)').format(store.qualified('pipeline_runs')),(run_id,Json(manifest)))
                for name, meta in manifest['source'].items():
                    count = 0
                    for batch in batches(reader.source_rows(name), max_rows=batch_rows):
                        values = [(run_id,name,number,meta['sha256'],Json(row)) for number,row in batch]
                        store.execute_values(cur,sql.SQL('INSERT INTO {} VALUES %s').format(store.qualified('source_records')),values,page_size=batch_rows)
                        count += len(batch)
                    if count != meta['rows']:
                        raise ValueError('ODS input count differs')
                for batch in batches(reader.rows('dispositions'), max_rows=batch_rows):
                    for row in batch:
                        if row['source_table'] not in REQUIRED or row['source_sha256'] != manifest['source'][row['source_table']]['sha256'] or row['source_row'] < 1:
                            raise ValueError('Invalid disposition lineage')
                    store.execute_values(cur,sql.SQL('INSERT INTO {} VALUES %s').format(store.qualified('row_dispositions')),
                        [(run_id,r['source_table'],r['source_row'],r['disposition'],Json(r['reasons'])) for r in batch],page_size=batch_rows)
                for name, columns in COLUMNS.items():
                    statement = sql.SQL('INSERT INTO {} ({}) VALUES %s').format(store.qualified(name),sql.SQL(',').join(map(sql.Identifier,['run_id']+columns)))
                    for batch in batches(reader.rows(name), max_rows=batch_rows):
                        store.execute_values(cur,statement,[(run_id,*[row[c] for c in columns]) for row in batch],page_size=batch_rows)
                for name in [*COLUMNS, 'row_dispositions']:
                    cur.execute(sql.SQL('ANALYZE {}').format(store.qualified(name)))
                counts, checks = verify(cur,store,manifest)
                reader.verify(names)
                cur.execute(sql.SQL('INSERT INTO {} VALUES (1,%s) ON CONFLICT(singleton) DO UPDATE SET run_id=excluded.run_id').format(store.qualified('current_snapshot')),(run_id,))
        return {'run_id':run_id,'status':'PUBLISHED','counts':counts,'checks':checks,
                'batch_rows':batch_rows,'batch_bytes':2*1024**2,'elapsed_seconds':round(time.perf_counter()-started,3)}
    finally:
        conn.close()

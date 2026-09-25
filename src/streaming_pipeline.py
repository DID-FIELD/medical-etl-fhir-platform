"""Disk-backed snapshot ETL sharing the reference quality/model rules.

Related patient components stay together for cross-chunk conflict detection.
An oversized component fails explicitly instead of exceeding the batch budget.
"""
import argparse
from collections import Counter
from contextlib import closing
import csv
from datetime import datetime, timezone
import gc
import hashlib
import io
import json
import os
from pathlib import Path
import re
import platform
import sqlite3
import tempfile
import time
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq

from .synthea_pipeline import REQUIRED, COLUMNS, KEYS, transform, reconcile, date_zone, write_json
from .stream_io import file_hash, write_array

INTEGERS = {'source_row', 'birth_year', 'exam_count', 'patient_count', 'modality_count'}
LINEAGE = {'dim_patient': 'patients', 'dwd_encounter': 'encounters',
           'dwd_imaging_study': 'imaging_studies', 'dwd_imaging_instance': 'imaging_studies'}


def canonical(row):
    return json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def stage(db, archive, target):
    db.execute('CREATE TABLE raw (source TEXT,rownum INTEGER,owner TEXT,business TEXT,encounter TEXT,study TEXT,series TEXT,record TEXT,PRIMARY KEY(source,rownum))')
    inventory = {}
    with zipfile.ZipFile(archive) as z:
        for name, required in REQUIRED.items():
            matches = [n for n in z.namelist() if Path(n).name == name + '.csv']
            if len(matches) != 1:
                raise ValueError('Expected exactly one ' + name + '.csv')
            member = matches[0]
            digest = hashlib.sha256()
            with z.open(member) as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(block)
            count = 0
            with z.open(member) as raw, io.TextIOWrapper(raw, encoding='utf-8-sig', newline='') as stream:
                reader = csv.DictReader(stream)
                if not set(required).issubset(reader.fieldnames or []):
                    raise ValueError(name + ': missing required columns')
                if len(reader.fieldnames) != len(set(reader.fieldnames)):
                    raise ValueError(name + ': duplicate CSV columns')
                batch = []
                for count, row in enumerate(reader, 1):
                    if None in row or any(v is None for v in row.values()):
                        raise ValueError(name + ': inconsistent CSV row width')
                    payload = json.dumps(row, ensure_ascii=False)
                    if len(payload.encode('utf-8')) > 65536:
                        raise ValueError('Source row exceeds 64KiB budget')
                    owner = row['Id'] if name == 'patients' else row['PATIENT']
                    business = row['INSTANCE_UID'] if name == 'imaging_studies' else row['Id']
                    batch.append((name, count, owner, business, row.get('ENCOUNTER', ''),
                                  row['Id'] if name == 'imaging_studies' else '',
                                  row.get('SERIES_UID', ''), payload))
                    if len(batch) == 500:
                        db.executemany('INSERT INTO raw VALUES (?,?,?,?,?,?,?,?)', batch)
                        batch.clear()
                db.executemany('INSERT INTO raw VALUES (?,?,?,?,?,?,?,?)', batch)
            inventory[name] = {'file': member, 'rows': count, 'bytes': z.getinfo(member).file_size,
                               'sha256': digest.hexdigest()}
            db.commit()
            write_array(target / ('ods_' + name + '.json'),
                        (json.loads(r[0]) for r in db.execute('SELECT record FROM raw WHERE source=? ORDER BY rownum', (name,))))
    for col in ['owner', 'business', 'study', 'series']:
        fields = col if col == 'owner' else 'source,' + col
        db.execute('CREATE INDEX raw_' + col + ' ON raw(' + fields + ')')
    return inventory


def partitions(db, batch_rows):
    parent = {r[0]: r[0] for r in db.execute('SELECT DISTINCT owner FROM raw')}
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    def union(a, b):
        a, b = find(a), find(b)
        if a != b:
            if a > b: a, b = b, a
            parent[b] = a
    for source, column in [('encounters', 'business'), ('imaging_studies', 'business'),
                           ('imaging_studies', 'study'), ('imaging_studies', 'series')]:
        query = f'''SELECT DISTINCT r.owner,g.owner FROM raw r JOIN
            (SELECT {column} id,MIN(owner) owner FROM raw WHERE source=? GROUP BY {column}) g
            ON r.{column}=g.id WHERE r.source=? AND r.owner<>g.owner'''
        for a, b in db.execute(query, (source, source)): union(a, b)
    for a, b in db.execute('''SELECT DISTINCT i.owner,e.owner FROM raw i JOIN raw e
        ON i.encounter=e.business AND e.source='encounters'
        WHERE i.source='imaging_studies' AND i.owner<>e.owner'''):
        union(a, b)
    sizes = Counter(); byte_sizes = Counter()
    for owner, count, size in db.execute('SELECT owner,COUNT(*),SUM(length(CAST(record AS BLOB))) FROM raw GROUP BY owner'):
        sizes[find(owner)] += count
        byte_sizes[find(owner)] += size
    assignments = {}; group = 0; used = 0; used_bytes = 0
    for root in sorted(sizes):
        if sizes[root] > batch_rows or byte_sizes[root] > 32 * 1024**2:
            raise ValueError(f'Connected patient component exceeds batch budget: {sizes[root]} rows, {byte_sizes[root]} bytes; refusing unsafe split')
        if used and (used + sizes[root] > batch_rows or used_bytes + byte_sizes[root] > 32 * 1024**2):
            group += 1; used = 0; used_bytes = 0
        assignments[root] = group
        used += sizes[root]; used_bytes += byte_sizes[root]
    db.execute('CREATE TABLE owner_partition (owner TEXT PRIMARY KEY, partition INTEGER)')
    db.executemany('INSERT INTO owner_partition VALUES (?,?)', ((o, assignments[find(o)]) for o in parent))
    db.execute('CREATE INDEX partition_lookup ON owner_partition(partition)')
    db.commit()
    return len(set(assignments.values()))


def create_results(db):
    for name, columns in COLUMNS.items():
        fields = ','.join('"' + c + '" ' + ('INTEGER' if c in INTEGERS else 'TEXT') for c in columns)
        suffix = '' if name == 'dws_imaging_daily_modality' else ', PRIMARY KEY(' + ','.join(KEYS[name]) + ')'
        db.execute('CREATE TABLE ' + name + ' (' + fields + suffix + ')')
    db.execute('CREATE TABLE ledger (source TEXT,rownum INTEGER,record TEXT,PRIMARY KEY(source,rownum))')
    db.execute('CREATE TABLE warnings (ref TEXT,record TEXT)')


def process_partitions(db, inventory, count, birth_date_offset):
    quality = {name: Counter({'accepted': 0, 'quarantined': 0, 'duplicate': 0}) for name in REQUIRED}
    for part in range(count):
        sources = {name: [] for name in REQUIRED}; positions = {name: [] for name in REQUIRED}
        for source, rownum, payload in db.execute('''SELECT r.source,r.rownum,r.record FROM owner_partition p
            JOIN raw r ON r.owner=p.owner WHERE p.partition=? ORDER BY r.source,r.rownum''', (part,)):
            sources[source].append(json.loads(payload)); positions[source].append(rownum)
        local_inventory = {name: {**inventory[name], 'rows': len(rows)} for name, rows in sources.items()}
        tables, ledger, warnings = transform(sources, local_inventory, birth_date_offset)
        reconcile(tables, ledger, local_inventory)
        for name, rows in tables.items():
            source = LINEAGE.get(name)
            if source:
                for row in rows: row['source_row'] = positions[source][row['source_row'] - 1]
            columns = COLUMNS[name]
            db.executemany('INSERT INTO ' + name + ' VALUES (' + ','.join('?' for _ in columns) + ')',
                           (tuple(r[c] for c in columns) for r in rows))
        for row in ledger:
            source = row['source_table']; row['source_row'] = positions[source][row['source_row'] - 1]
            quality[source][row['disposition']] += 1
        db.executemany('INSERT INTO ledger VALUES (?,?,?)',
                       ((r['source_table'], r['source_row'], canonical(r)) for r in ledger))
        db.executemany('INSERT INTO warnings VALUES (?,?)',
                       ((r['source_instance_ref'], canonical(r)) for r in warnings))
        db.commit()
        del sources, positions, tables, ledger, warnings
        gc.collect()
    # Each patient belongs to exactly one partition, so distinct patient counts
    # are additive across partitions for a given day/modality.
    db.execute('''CREATE TABLE merged_daily AS SELECT stat_date,modality_code,SUM(exam_count) exam_count,
        SUM(patient_count) patient_count FROM dws_imaging_daily_modality GROUP BY stat_date,modality_code''')
    db.execute('DROP TABLE dws_imaging_daily_modality')
    db.execute('ALTER TABLE merged_daily RENAME TO dws_imaging_daily_modality')
    return quality


def verify_disk(db, inventory):
    scalar = lambda query: db.execute(query).fetchone()[0]
    counts = {name: scalar('SELECT COUNT(*) FROM ' + name) for name in COLUMNS}
    checks = {}
    for name in COLUMNS:
        checks[name + '.unique_key'] = scalar('SELECT COUNT(*) FROM (SELECT ' + ','.join(KEYS[name]) +
            ' FROM ' + name + ' GROUP BY ' + ','.join(KEYS[name]) + ' HAVING COUNT(*)>1)') == 0
    for name, meta in inventory.items():
        checks[name + '.input_balance'] = db.execute('SELECT COUNT(*) FROM ledger WHERE source=?', (name,)).fetchone()[0] == meta['rows']
    for label, query in {
        'encounter_patient_fk': 'SELECT COUNT(*) FROM dwd_encounter e LEFT JOIN dim_patient p USING(patient_key) WHERE p.patient_key IS NULL',
        'study_parent_consistency': 'SELECT COUNT(*) FROM dwd_imaging_study s LEFT JOIN dwd_encounter e USING(encounter_key) WHERE e.encounter_key IS NULL OR e.patient_key<>s.patient_key',
        'series_study_fk': 'SELECT COUNT(*) FROM dwd_imaging_series s LEFT JOIN dwd_imaging_study p USING(study_key) WHERE p.study_key IS NULL',
        'bridge_study_fk': 'SELECT COUNT(*) FROM bridge_study_modality b LEFT JOIN dwd_imaging_study s USING(study_key) WHERE s.study_key IS NULL',
        'instance_series_fk': 'SELECT COUNT(*) FROM dwd_imaging_instance i LEFT JOIN dwd_imaging_series s USING(study_key,series_uid) WHERE s.series_uid IS NULL',
    }.items(): checks[label] = scalar(query) == 0
    for name in ['dws_patient_imaging_summary', 'ads_patient_imaging_profile']:
        checks[name + '.patient_coverage'] = counts[name] == counts['dim_patient'] and scalar(
            'SELECT COUNT(*) FROM dim_patient p LEFT JOIN ' + name + ' s USING(patient_key) WHERE s.patient_key IS NULL') == 0
        checks[name + '.exam_balance'] = scalar('SELECT COALESCE(SUM(exam_count),0) FROM ' + name) == counts['dwd_imaging_study']
    checks['daily_bridge_balance'] = scalar('SELECT COALESCE(SUM(exam_count),0) FROM dws_imaging_daily_modality') == counts['bridge_study_modality']
    if not all(checks.values()): raise ValueError('Disk reconciliation failed')
    return counts, checks


def export_results(db, target, run_id):
    fingerprints = {}; checks = {}
    for name, columns in COLUMNS.items():
        query = 'SELECT ' + ','.join(columns) + ' FROM ' + name + ' ORDER BY ' + ','.join(KEYS[name])
        schema = pa.schema([pa.field('run_id', pa.string())] +
                           [pa.field(c, pa.int64() if c in INTEGERS else pa.string()) for c in columns])
        expected = hashlib.sha256(); count = 0
        with pq.ParquetWriter(target / (name + '.parquet'), schema) as writer:
            def output():
                nonlocal count
                cur = db.execute(query)
                while True:
                    batch = cur.fetchmany(500)
                    if not batch: break
                    rows = [dict(zip(columns, row)) for row in batch]
                    writer.write_table(pa.Table.from_pylist([{'run_id': run_id, **r} for r in rows], schema=schema))
                    for row in rows:
                        expected.update((canonical(row) + '\n').encode()); count += 1
                        yield row
            write_array(target / (name + '.json'), output())
        actual = hashlib.sha256(); read_count = 0
        for batch in pq.ParquetFile(target / (name + '.parquet')).iter_batches(batch_size=500):
            for row in batch.to_pylist():
                if row.pop('run_id') != run_id: raise ValueError('Wrong Parquet run id')
                actual.update((canonical(row) + '\n').encode()); read_count += 1
        checks[name + '.readback'] = count == read_count and expected.digest() == actual.digest()
        fingerprints[name] = expected.hexdigest()
    if not all(checks.values()): raise ValueError('Parquet readback differs')
    write_array(target / 'dispositions.json', (json.loads(r[0]) for r in db.execute('SELECT record FROM ledger ORDER BY source,rownum')))
    write_array(target / 'warnings.json', (json.loads(r[0]) for r in db.execute('SELECT record FROM warnings ORDER BY ref,record')))
    return fingerprints, checks


def run(archive, output_root='output/scale-stream', run_id=None, birth_date_offset='+00:00', batch_rows=40000):
    date_zone(birth_date_offset)
    if not 1 <= batch_rows <= 40000: raise ValueError('batch_rows must be between 1 and 40000')
    run_id = run_id or datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    if not re.fullmatch('[A-Za-z0-9_-]{1,100}', run_id): raise ValueError('Invalid run_id')
    root = Path(output_root); target = root / 'runs' / run_id
    target.mkdir(parents=True, exist_ok=False)
    started = time.perf_counter()
    manifest = {'run_id': run_id, 'status': 'RUNNING', 'model_version': 'synthea-v1',
                'backend': 'sqlite-staged-parquet', 'birth_date_offset': birth_date_offset,
                'batch_rows': batch_rows, 'component_byte_budget': 32 * 1024**2,
                'python': platform.python_version(), 'pyarrow': pa.__version__,
                'sqlite': sqlite3.sqlite_version,
                'stream_io_sha256': file_hash(Path(__file__).with_name('stream_io.py')),
                'code_sha256': file_hash(__file__),
                'rules_sha256': file_hash(Path(__file__).with_name('synthea_pipeline.py'))}
    write_json(target / 'manifest.json', manifest)
    try:
        with tempfile.TemporaryDirectory(prefix='stream-', dir=root) as temp:
            with closing(sqlite3.connect(Path(temp) / 'work.sqlite')) as db:
                db.execute('PRAGMA cache_size=-16384')
                db.execute('PRAGMA temp_store=FILE')
                db.execute('PRAGMA mmap_size=0')
                db.execute('PRAGMA synchronous=OFF')
                manifest['source'] = inventory = stage(db, archive, target)
                manifest['archive_sha256'] = file_hash(archive)
                if not inventory['patients']['rows']: raise ValueError('Empty patient snapshot is not publishable')
                manifest['partitions'] = count = partitions(db, batch_rows)
                create_results(db)
                manifest['quality'] = process_partitions(db, inventory, count, birth_date_offset)
                counts, checks = verify_disk(db, inventory)
                if not counts['dim_patient']: raise ValueError('No valid patients; refusing to publish')
                fingerprints, readback = export_results(db, target, run_id)
                checks.update(readback)
                manifest.update(counts=counts, checks=checks, semantic_sha256=fingerprints,
                    zero_exam_patients=db.execute('SELECT COUNT(*) FROM ads_patient_imaging_profile WHERE exam_count=0').fetchone()[0],
                    warning_count=db.execute('SELECT COUNT(*) FROM warnings').fetchone()[0],
                    warnings=[json.loads(r[0]) for r in db.execute('SELECT record FROM warnings ORDER BY ref LIMIT 100')],
                    warnings_artifact='warnings.json')
        manifest.update(status='SUCCESS', elapsed_seconds=round(time.perf_counter()-started, 3),
                        artifacts={p.name: file_hash(p) for p in sorted(target.iterdir()) if p.name != 'manifest.json'})
        write_json(target / 'manifest.json', manifest)
        pending = root / ('.current-' + run_id + '.json')
        write_json(pending, {'run_id': run_id})
        os.replace(pending, root / 'current.json')
        return manifest
    except Exception as exc:
        manifest.update(status='FAILED', error_type=type(exc).__name__, error=str(exc))
        write_json(target / 'manifest.json', manifest)
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', required=True)
    parser.add_argument('--output-root', default='output/scale-stream')
    parser.add_argument('--run-id')
    parser.add_argument('--birth-date-offset', default='+00:00')
    parser.add_argument('--batch-rows', type=int, default=40000)
    result = run(**vars(parser.parse_args()))
    print(json.dumps({k: result[k] for k in ['run_id', 'status', 'counts', 'quality', 'partitions', 'elapsed_seconds']}, indent=2))


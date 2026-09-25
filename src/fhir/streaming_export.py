"""Disk-indexed R4B summary export; memory bounded by one input record."""
import json
import sqlite3
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from importlib.metadata import version

from src.snapshot_reader import SnapshotReader
from src.stream_io import file_hash
from src.fhir.synthea_export import MODELS, TABLES, build_resources, digest


def export_streaming(snapshot, output):
    snapshot, output = Path(snapshot), Path(output)
    if output.exists():
        raise FileExistsError('Export directory already exists')
    reader = SnapshotReader(snapshot)
    manifest_bytes, source = reader.manifest_bytes, reader.manifest
    names = [t for t, _ in TABLES.values()] + ['dwd_imaging_series', 'dwd_imaging_instance']
    reader.verify(names)
    output.mkdir(parents=True, exist_ok=False)
    report = dict(status='RUNNING', fhir_version='4.3.0', mapping_version='synthea-summary-v1',
        source_run_id=source['run_id'], source_manifest_sha256=digest(manifest_bytes),
        exporter_sha256=file_hash(Path(__file__)),
        mapping_sha256=file_hash(Path(__file__).with_name('synthea_export.py')),
        fhir_resources_version=version('fhir.resources'),
        created_at=datetime.now(timezone.utc).isoformat())
    def save():
        (output / 'manifest.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    save()
    try:
        # Keep random SQLite I/O on worker-local storage; output may be a
        # Windows/WSL shared mount. Only final artifacts belong in output.
        with tempfile.TemporaryDirectory(prefix='synthea-fhir-') as tmp:
            db = sqlite3.connect(str(Path(tmp) / 'index.sqlite'))
            try:
                db.execute('PRAGMA cache_size=-8192')
                db.execute('PRAGMA temp_store=FILE')
                db.execute('CREATE TABLE resources(kind TEXT, id TEXT, patient TEXT, encounter TEXT, body TEXT, PRIMARY KEY(kind,id))')
                db.execute('CREATE TABLE series(study TEXT, modality TEXT)')
                db.execute('CREATE INDEX series_study ON series(study)')
                db.execute('CREATE TABLE instances(study TEXT PRIMARY KEY, n INTEGER)')
                for row in reader.rows('dwd_imaging_series'):
                    db.execute('INSERT INTO series VALUES (?,?)', (row['study_key'], row['modality_code']))
                for row in reader.rows('dwd_imaging_instance'):
                    db.execute('INSERT INTO instances VALUES (?,1) ON CONFLICT(study) DO UPDATE SET n=n+1', (row['study_key'],))
                counts = {}
                for kind, (table, key) in TABLES.items():
                    counts[kind] = 0
                    with (output / (kind + '.ndjson')).open('w', encoding='utf-8', newline='\n') as stream:
                        for row in reader.rows(table):
                            tables = {n: [] for n in names}
                            tables[table] = [row]
                            if kind == 'ImagingStudy':
                                tables['dwd_imaging_series'] = [dict(study_key=row[key], modality_code=m) for (m,) in db.execute('SELECT DISTINCT modality FROM series WHERE study=?', (row[key],))]
                            resource = build_resources(tables)[kind][0]
                            if kind == 'ImagingStudy':
                                count = db.execute('SELECT n FROM instances WHERE study=?', (row[key],)).fetchone()
                                resource['numberOfInstances'] = count[0] if count else 0
                                resource['numberOfSeries'] = db.execute('SELECT count(*) FROM series WHERE study=?', (row[key],)).fetchone()[0]
                            MODELS[kind].model_validate(resource)
                            body = json.dumps(resource, ensure_ascii=False, separators=(',', ':'))
                            db.execute('INSERT INTO resources VALUES (?,?,?,?,?)', (kind, resource['id'], row.get('patient_key'), row.get('encounter_key'), body))
                            stream.write(body + '\n')
                            counts[kind] += 1
                invalid = db.execute("SELECT count(*) FROM resources r LEFT JOIN resources p ON p.kind='Patient' AND p.id=r.patient LEFT JOIN resources e ON e.kind='Encounter' AND e.id=r.encounter WHERE (r.kind='Encounter' AND p.id IS NULL) OR (r.kind='ImagingStudy' AND (p.id IS NULL OR e.id IS NULL OR e.patient<>r.patient))").fetchone()[0]
                if invalid:
                    raise ValueError('FHIR references or patient match failed')
                artifacts = {}
                for kind in MODELS:
                    target = output / (kind + '.ndjson')
                    expected = db.execute('SELECT body FROM resources WHERE kind=? ORDER BY rowid', (kind,))
                    n = 0
                    with target.open(encoding='utf-8') as stream:
                        for line in stream:
                            row = expected.fetchone()
                            if row is None or json.loads(line) != json.loads(row[0]):
                                raise ValueError('FHIR readback differs')
                            MODELS[kind].model_validate(json.loads(line))
                            n += 1
                    if n != counts[kind] or expected.fetchone() is not None:
                        raise ValueError('FHIR readback count differs')
                    artifacts[target.name] = file_hash(target)
                # Recheck source binding after all reads.
                reader.verify(names)
                report.update(status='SUCCESS', counts=counts, artifacts=artifacts,
                    checks={'model_validation': True, 'unique_ids': True, 'references_and_patient_match': True,
                            'ndjson_readback': True, 'source_hashes': True},
                    summary_only_studies=counts['ImagingStudy'],
                    limitations=['No source StudyInstanceUID; summary only, no invented series or identifiers.',
                                 'Python R4B models and local checks; no HL7 validator or terminology service.',
                                 'NDJSON files, not a FHIR server or Bulk Data protocol.'])
            finally:
                db.close()
        save()
        return report
    except Exception as exc:
        report.update(status='FAILED', error_type=type(exc).__name__)
        save()
        raise

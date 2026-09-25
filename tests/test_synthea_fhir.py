from copy import deepcopy
import json

import pytest

from src.fhir.synthea_export import build_resources, validate_resources, export_snapshot, CLASSES
from src.synthea_pipeline import run
from test_synthea_pipeline import fixture_sources, convert, archive


def fixture_tables():
    return convert(fixture_sources())[0]


def test_summary_retains_counts_references_and_zero_imaging_patient():
    tables = fixture_tables()
    resources = build_resources(tables)
    assert all(validate_resources(resources, tables).values())
    assert len(resources['Patient']) == 2
    study = resources['ImagingStudy'][0]
    assert study['numberOfSeries'] == 2 and study['numberOfInstances'] == 3
    assert {m['code'] for m in study['modality']} == {'CT', 'MR'}
    assert 'series' not in study and 'identifier' not in study
    assert resources['Patient'][0]['birthDate'] in {'1980', '1981'}
    assert study['status'] == 'unknown'


@pytest.mark.parametrize('defect', ['orphan', 'wrong_patient', 'count', 'duplicate', 'privacy', 'invented_uid'])
def test_rejects_semantic_corruption(defect):
    tables = fixture_tables()
    resources = build_resources(tables)
    study = resources['ImagingStudy'][0]
    if defect == 'orphan':
        study['encounter']['reference'] = 'Encounter/missing'
    elif defect == 'wrong_patient':
        study['subject']['reference'] = next('Patient/' + r['id'] for r in resources['Patient']
            if 'Patient/' + r['id'] != study['subject']['reference'])
    elif defect == 'count':
        study['numberOfInstances'] = 99
    elif defect == 'duplicate':
        resources['Patient'].append(deepcopy(resources['Patient'][0]))
    elif defect == 'privacy':
        resources['Patient'][0]['name'] = [{'text': 'Unexpected name'}]
    else:
        study['identifier'] = [{'system': 'urn:dicom:uid', 'value': 'urn:oid:1.2.3'}]
    with pytest.raises(ValueError):
        validate_resources(resources, tables)


def test_class_mapping_unknown_status_and_unknown_class_fail_closed():
    tables = fixture_tables()
    for category, code in CLASSES.items():
        tables['dwd_encounter'][0]['encounter_class'] = category
        assert build_resources(tables)['Encounter'][0]['class']['code'] == code
    tables['dwd_encounter'][0]['stop_at'] = None
    assert build_resources(tables)['Encounter'][0]['status'] == 'unknown'
    tables['dwd_encounter'][0]['encounter_class'] = 'unmapped'
    with pytest.raises(ValueError, match='Unmapped encounter class'):
        build_resources(tables)


def test_no_imaging_export_is_valid():
    sources = fixture_sources()
    sources['imaging_studies'] = []
    tables = convert(sources)[0]
    resources = build_resources(tables)
    assert resources['ImagingStudy'] == []
    assert all(validate_resources(resources, tables).values())


def test_export_hashes_repeatability_and_tamper_rejection(tmp_path):
    root = tmp_path / 'warehouse'
    run(archive(tmp_path, fixture_sources()), root, 'source')
    snapshot = root / 'runs/source'
    one = export_snapshot(snapshot, tmp_path / 'one')
    two = export_snapshot(snapshot, tmp_path / 'two')
    assert one['artifacts'] == two['artifacts']
    assert one['counts'] == {'Patient': 2, 'Encounter': 1, 'ImagingStudy': 1}
    assert all(one['checks'].values())
    with pytest.raises(FileExistsError):
        export_snapshot(snapshot, tmp_path / 'one')
    target = snapshot / 'dim_patient.json'
    target.write_text('[]', encoding='utf-8')
    with pytest.raises(ValueError, match='hash mismatch'):
        export_snapshot(snapshot, tmp_path / 'tampered')
    assert not (tmp_path / 'tampered').exists()
    assert json.loads((root / 'current.json').read_text())['run_id'] == 'source'


@pytest.mark.parametrize('defect', ['duplicate', 'orphan', 'wrong_patient', 'invalid_class'])
def test_streaming_export_fails_closed(tmp_path, defect, monkeypatch):
    from src.stream_io import file_hash
    import sqlite3
    import tempfile
    scratch = tmp_path / "worker-temp"
    scratch.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(scratch))
    root = tmp_path / 'warehouse'
    run(archive(tmp_path, fixture_sources()), root, 'source')
    snapshot = root / 'runs/source'
    name = 'dim_patient' if defect == 'duplicate' else 'dwd_encounter'
    path = snapshot / (name + '.json')
    rows = json.loads(path.read_text())
    if defect == 'duplicate':
        rows.append(rows[0])
    elif defect == 'invalid_class':
        rows[0]['encounter_class'] = 'invalid'
    elif defect == 'orphan':
        rows[0]['patient_key'] = 'missing'
    else:
        patients = json.loads((snapshot / 'dim_patient.json').read_text())
        rows[0]['patient_key'] = next(p['patient_key'] for p in patients if p['patient_key'] != rows[0]['patient_key'])
    path.write_text(json.dumps(rows))
    manifest_path = snapshot / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    manifest['artifacts'][path.name] = file_hash(path)
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises((ValueError, sqlite3.IntegrityError)):
        export_snapshot(snapshot, tmp_path / 'export')
    report = json.loads((tmp_path / 'export/manifest.json').read_text())
    assert report['status'] == 'FAILED'
    assert not list((tmp_path / 'export').glob('.fhir-*'))
    assert list(scratch.iterdir()) == []


def test_streaming_export_uses_worker_temp_and_cleans_up(tmp_path, monkeypatch):
    import tempfile
    from pathlib import Path
    from src.fhir import streaming_export
    scratch = tmp_path / 'worker-temp'
    scratch.mkdir()
    monkeypatch.setattr(tempfile, 'tempdir', str(scratch))
    root = tmp_path / 'warehouse'
    run(archive(tmp_path, fixture_sources()), root, 'source')
    connect = streaming_export.sqlite3.connect
    locations = []

    def capture(path, *args, **kwargs):
        locations.append(Path(path))
        assert Path(path).is_relative_to(scratch)
        return connect(path, *args, **kwargs)

    monkeypatch.setattr(streaming_export.sqlite3, 'connect', capture)
    report = streaming_export.export_streaming(root / 'runs/source', tmp_path / 'export')
    assert report['status'] == 'SUCCESS'
    assert report['counts'] == {'Patient': 2, 'Encounter': 1, 'ImagingStudy': 1}
    assert locations and list(scratch.iterdir()) == []
    assert {p.name for p in (tmp_path / 'export').iterdir()} == {
        'manifest.json', 'Patient.ndjson', 'Encounter.ndjson', 'ImagingStudy.ndjson'}

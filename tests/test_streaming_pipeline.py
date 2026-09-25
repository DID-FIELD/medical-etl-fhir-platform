import json
from copy import deepcopy

import pytest

from test_synthea_pipeline import archive, fixture_sources
from src.synthea_pipeline import COLUMNS, read_archive, transform
from src.streaming_pipeline import run


def compare(tmp_path, sources, offset='+00:00', batch_rows=5):
    path = archive(tmp_path, sources)
    raw, inventory = read_archive(path)
    tables, ledger, warnings = transform(raw, inventory, offset)
    root = tmp_path / 'stream'
    result = run(path, root, 'test', offset, batch_rows)
    for name in COLUMNS:
        assert json.loads((root / 'runs/test' / (name + '.json')).read_text()) == tables[name]
    assert json.loads((root / 'runs/test/dispositions.json').read_text()) == ledger
    assert sorted(json.loads((root / 'runs/test/warnings.json').read_text()), key=str) == sorted(warnings, key=str)
    assert all(result['checks'].values())
    assert not list(root.glob('stream-*'))
    return result, tables, ledger


def multiple():
    sources = fixture_sources()
    sources['encounters'].append({**sources['encounters'][0], 'Id': 'e2', 'PATIENT': 'p2'})
    sources['imaging_studies'].append({**sources['imaging_studies'][0], 'Id': 's2', 'PATIENT': 'p2',
                                     'ENCOUNTER': 'e2', 'INSTANCE_UID': 'i4', 'SERIES_UID': 'se3'})
    return sources


def test_partition_equivalence_lineage_and_daily_merge(tmp_path):
    result, tables, _ = compare(tmp_path, multiple())
    assert result['partitions'] == 2
    assert next(r for r in tables['dws_imaging_daily_modality'] if r['modality_code'] == 'CT')['patient_count'] == 2


@pytest.mark.parametrize('conflict', ['SERIES_UID', 'INSTANCE_UID', 'encounter', 'late_duplicate'])
def test_cross_patient_and_late_conflicts(tmp_path, conflict):
    sources = multiple()
    if conflict in ('SERIES_UID', 'INSTANCE_UID'):
        sources['imaging_studies'][-1][conflict] = sources['imaging_studies'][0][conflict]
    elif conflict == 'encounter':
        sources['imaging_studies'][-1]['ENCOUNTER'] = 'e1'
    else:
        sources['patients'].extend([deepcopy(sources['patients'][0]), {**sources['patients'][0], 'GENDER': 'F'}])
    _, tables, ledger = compare(tmp_path, sources, batch_rows=20)
    assert any(r['disposition'] == 'quarantined' for r in ledger)
    if conflict in ('SERIES_UID', 'INSTANCE_UID'):
        assert not tables['dwd_imaging_study']
    if conflict == 'late_duplicate':
        assert all(r['disposition'] == 'quarantined' for r in ledger if r['source_table'] == 'patients' and r['source_row'] != 2)


@pytest.mark.parametrize('offset,expected', [('+00:00', 0), ('+08:00', 1)])
def test_birth_date_timezone_boundary(tmp_path, offset, expected):
    sources = fixture_sources()
    sources['patients'][0]['BIRTHDATE'] = '2024-01-01'
    sources['encounters'][0]['START'] = '2023-12-31T17:00:00Z'
    _, tables, _ = compare(tmp_path, sources, offset)
    assert len(tables['dwd_encounter']) == expected
    if expected:
        assert tables['dwd_encounter'][0]['start_at'] == '2023-12-31T17:00:00+00:00'


def test_empty_imaging(tmp_path):
    sources = fixture_sources()
    sources['imaging_studies'] = []
    result, _, _ = compare(tmp_path, sources)
    assert result['zero_exam_patients'] == 2


def test_oversize_and_empty_fail_without_publishing_and_cleanup(tmp_path):
    path = archive(tmp_path, fixture_sources())
    root = tmp_path / 'stream'
    run(path, root, 'good')
    with pytest.raises(ValueError, match='batch budget'):
        run(path, root, 'oversize', batch_rows=2)
    sources = fixture_sources(); sources['patients'] = []
    with pytest.raises(ValueError, match='Empty patient'):
        run(archive(tmp_path, sources, 'empty.zip'), root, 'empty')
    assert json.loads((root / 'current.json').read_text())['run_id'] == 'good'
    for run_id in ['oversize', 'empty']:
        assert json.loads((root / 'runs' / run_id / 'manifest.json').read_text())['status'] == 'FAILED'
    assert not list(root.glob('stream-*'))

"""Differential tests for the standard-library Spark worker rules."""
from copy import deepcopy

import pytest

from src.spark import synthea_rules as rules
from src.synthea_pipeline import transform, key, timestamp, date_zone
from spark_fixtures import fixture_sources


def reference(sources, offset='+00:00'):
    inventory = {name: {'rows': len(rows), 'sha256': 'a' * 64}
                 for name, rows in sources.items()}
    return transform(sources, inventory, offset)


@pytest.mark.parametrize('changes', [
    {}, {'BIRTHDATE': ''}, {'BIRTHDATE': '1980-13-01'},
    {'BIRTHDATE': '19800101'}, {'DEATHDATE': '1979-12-31'},
    {'DEATHDATE': 'bad'}, {'GENDER': 'unknown'},
    {'BIRTHDATE': 'bad', 'DEATHDATE': 'bad', 'GENDER': ''},
])
def test_patient_reasons_match_reference(changes):
    sources = fixture_sources()
    sources['patients'][0].update(changes)
    _, ledger, _ = reference(sources)
    entry = next(e for e in ledger if e['source_table'] == 'patients' and e['source_row'] == 1)
    assert rules.patient_errors(sources['patients'][0]) == entry['reasons']


@pytest.mark.parametrize('offset', ['+00:00', '+08:00'])
@pytest.mark.parametrize('changes', [
    {}, {'PATIENT': 'missing'}, {'START': '2024-01-01T00:00:00'},
    {'START': 'bad'}, {'STOP': 'bad'}, {'STOP': ''},
    {'START': '1979-12-31T20:00:00Z'},
    {'STOP': '2023-12-31T00:00:00Z'},
    {'PATIENT': 'missing', 'START': 'bad'},
])
def test_encounter_reasons_match_reference(changes, offset):
    sources = fixture_sources()
    row = sources['encounters'][0]
    row.update(changes)
    _, ledger, _ = reference(sources, offset)
    expected = next(e for e in ledger if e['source_table'] == 'encounters')
    birth = next((p['BIRTHDATE'] for p in sources['patients'] if p['Id'] == row['PATIENT']), None)
    assert rules.encounter_errors(row, birth, offset) == expected['reasons']


@pytest.mark.parametrize('offset', ['+00:00', '+08:00'])
@pytest.mark.parametrize('changes', [
    {}, {'PATIENT': 'p2'}, {'PATIENT': 'missing'}, {'ENCOUNTER': 'missing'},
    {'DATE': 'bad'}, {'DATE': '2024-01-01T01:00:00'},
    {'DATE': '1979-12-31T20:00:00Z'}, {'DATE': '2024-01-03T00:00:00Z'},
    {'SOP_CODE': '', 'DATE': '2024-01-03T00:00:00Z'},
    {'Id': ' ', 'SERIES_UID': '', 'MODALITY_CODE': '', 'SOP_CODE': ''},
])
def test_imaging_reasons_and_warnings_match_reference(changes, offset):
    sources = fixture_sources()
    row = deepcopy(sources['imaging_studies'][0])
    row.update(changes)
    sources['imaging_studies'] = [row]
    _, ledger, warnings = reference(sources, offset)
    expected = next(e for e in ledger if e['source_table'] == 'imaging_studies')
    birth = next((p['BIRTHDATE'] for p in sources['patients'] if p['Id'] == row['PATIENT']), None)
    encounter = next((e for e in sources['encounters'] if e['Id'] == row['ENCOUNTER']), {})
    reasons, warning = rules.imaging_validation(row, birth, encounter.get('PATIENT'),
                                               encounter.get('START'), encounter.get('STOP'), offset)
    assert sorted(reasons) == sorted(r for r in expected['reasons'] if r != 'incomplete_or_conflicting_study')
    assert warnings == ([{'rule': 'imaging_outside_encounter',
                          'source_instance_ref': rules.stable_key('instance', row['INSTANCE_UID'])}] if warning else [])


@pytest.mark.parametrize('offset', ['+00:00', '+08:00', '-14:00', '+14:00', '+14:01', 'UTC', '+8:00'])
def test_offset_contract(offset):
    try:
        expected = date_zone(offset)
    except ValueError:
        with pytest.raises(ValueError):
            rules.date_zone(offset)
    else:
        assert rules.date_zone(offset) == expected


@pytest.mark.parametrize('value', ['2024-01-01T09:00:00+08:00', '2024-01-01T00:00:00Z', '2024-01-01', 'bad'])
def test_timestamp_contract(value):
    try:
        expected = timestamp(value)
    except ValueError:
        with pytest.raises(ValueError):
            rules.utc(value)
    else:
        assert rules.utc(value) == expected
    assert rules.stable_key('patient', value) == key('patient', value)

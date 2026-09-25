"""FHIR R4B summary export from an immutable Synthea warehouse snapshot.

No StudyInstanceUID is present in the CSV contract: series details remain in DWD.
This is a file export, not a FHIR server or a Bulk Data implementation.
"""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from hashlib import sha256
from importlib.metadata import version
import json
from pathlib import Path

from fhir.resources.R4B.patient import Patient
from fhir.resources.R4B.encounter import Encounter
from fhir.resources.R4B.imagingstudy import ImagingStudy

MODELS = {'Patient': Patient, 'Encounter': Encounter, 'ImagingStudy': ImagingStudy}
TABLES = {'Patient': ('dim_patient', 'patient_key'),
          'Encounter': ('dwd_encounter', 'encounter_key'),
          'ImagingStudy': ('dwd_imaging_study', 'study_key')}
# Matches Synthea HealthRecord.EncounterType; retain original class in Encounter.type.
CLASSES = {'ambulatory': 'AMB', 'outpatient': 'AMB', 'wellness': 'AMB',
           'urgentcare': 'AMB', 'inpatient': 'IMP', 'emergency': 'EMER',
           'home': 'HH', 'hospice': 'HH', 'snf': 'IMP', 'virtual': 'VR'}
ACT = 'http://terminology.hl7.org/CodeSystem/v3-ActCode'
LOCAL = 'https://medical-etl.example/CodeSystem/synthea-encounter-class'
DICOM = 'http://dicom.nema.org/resources/ontology/DCM'


def build_resources(tables):
    result = {name: [] for name in MODELS}
    for row in tables['dim_patient']:
        result['Patient'].append(dict(resourceType='Patient', id=row['patient_key'],
            gender={'M': 'male', 'F': 'female'}.get(row['gender'], 'unknown'),
            birthDate=str(row['birth_year'])))
    for row in tables['dwd_encounter']:
        category = row['encounter_class'].lower()
        if category not in CLASSES:
            raise ValueError('Unmapped encounter class: ' + category)
        period = {'start': row['start_at']}
        if row['stop_at']:
            period['end'] = row['stop_at']
        result['Encounter'].append({
            'resourceType': 'Encounter', 'id': row['encounter_key'],
            'status': 'finished' if row['stop_at'] else 'unknown',
            'class': {'system': ACT, 'code': CLASSES[category]},
            'type': [{'coding': [{'system': LOCAL, 'code': category}]}],
            'subject': {'reference': 'Patient/' + row['patient_key']}, 'period': period})
    series = Counter(r['study_key'] for r in tables['dwd_imaging_series'])
    instances = Counter(r['study_key'] for r in tables['dwd_imaging_instance'])
    modalities = defaultdict(set)
    for row in tables['dwd_imaging_series']:
        modalities[row['study_key']].add(row['modality_code'])
    for row in tables['dwd_imaging_study']:
        sid = row['study_key']
        result['ImagingStudy'].append(dict(resourceType='ImagingStudy', id=sid,
            status='unknown', subject={'reference': 'Patient/' + row['patient_key']},
            encounter={'reference': 'Encounter/' + row['encounter_key']}, started=row['started_at'],
            numberOfSeries=series[sid], numberOfInstances=instances[sid],
            modality=[{'system': DICOM, 'code': m} for m in sorted(modalities[sid])]))
    # Normalize dates and primitive representations through the explicit R4B models.
    return {name: [MODELS[name].model_validate(r).model_dump(mode='json', exclude_none=True)
                   for r in rows] for name, rows in result.items()}


def validate_resources(resources, tables):
    """Model validation plus local reference/count/semantic checks; no terminology server."""
    if set(resources) != set(MODELS):
        raise ValueError('Unexpected resource types')
    checks = {}
    indexes = {}
    for kind, model in MODELS.items():
        for resource in resources[kind]:
            model.model_validate(resource)
        indexes[kind] = {r['id']: r for r in resources[kind]}
        table, column = TABLES[kind]
        checks[kind + '.ids_and_counts'] = (
            len(indexes[kind]) == len(resources[kind]) == len(tables[table])
            and set(indexes[kind]) == {r[column] for r in tables[table]})
    patient_refs = {'Patient/' + k for k in indexes['Patient']}
    encounters = {'Encounter/' + k: r for k, r in indexes['Encounter'].items()}
    checks['encounter_patient_references'] = all(
        r['subject']['reference'] in patient_refs for r in resources['Encounter'])
    checks['study_references_and_patient_match'] = all(
        r['subject']['reference'] in patient_refs
        and r['encounter']['reference'] in encounters
        and encounters[r['encounter']['reference']]['subject'] == r['subject']
        for r in resources['ImagingStudy'])
    expected = build_resources(tables)
    checks['warehouse_mapping_matches'] = all(
        sorted(resources[k], key=lambda r: r['id']) == sorted(expected[k], key=lambda r: r['id'])
        for k in MODELS)
    checks['no_invented_dicom_uid_or_endpoint'] = all(
        not any(field in r for field in ('series', 'identifier', 'endpoint'))
        for r in resources['ImagingStudy'])
    checks['patient_minimum_fields'] = all(
        set(r) == {'resourceType', 'id', 'gender', 'birthDate'} for r in resources['Patient'])
    if not all(checks.values()):
        raise ValueError('FHIR checks failed: ' + ', '.join(k for k, ok in checks.items() if not ok))
    return checks


def digest(data):
    return sha256(data).hexdigest()


def export_snapshot(snapshot, output):
    from src.fhir.streaming_export import export_streaming
    return export_streaming(snapshot, output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    print(json.dumps(export_snapshot(args.snapshot, args.output), indent=2))

"""Auditable single-machine EMR snapshot pipeline; no database or Spark required."""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.data_access.emr_loader import load_emr_data
from src.fhir.converter import convert_to_fhir_observation, convert_to_fhir_patient
from src.processing.anonymizer import anonymize_patient_data
from src.processing.cleaner import standardize_fields
from src.quality.rules import run_emr_quality_checks

REQUIRED = ['patient_id', 'patient_name', 'gender', 'birth_date', 'exam_type', 'study_date']


def digest(value):
    return hashlib.sha256(value).hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')


def prepare_records(raw):
    """Return cleaned rows plus mutually exclusive disposition and all error reasons."""
    missing = set(REQUIRED) - set(raw.columns)
    if missing:
        raise ValueError(f'Missing columns: {sorted(missing)}')
    clean = standardize_fields(raw, 'emr')
    clean['exam_type'] = clean.exam_type.astype('string').str.strip()
    flags = pd.DataFrame(index=clean.index)
    flags['missing_patient_id'] = clean.patient_id.isna() | clean.patient_id.eq('UNKNOWN')
    flags['invalid_gender'] = ~clean.gender.isin(['M', 'F', 'UNKNOWN'])
    flags['invalid_study_date'] = clean.study_date.isna()
    flags['missing_exam_type'] = clean.exam_type.isna() | clean.exam_type.isin(['', 'UNKNOWN'])
    supplied_birth = raw.birth_date.fillna('').str.strip().ne('')
    flags['invalid_birth_date'] = supplied_birth & clean.birth_date.isna()
    flags['study_before_birth'] = (clean.study_date < clean.birth_date).fillna(False)
    # Conflicting patient attributes are quarantined rather than choosing an arbitrary row.
    eligible = ~flags.fillna(False).any(axis=1)
    demo = clean.loc[eligible, ['patient_id', 'gender', 'birth_date']].fillna('')
    conflicts = demo.groupby('patient_id')[['gender', 'birth_date']].transform('nunique').gt(1).any(axis=1)
    flags['conflicting_demographics'] = conflicts.reindex(clean.index, fill_value=False)
    if 'source_record_id' in raw:
        key = raw.source_record_id.fillna('').str.strip()
        flags['missing_source_record_id'] = key.eq('')
        # An event identifier reused for different clinical data is not a duplicate.
        variant = clean[REQUIRED].fillna('').astype(str).agg(tuple, axis=1)
        conflicts = pd.DataFrame({'key': key, 'variant': variant}).groupby('key').variant.transform('nunique').gt(1)
        flags['conflicting_source_record_id'] = conflicts
        clean['business_key'] = key
    else:
        clean['business_key'] = clean[['patient_id', 'study_date', 'exam_type']].fillna('').apply(
            lambda row: json.dumps(row.tolist(), ensure_ascii=False), axis=1)
    invalid = flags.fillna(False).any(axis=1)
    duplicate = pd.Series(False, index=clean.index)
    duplicate.loc[~invalid] = clean.loc[~invalid].duplicated('business_key', keep='first')
    clean['disposition'] = 'accepted'
    clean.loc[invalid, 'disposition'] = 'quarantined'
    clean.loc[duplicate, 'disposition'] = 'duplicate'
    clean['reason'] = pd.Series([';'.join(flags.columns[row]) for row in flags.fillna(False).to_numpy(dtype=bool)], index=clean.index, dtype='string')
    clean.loc[duplicate, 'reason'] = 'duplicate_business_key'
    return clean


def run_audited_etl(emr_csv, output_root='output/runs', run_id=None):
    start = time.perf_counter()
    run_id = run_id or datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,100}', run_id):
        raise ValueError('run_id must be a safe directory name')
    target = Path(output_root) / run_id
    target.mkdir(parents=True, exist_ok=False)  # Existing successful runs are never overwritten.
    manifest = {'run_id': run_id, 'status': 'RUNNING', 'backend': 'pandas-parquet',
                'started_at': datetime.now(timezone.utc).isoformat(),
                'environment': {'python': platform.python_version(), 'pandas': pd.__version__,
                                'platform': platform.platform()}, 'stage_seconds': {}}
    write_json(target / 'manifest.json', manifest)
    try:
        source = Path(emr_csv)
        source_bytes = source.read_bytes()
        source_hash = digest(source_bytes)
        raw = load_emr_data(source)
        # Reserve names so external columns cannot override lineage.
        raw = raw[REQUIRED + (['source_record_id'] if 'source_record_id' in raw else [])].copy()
        raw['source_row'] = range(1, len(raw) + 1)
        raw['source_sha256'] = source_hash
        raw['run_id'] = run_id
        manifest['input'] = {'path': str(source.resolve()), 'bytes': len(source_bytes),
                             'sha256': source_hash, 'files': 1, 'rows': len(raw),
                             'distinct_patient_ids': int(raw.patient_id.nunique())}
        raw.to_parquet(target / 'ods_emr_raw.parquet', index=False)
        manifest['stage_seconds']['extract_ods'] = round(time.perf_counter() - start, 4)
        t = time.perf_counter()
        clean = prepare_records(raw)
        pd.DataFrame([r.as_dict() for r in run_emr_quality_checks(clean)]).to_csv(target / 'quality_rules.csv', index=False)
        clean[['run_id', 'source_sha256', 'source_row', 'disposition', 'reason']].to_parquet(target / 'dispositions.parquet', index=False)
        clean.loc[clean.disposition != 'accepted'].to_parquet(target / 'quarantine_and_duplicates.parquet', index=False)
        accepted = clean.loc[clean.disposition == 'accepted'].copy()
        anon = anonymize_patient_data(accepted).drop(columns=['original_patient_id'], errors='ignore')
        anon['exam_record_id'] = accepted.business_key.map(lambda v: 'OBS-' + digest(v.encode())[:40])
        dim = anon[['patient_id', 'patient_name', 'gender', 'birth_year']].drop_duplicates('patient_id').copy()
        dwd = anon[['exam_record_id', 'patient_id', 'study_date', 'exam_type', 'run_id', 'source_sha256', 'source_row']].copy()
        dwd['body_part'] = 'UNKNOWN'
        dws = dwd.groupby('patient_id', as_index=False).agg(exam_count=('exam_record_id', 'size'),
                   latest_exam_date=('study_date', 'max'), exam_type_count=('exam_type', 'nunique'))
        ads = dim.merge(dws, on='patient_id', how='left', validate='one_to_one')
        frames = {'dim_patient': dim, 'dwd_exam_record_detail': dwd,
                  'dws_patient_exam_summary': dws, 'ads_patient_360_view': ads}
        for name, frame in frames.items():
            frame.to_parquet(target / f'{name}.parquet', index=False)
        manifest['stage_seconds']['quality_transform_write'] = round(time.perf_counter() - t, 4)
        t = time.perf_counter()
        patients = [convert_to_fhir_patient(row) for row in dim.to_dict('records')]
        observations = [convert_to_fhir_observation({**row, 'obs_id': row['exam_record_id']}) for row in dwd.to_dict('records')]
        write_json(target / 'patient.json', patients)
        write_json(target / 'observation.json', observations)
        manifest['stage_seconds']['fhir_validate_write'] = round(time.perf_counter() - t, 4)
        counts = {'input': len(raw), 'quarantined': int(clean.disposition.eq('quarantined').sum()),
                  'duplicates': int(clean.disposition.eq('duplicate').sum()), 'accepted': len(accepted),
                  **{k: len(v) for k, v in frames.items()}, 'fhir_patient': len(patients),
                  'fhir_observation': len(observations)}
        patient_ids = {p['id'] for p in patients}
        checks = {
            'input_balance': counts['input'] == counts['quarantined'] + counts['duplicates'] + counts['accepted'],
            'patient_count': len(dim) == accepted.patient_id.nunique() == len(patients),
            'exam_count': len(dwd) == len(accepted) == len(observations),
            'aggregate_balance': int(dws.exam_count.sum()) == len(dwd),
            'ads_balance': len(ads) == len(dim) and int(ads.exam_count.sum()) == len(dwd),
            'unique_observation_ids': len({o['id'] for o in observations}) == len(observations),
            'unique_patient_ids': len(patient_ids) == len(patients),
            'fhir_references': all(o['subject']['reference'].removeprefix('Patient/') in patient_ids for o in observations),
            'lineage_complete': dwd.source_row.is_unique and len(dwd) == len(accepted),
            'no_direct_identifiers_in_dwd': not {'patient_name','birth_date','original_patient_id'} & set(dwd.columns),
        }
        # Verify what is on disk, not just in-memory counts.
        for name, frame in frames.items():
            saved = pd.read_parquet(target / f'{name}.parquet')
            checks[f'readback_{name}'] = saved.equals(frame.reset_index(drop=True))
        manifest.update(counts=counts, checks={k: bool(v) for k,v in checks.items()},
                        reason_counts={k: int(v) for k,v in clean.loc[clean.disposition != 'accepted', 'reason'].value_counts().items()},
                        fhir_version='R4B 4.3.0',
                        grain='source_record_id when supplied; otherwise patient_id + study_date + exam_type',
                        elapsed_seconds=round(time.perf_counter() - start, 4))
        if not all(checks.values()):
            raise ValueError(f'Reconciliation failed: {[k for k,v in checks.items() if not v]}')
        manifest['artifacts'] = {p.name: {'bytes': p.stat().st_size, 'sha256': digest(p.read_bytes())}
                                 for p in sorted(target.iterdir()) if p.name != 'manifest.json'}
        manifest['status'] = 'SUCCESS'
        write_json(target / 'manifest.json', manifest)
        return manifest
    except Exception as exc:
        manifest.update(status='FAILED', error_type=type(exc).__name__, error=str(exc),
                        elapsed_seconds=round(time.perf_counter() - start, 4))
        write_json(target / 'manifest.json', manifest)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--emr-csv', required=True)
    parser.add_argument('--output-root', default='output/runs')
    parser.add_argument('--run-id')
    args = parser.parse_args()
    result = run_audited_etl(**vars(args))
    print(json.dumps({k: result[k] for k in ['run_id', 'status', 'counts', 'elapsed_seconds']}, indent=2))


if __name__ == '__main__':
    main()

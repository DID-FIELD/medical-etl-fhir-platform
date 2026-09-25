"""Prepare public Synthea imaging-study input and deterministic scale fixtures."""
import argparse
import hashlib
import io
import json
import zipfile
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

URL = 'https://synthetichealth.github.io/synthea-sample-data/downloads/latest/synthea_sample_data_csv_latest.zip'


def write_manifest(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')


def prepare_synthea(archive, destination):
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    inventory = []
    with zipfile.ZipFile(archive) as z:
        for item in z.infolist():
            if item.filename.endswith('.csv'):
                body = z.read(item)
                table = pd.read_csv(io.BytesIO(body), dtype=str)
                inventory.append({'file': item.filename, 'bytes': len(body), 'rows': len(table),
                                  'sha256': hashlib.sha256(body).hexdigest()})
        def read(name):
            return pd.read_csv(io.BytesIO(z.read(next(n for n in z.namelist() if n.endswith(name)))), dtype=str)
        patients = read('patients.csv')
        imaging = read('imaging_studies.csv')
    if patients.Id.duplicated().any():
        raise ValueError('Non-unique source patient identifier')
    stable = imaging.groupby('Id')[['PATIENT','DATE']].nunique()
    if stable.gt(1).any().any():
        raise ValueError('Study identifier has conflicting patient/date')
    # Source rows are imaging instances; the warehouse input grain is one study.
    studies = imaging.groupby('Id', sort=True).agg(
        patient_id=('PATIENT','first'), timestamp=('DATE','first'),
        modalities=('MODALITY_CODE', lambda x: '+'.join(sorted(set(x.dropna())))),
        body_sites=('BODYSITE_DESCRIPTION', lambda x: '+'.join(sorted(set(x.dropna())))),
        source_instances=('INSTANCE_UID','size')).reset_index()
    joined = studies.merge(patients[['Id','FIRST','LAST','GENDER','BIRTHDATE']],
                           left_on='patient_id', right_on='Id', how='left', validate='many_to_one', suffixes=('', '_patient'), indicator=True)
    if not joined['_merge'].eq('both').all():
        raise ValueError('Imaging study references an unknown patient')
    frame = pd.DataFrame({'patient_id': joined.patient_id,
                          'patient_name': joined.FIRST.fillna('') + ' ' + joined.LAST.fillna(''),
                          'gender': joined.GENDER, 'birth_date': joined.BIRTHDATE,
                          'exam_type': joined.modalities + ':' + joined.body_sites,
                          'study_date': joined.timestamp.str[:10], 'source_record_id': joined.Id})
    frame.to_csv(destination/'emr.csv', index=False)
    imaging.reset_index(names='source_zero_based_row').to_parquet(destination/'imaging_source_lineage.parquet', index=False)
    archive = Path(archive)
    manifest = {'dataset': 'Synthea public synthetic CSV sample (not real patients)',
                'source_page': 'https://synthetichealth.github.io/downloads.html', 'download_url': URL,
                'archive_bytes': archive.stat().st_size, 'archive_sha256': hashlib.sha256(archive.read_bytes()).hexdigest(),
                'inventory': inventory, 'patient_rows': len(patients), 'imaging_instance_rows': len(imaging),
                'imaging_studies': len(frame), 'imaged_patients': int(frame.patient_id.nunique()),
                'instance_to_study_reduction': len(imaging)-len(frame), 'orphan_studies': 0,
                'mapping': {'patient_id':'imaging_studies.PATIENT -> patients.Id',
                            'source_record_id':'imaging_studies.Id (study grain)',
                            'study_date':'DATE calendar date; source timestamp retained in lineage',
                            'exam_type':'distinct MODALITY_CODE + BODYSITE_DESCRIPTION per study'},
                'emr_sha256': hashlib.sha256((destination/'emr.csv').read_bytes()).hexdigest()}
    write_manifest(destination/'dataset_manifest.json', manifest)
    return manifest


def generate(rows, destination, dirty=False):
    if rows < 100:
        raise ValueError('Use at least 100 records for scale fixtures')
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    patients = max(1, rows//10)
    records = []
    for i in range(rows):
        p = i % patients
        records.append({'patient_id': f'SYN-{p:07d}', 'patient_name': f'Synthetic {p}',
                        'gender': 'F' if p%2 else 'M', 'birth_date': '1980-01-01',
                        'exam_type': ['CT','MR','XRAY'][i%3],
                        'study_date': (date(2024,1,1)+timedelta(days=i//patients)).strftime('%Y%m%d')})
    k = rows//100 if dirty else 0
    # Disjoint mutations: four invalid groups + one duplicate group.
    for i in range(k):
        records[i]['patient_id'] = ''
        records[k+i]['gender'] = 'INVALID'
        records[2*k+i]['study_date'] = '20240230'
        records[3*k+i]['exam_type'] = ''
        records[4*k+i] = records[5*k+i].copy()
    pd.DataFrame(records).to_csv(destination/'emr.csv', index=False)
    manifest = {'dataset':'deterministic synthetic load fixture; not real-world prevalence',
                'generator':'scripts.prepare_datasets.generate v1; deterministic arithmetic, no randomness',
                'rows':rows, 'nominal_patients':patients, 'dirty':dirty,
                'expected':{'quarantined':4*k,'duplicates':k,'accepted':rows-5*k},
                'bytes':(destination/'emr.csv').stat().st_size,
                'sha256':hashlib.sha256((destination/'emr.csv').read_bytes()).hexdigest()}
    write_manifest(destination/'dataset_manifest.json',manifest)
    return manifest


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='mode',required=True)
    s=sub.add_parser('synthea'); s.add_argument('--archive',required=True); s.add_argument('--destination',required=True)
    s=sub.add_parser('generate'); s.add_argument('--rows',type=int,required=True); s.add_argument('--destination',required=True); s.add_argument('--dirty',action='store_true')
    args=vars(parser.parse_args()); mode=args.pop('mode')
    result=prepare_synthea(**args) if mode=='synthea' else generate(**args)
    print(json.dumps(result,ensure_ascii=False,indent=2))

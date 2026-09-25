"""Read-only profiling of three Synthea CSVs; writes aggregate evidence, never warehouse data."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import zipfile

import pandas as pd

TABLES = ['patients', 'encounters', 'imaging_studies']


def missing(series):
    return series.isna() | series.astype('string').str.strip().eq('')


def profile(archive):
    archive = Path(archive)
    output = {'archive': {'name': archive.name, 'bytes': archive.stat().st_size,
                         'sha256': hashlib.sha256(archive.read_bytes()).hexdigest()},
              'tables': {}, 'checks': {}, 'relationships': {}, 'distributions': {}}
    frames = {}
    with zipfile.ZipFile(archive) as z:
        for name in TABLES:
            members = [n for n in z.namelist() if Path(n).name == name + '.csv']
            if len(members) != 1:
                raise ValueError(f'Expected exactly one {name}.csv')
            body = z.read(members[0])
            # Preserve identifiers and CSV strings; do not treat codes such as NA as null.
            frame = pd.read_csv(io.BytesIO(body), dtype=str, keep_default_na=False)
            frames[name] = frame
            output['tables'][name] = {
                'rows': len(frame), 'bytes': len(body), 'columns': len(frame.columns),
                'sha256': hashlib.sha256(body).hexdigest(),
                'exact_duplicate_excess': int(frame.duplicated().sum()),
                'fields': {c: {'missing': int(missing(frame[c]).sum()),
                               'distinct_nonempty': int(frame.loc[~missing(frame[c]), c].nunique())}
                           for c in frame.columns}}
    p, e, i = (frames[n] for n in TABLES)
    checks = output['checks']
    for name, frame, cols in [('patients',p,['Id','BIRTHDATE','GENDER']),
                              ('encounters',e,['Id','PATIENT','START']),
                              ('imaging_studies',i,['Id','DATE','PATIENT','ENCOUNTER','SERIES_UID','INSTANCE_UID'])]:
        for c in cols:
            checks[f'{name}.{c}.missing'] = int(missing(frame[c]).sum())
    for name, frame in [('patients',p),('encounters',e)]:
        checks[f'{name}.Id.duplicate_excess'] = int(frame.Id.duplicated().sum())
    checks['encounters.patient_orphan_rows'] = int((~e.PATIENT.isin(p.Id)).sum())
    checks['imaging.patient_orphan_rows'] = int((~i.PATIENT.isin(p.Id)).sum())
    checks['imaging.encounter_orphan_rows'] = int((~i.ENCOUNTER.isin(e.Id)).sum())
    merged = i.merge(e[['Id','PATIENT','START','STOP']], left_on='ENCOUNTER', right_on='Id',
                     how='left', validate='many_to_one', suffixes=('', '_enc'))
    checks['imaging.patient_encounter_mismatch_rows'] = int((merged.PATIENT != merged.PATIENT_enc).sum())
    for c in ['PATIENT','ENCOUNTER','DATE']:
        checks[f'imaging.study_conflicting_{c}_keys'] = int(i.groupby('Id')[c].nunique(dropna=False).gt(1).sum())
    checks['imaging.instance_uid_duplicate_excess'] = int(i.INSTANCE_UID.duplicated().sum())
    checks['imaging.series_uid_multiple_studies'] = int(i.groupby('SERIES_UID').Id.nunique().gt(1).sum())
    for c in ['MODALITY_CODE','BODYSITE_CODE','BODYSITE_DESCRIPTION']:
        checks[f'imaging.series_conflicting_{c}_keys'] = int(i.groupby(['Id','SERIES_UID'])[c].nunique(dropna=False).gt(1).sum())
    dates = {}
    for name, frame, cols in [('patients',p,['BIRTHDATE','DEATHDATE']),('encounters',e,['START','STOP']),('imaging_studies',i,['DATE'])]:
        for c in cols:
            s = frame[c]
            parsed = pd.to_datetime(s.mask(missing(s)), utc=True, format='ISO8601', errors='coerce')
            checks[f'{name}.{c}.invalid_nonempty'] = int((~missing(s) & parsed.isna()).sum())
            dates[f'{name}.{c}'] = {'min': str(parsed.min()), 'max':str(parsed.max()), 'missing':int(missing(s).sum())}
    output['date_ranges'] = dates
    starts = pd.to_datetime(e.START,utc=True,format='ISO8601',errors='coerce')
    stops = pd.to_datetime(e.STOP,utc=True,format='ISO8601',errors='coerce')
    checks['encounters.stop_before_start'] = int((stops < starts).sum())
    when = pd.to_datetime(merged.DATE,utc=True,format='ISO8601',errors='coerce')
    start = pd.to_datetime(merged.START,utc=True,format='ISO8601',errors='coerce')
    stop = pd.to_datetime(merged.STOP,utc=True,format='ISO8601',errors='coerce')
    checks['imaging.before_encounter_start_rows'] = int((when < start).sum())
    checks['imaging.after_encounter_stop_rows'] = int((when > stop).sum())
    # These observations inform rules; do not automatically declare clinical invalidity.
    checks['imaging.before_patient_birth_rows'] = int((when < pd.to_datetime(merged.PATIENT.map(p.set_index('Id').BIRTHDATE),utc=True,format='ISO8601',errors='coerce')).sum())
    studies = i.groupby('Id',sort=True).agg(patient=('PATIENT','first'),encounter=('ENCOUNTER','first'),
                date=('DATE','first'),instances=('INSTANCE_UID','size'),series=('SERIES_UID','nunique'),modalities=('MODALITY_CODE','nunique'))
    def counts(series):
        return {str(k):int(v) for k,v in series.value_counts().sort_index().items()}
    dist = output['distributions']
    dist['instances_per_study'] = counts(studies.instances)
    dist['series_per_study'] = counts(studies.series)
    dist['modalities_per_study'] = counts(studies.modalities)
    dist['instance_rows_by_modality'] = counts(i.MODALITY_CODE)
    dist['studies_by_modality'] = counts(i[['Id','MODALITY_CODE']].drop_duplicates().MODALITY_CODE)
    dist['encounter_class'] = counts(e.ENCOUNTERCLASS)
    dist['gender'] = counts(p.GENDER)
    rel = output['relationships']
    rel.update(patients=len(p), encounters=len(e), imaging_rows=len(i), studies=len(studies),
               series=int(i[['Id','SERIES_UID']].drop_duplicates().shape[0]), instances=int(i.INSTANCE_UID.nunique()),
               imaged_patients=int(i.PATIENT.nunique()),imaged_encounters=int(i.ENCOUNTER.nunique()),
               patients_without_imaging=int((~p.Id.isin(i.PATIENT)).sum()),
               patients_without_encounters=int((~p.Id.isin(e.PATIENT)).sum()),
               encounters_without_imaging=int((~e.Id.isin(i.ENCOUNTER)).sum()),
               patient_join_encounters_rows=len(e.merge(p[['Id']],left_on='PATIENT',right_on='Id',validate='many_to_one')),
               imaging_join_encounters_rows=len(merged), multi_modality_studies=int(studies.modalities.gt(1).sum()))
    rel['daily_modality_groups'] = int(i.assign(utc_date=i.DATE.str[:10])[['utc_date','MODALITY_CODE']].drop_duplicates().shape[0])
    # Show the danger of the old patient/date/exam-type key on this actual dataset.
    study_modality = i.groupby('Id').MODALITY_CODE.agg(lambda s:'+'.join(sorted(set(s))))
    simplified = studies.assign(day=studies.date.str[:10],modality=study_modality)
    rel['patient_day_modality_duplicate_excess'] = int(simplified.duplicated(['patient','day','modality']).sum())
    # One pseudonymized real lineage example; identifiers remain only inside the local CSV.
    picked = studies.sort_values(['instances','series'],ascending=False).iloc[0]
    study_id = studies.sort_values(['instances','series'],ascending=False).index[0]
    subset = i.loc[i.Id == study_id]
    patient_idx = p.index[p.Id == picked.patient][0]
    encounter_idx = e.index[e.Id == picked.encounter][0]
    output['lineage_example'] = {
        'source_rows_are':'one-based CSV data record positions, excluding header; not physical text lines',
        'patient_source_row':int(patient_idx+1),'encounter_source_row':int(encounter_idx+1),
        'imaging_source_rows':[int(v+1) for v in subset.index],
        'study_ref':'study-'+hashlib.sha256(study_id.encode()).hexdigest()[:12],
        'patient_ref':'patient-'+hashlib.sha256(picked.patient.encode()).hexdigest()[:12],
        'encounter_ref':'encounter-'+hashlib.sha256(picked.encounter.encode()).hexdigest()[:12],
        'series_count':int(picked.series),'instance_rows':int(picked.instances),
        'modalities':sorted(set(subset.MODALITY_CODE)),
        'input_imaging_sha256':output['tables']['imaging_studies']['sha256']}
    # Internal cross-checks use different aggregations.
    assertions = {
       'instance_group_sum':int(studies.instances.sum())==len(i),
       'patient_partition':rel['imaged_patients']+rel['patients_without_imaging']==len(p),
       'encounter_partition':rel['imaged_encounters']+rel['encounters_without_imaging']==len(e),
       'study_distribution':sum(dist['instances_per_study'].values())==len(studies),
       'weighted_instance_distribution':sum(int(k)*v for k,v in dist['instances_per_study'].items())==len(i)}
    output['profile_self_checks'] = assertions
    if not all(assertions.values()): raise ValueError('Profile reconciliation failed')
    return output


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive',default='data/external/synthea_csv.zip')
    parser.add_argument('--output',default='docs/stage-b/profile.json')
    args=parser.parse_args()
    result=profile(args.archive)
    Path(args.output).write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({k:result[k] for k in ['relationships','checks','distributions','lineage_example']},ensure_ascii=False,indent=2))

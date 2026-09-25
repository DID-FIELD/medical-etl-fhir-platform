"""Synthea three-table snapshot ETL with explicit grain, row dispositions and lineage."""
from __future__ import annotations
import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import time
from datetime import date, datetime, timezone, timedelta
from uuid import uuid5, NAMESPACE_URL
import zipfile

import pandas as pd

REQUIRED = {
    'patients': ['Id', 'BIRTHDATE', 'DEATHDATE', 'GENDER'],
    'encounters': ['Id','PATIENT','START','STOP','ENCOUNTERCLASS','CODE','DESCRIPTION'],
    'imaging_studies': ['Id','PATIENT','ENCOUNTER','DATE','SERIES_UID','INSTANCE_UID',
                       'MODALITY_CODE','BODYSITE_CODE','BODYSITE_DESCRIPTION','SOP_CODE'],
}
# Ordered contracts used by file output, PostgreSQL loading and checks.
COLUMNS = {
 'dim_patient': ['patient_key','gender','birth_year','source_sha256','source_row'],
 'dwd_encounter': ['encounter_key','patient_key','start_at','stop_at','encounter_class','code','description','source_sha256','source_row'],
 'dwd_imaging_study': ['study_key','patient_key','encounter_key','started_at','source_sha256','source_row'],
 'dwd_imaging_series': ['study_key','series_uid','modality_code','body_site_code','body_site_description'],
 'dwd_imaging_instance': ['instance_uid','study_key','series_uid','sop_code','source_sha256','source_row'],
 'bridge_study_modality': ['study_key','modality_code'],
 'dws_patient_imaging_summary': ['patient_key','exam_count','latest_exam_at','modality_count'],
 'dws_imaging_daily_modality': ['stat_date','modality_code','exam_count','patient_count'],
 'ads_patient_imaging_profile': ['patient_key','gender','birth_year','exam_count','latest_exam_at','modality_count'],
}
KEYS = {
 'dim_patient':['patient_key'], 'dwd_encounter':['encounter_key'], 'dwd_imaging_study':['study_key'],
 'dwd_imaging_series':['study_key','series_uid'], 'dwd_imaging_instance':['instance_uid'],
 'bridge_study_modality':['study_key','modality_code'], 'dws_patient_imaging_summary':['patient_key'],
 'dws_imaging_daily_modality':['stat_date','modality_code'], 'ads_patient_imaging_profile':['patient_key'],
}


def key(kind, value):
    return str(uuid5(NAMESPACE_URL, 'synthea:' + kind + ':' + value))


def sha(data):
    return hashlib.sha256(data).hexdigest()


def timestamp(value):
    parsed = datetime.fromisoformat(value.replace('Z','+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('timezone required')
    return parsed.astimezone(timezone.utc).isoformat()


def read_archive(path):
    sources, inventory = {}, {}
    with zipfile.ZipFile(path) as archive:
        for table, required in REQUIRED.items():
            names=[n for n in archive.namelist() if Path(n).name == table+'.csv']
            if len(names)!=1: raise ValueError(f'Expected exactly one {table}.csv')
            body=archive.read(names[0])
            reader=csv.DictReader(io.StringIO(body.decode('utf-8-sig')))
            if not set(required).issubset(reader.fieldnames or []):
                raise ValueError(f'{table}: missing required columns')
            if len(reader.fieldnames) != len(set(reader.fieldnames)):
                raise ValueError(f'{table}: duplicate CSV columns')
            rows=list(reader)
            if any(None in row or any(v is None for v in row.values()) for row in rows):
                raise ValueError(f'{table}: inconsistent CSV row width')
            sources[table]=rows
            inventory[table]={'file':names[0],'rows':len(rows),'bytes':len(body),'sha256':sha(body)}
    return sources, inventory


def date_zone(offset):
    if not re.fullmatch(r'[+-](?:0\d|1[0-4]):[0-5]\d', offset):
        raise ValueError('Expected date timezone offset such as +00:00 or +08:00')
    minutes = int(offset[1:3]) * 60 + int(offset[4:6])
    if minutes > 840: raise ValueError('Date timezone offset exceeds 14 hours')
    return timezone(timedelta(minutes=minutes * (-1 if offset[0] == '-' else 1)))


def transform(sources, inventory, birth_date_offset='+00:00'):
    birth_zone = date_zone(birth_date_offset)
    def birth_day(value):
        return datetime.fromisoformat(value).astimezone(birth_zone).date().isoformat()
    ledger=[]
    def resolve(table, identity, validator):
        rows=sources[table]
        # Resolve conflicts before assigning dispositions, including earlier duplicates.
        signatures={}; conflicts=set(); seen=set(); accepted=[]
        for row in rows:
            identity_value=row[identity]
            signature=sha(json.dumps(row,sort_keys=True,separators=(',',':')).encode())
            previous=signatures.setdefault(identity_value,signature)
            if previous != signature: conflicts.add(identity_value)
        for n,row in enumerate(rows,1):
            identity_value=row[identity]
            reasons=[]
            if not identity_value.strip(): reasons.append('missing_business_key')
            if identity_value in conflicts: reasons.append('conflicting_business_key')
            reasons.extend(validator(row))
            disposition='quarantined' if reasons else ('duplicate' if identity_value in seen else 'accepted')
            entry={'source_table':table,'source_row':n,'source_sha256':inventory[table]['sha256'],
                   'disposition':disposition,'reasons':reasons or (['exact_duplicate'] if identity_value in seen else [])}
            ledger.append(entry)
            seen.add(identity_value)
            if disposition=='accepted': accepted.append((row,entry))
        return accepted
    def valid_patient(row):
        reasons=[]
        try: birth=date.fromisoformat(row['BIRTHDATE'])
        except ValueError: reasons.append('invalid_birthdate'); birth=None
        if row['DEATHDATE']:
            try:
                death=date.fromisoformat(row['DEATHDATE'])
                if birth and death<birth: reasons.append('death_before_birth')
            except ValueError: reasons.append('invalid_deathdate')
        if row['GENDER'] not in {'M','F','UNKNOWN'}: reasons.append('invalid_gender')
        return reasons
    patients=resolve('patients','Id',valid_patient)
    patient_map={row['Id']:row for row,_ in patients}
    def valid_encounter(row):
        reasons=[]
        if row['PATIENT'] not in patient_map: reasons.append('unknown_patient')
        try:
            start=timestamp(row['START'])
            if row['STOP'] and timestamp(row['STOP'])<start: reasons.append('stop_before_start')
            if row['PATIENT'] in patient_map and birth_day(start)<patient_map[row['PATIENT']]['BIRTHDATE']:
                reasons.append('encounter_before_birth')
        except ValueError: reasons.append('invalid_encounter_time')
        return reasons
    encounters=resolve('encounters','Id',valid_encounter)
    encounter_map={row['Id']:row for row,_ in encounters}
    warnings=[]
    def valid_imaging(row):
        reasons=[]
        for col in ['Id','PATIENT','ENCOUNTER','SERIES_UID','MODALITY_CODE','SOP_CODE']:
            if not row[col].strip(): reasons.append('missing_'+col.lower())
        if row['PATIENT'] not in patient_map: reasons.append('unknown_patient')
        encounter=encounter_map.get(row['ENCOUNTER'])
        if not encounter: reasons.append('unknown_encounter')
        elif encounter['PATIENT']!=row['PATIENT']: reasons.append('patient_encounter_mismatch')
        try:
            when=timestamp(row['DATE'])
            if row['PATIENT'] in patient_map and birth_day(when)<patient_map[row['PATIENT']]['BIRTHDATE']:
                reasons.append('imaging_before_birth')
            if encounter and (when<timestamp(encounter['START']) or (encounter['STOP'] and when>timestamp(encounter['STOP']))):
                warnings.append({'rule':'imaging_outside_encounter','source_instance_ref':key('instance',row['INSTANCE_UID'])})
        except ValueError: reasons.append('invalid_imaging_time')
        return reasons
    imaging=resolve('imaging_studies','INSTANCE_UID',valid_imaging)
    # A partial or inconsistent study must not silently look complete downstream.
    study_meta={}; series_meta={}; bad_studies=set()
    for row in sources['imaging_studies']:
        study_meta.setdefault(row['Id'], {c:row[c] for c in ['PATIENT','ENCOUNTER','DATE']})
        if any(study_meta[row['Id']][c] != row[c] for c in ['PATIENT','ENCOUNTER','DATE']):
            bad_studies.add(row['Id'])
        series_meta.setdefault(row['SERIES_UID'], {c:row[c] for c in ['Id','MODALITY_CODE','BODYSITE_CODE','BODYSITE_DESCRIPTION']})
        if any(series_meta[row['SERIES_UID']][c] != row[c] for c in ['Id','MODALITY_CODE','BODYSITE_CODE','BODYSITE_DESCRIPTION']):
            bad_studies.add(row['Id'])
            bad_studies.add(series_meta[row['SERIES_UID']]['Id'])
    for entry in ledger:
        if entry['source_table']=='imaging_studies' and entry['disposition']=='quarantined':
            bad_studies.add(sources['imaging_studies'][entry['source_row']-1]['Id'])
    for entry in ledger:
        if entry['source_table']=='imaging_studies':
            row=sources['imaging_studies'][entry['source_row']-1]
            if row['Id'] in bad_studies:
                entry['disposition']='quarantined'
                entry['reasons']=sorted(set(entry['reasons']+['incomplete_or_conflicting_study']))
    imaging=[(row,entry) for row,entry in imaging if entry['disposition']=='accepted']
    result={name:[] for name in COLUMNS}
    def lineage(entry):return {c:entry[c] for c in ['source_sha256','source_row']}
    for row,entry in patients:
        result['dim_patient'].append(dict(patient_key=key('patient',row['Id']),gender=row['GENDER'],
                                          birth_year=date.fromisoformat(row['BIRTHDATE']).year,**lineage(entry)))
    for row,entry in encounters:
        result['dwd_encounter'].append(dict(encounter_key=key('encounter',row['Id']),patient_key=key('patient',row['PATIENT']),
          start_at=timestamp(row['START']),stop_at=timestamp(row['STOP']) if row['STOP'] else None,
          encounter_class=row['ENCOUNTERCLASS'],code=row['CODE'],description=row['DESCRIPTION'],**lineage(entry)))
    studies={};series={}; bridge=set()
    for row,entry in imaging:
        study_key=key('study',row['Id']); series_uid=row['SERIES_UID']
        studies.setdefault(study_key,dict(study_key=study_key,patient_key=key('patient',row['PATIENT']),
           encounter_key=key('encounter',row['ENCOUNTER']),started_at=timestamp(row['DATE']),**lineage(entry)))
        series.setdefault((study_key,series_uid),dict(study_key=study_key,series_uid=series_uid,
           modality_code=row['MODALITY_CODE'],body_site_code=row['BODYSITE_CODE'],body_site_description=row['BODYSITE_DESCRIPTION']))
        result['dwd_imaging_instance'].append(dict(instance_uid=row['INSTANCE_UID'],study_key=study_key,
           series_uid=series_uid,sop_code=row['SOP_CODE'],**lineage(entry)))
        bridge.add((study_key,row['MODALITY_CODE']))
    result['dwd_imaging_study']=list(studies.values());result['dwd_imaging_series']=list(series.values())
    result['bridge_study_modality']=[dict(study_key=s,modality_code=m) for s,m in sorted(bridge)]
    for patient in result['dim_patient']:
        own=[s for s in studies.values() if s['patient_key']==patient['patient_key']]
        own_keys={s['study_key'] for s in own}
        summary=dict(patient_key=patient['patient_key'],exam_count=len(own),
                     latest_exam_at=max((s['started_at'] for s in own),default=None),
                     modality_count=len({m for s,m in bridge if s in own_keys}))
        result['dws_patient_imaging_summary'].append(summary)
        result['ads_patient_imaging_profile'].append({**{c:patient[c] for c in ['patient_key','gender','birth_year']},**summary})
    daily={}
    for s,m in bridge: daily.setdefault((studies[s]['started_at'][:10],m),[]).append(studies[s])
    result['dws_imaging_daily_modality']=[dict(stat_date=d,modality_code=m,exam_count=len(rows),
        patient_count=len({r['patient_key'] for r in rows})) for (d,m),rows in daily.items()]
    for name, rows in result.items(): rows.sort(key=lambda row:tuple(str(row[c]) for c in KEYS[name]))
    ledger.sort(key=lambda entry:(entry['source_table'],entry['source_row']))
    return result,ledger,warnings


def reconcile(tables, ledger, inventory):
    checks={}
    for name,rows in tables.items():
        checks[name+'.unique_key']=len({tuple(row[c] for c in KEYS[name]) for row in rows})==len(rows)
    for source,meta in inventory.items():
        dispositions=[e for e in ledger if e['source_table']==source]
        checks[source+'.input_balance']=len(dispositions)==meta['rows'] and len({e['source_row'] for e in dispositions})==meta['rows']
    patients={r['patient_key'] for r in tables['dim_patient']}
    encounters={r['encounter_key']:r['patient_key'] for r in tables['dwd_encounter']}
    studies={r['study_key']:r for r in tables['dwd_imaging_study']}
    series={(r['study_key'],r['series_uid']) for r in tables['dwd_imaging_series']}
    checks['encounter_patient_fk']=set(encounters.values())<=patients
    checks['study_parent_consistency']=all(r['patient_key'] in patients and encounters.get(r['encounter_key'])==r['patient_key'] for r in studies.values())
    checks['series_study_fk']=all(s in studies for s,_ in series)
    checks['instance_series_fk']=all((r['study_key'],r['series_uid']) in series for r in tables['dwd_imaging_instance'])
    checks['bridge_study_fk']=all(r['study_key'] in studies for r in tables['bridge_study_modality'])
    for name in ['dws_patient_imaging_summary','ads_patient_imaging_profile']:
        checks[name+'.patient_coverage']={r['patient_key'] for r in tables[name]}==patients
        checks[name+'.exam_balance']=sum(r['exam_count'] for r in tables[name])==len(studies)
    checks['daily_bridge_balance']=sum(r['exam_count'] for r in tables['dws_imaging_daily_modality'])==len(tables['bridge_study_modality'])
    for table,source in [('dim_patient','patients'),('dwd_encounter','encounters'),('dwd_imaging_instance','imaging_studies')]:
        accepted={e['source_row'] for e in ledger if e['source_table']==source and e['disposition']=='accepted'}
        checks[table+'.accepted_lineage']={r['source_row'] for r in tables[table]}==accepted
    if not all(checks.values()): raise ValueError('Reconciliation failed: '+str([k for k,v in checks.items() if not v]))
    return checks


def write_json(path, value):
    path.write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')


def run(archive, output_root='output/synthea', run_id=None):
    started=time.perf_counter();run_id=run_id or datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    if not re.fullmatch('[A-Za-z0-9_-]{1,100}',run_id):raise ValueError('Invalid run_id')
    root=Path(output_root);target=root/'runs'/run_id;target.mkdir(parents=True,exist_ok=False)
    manifest={'run_id':run_id,'status':'RUNNING','model_version':'synthea-v1','backend':'parquet',
              'python':platform.python_version(),'pandas':pd.__version__,
              'code_sha256':sha(Path(__file__).read_bytes()),'started_at':datetime.now(timezone.utc).isoformat()}
    write_json(target/'manifest.json',manifest)
    try:
        sources,inventory=read_archive(archive)
        manifest['source']=inventory;manifest['archive_sha256']=sha(Path(archive).read_bytes())
        for name,rows in sources.items():write_json(target/f'ods_{name}.json',rows)
        if not sources['patients']:raise ValueError('Empty patient snapshot is not publishable')
        tables,ledger,warnings=transform(sources,inventory)
        write_json(target/'dispositions.json',ledger)
        if not tables['dim_patient']:raise ValueError('No valid patients; refusing to publish')
        checks=reconcile(tables,ledger,inventory)
        for name,rows in tables.items():
            write_json(target/f'{name}.json',rows)
            frame=pd.DataFrame(rows,columns=COLUMNS[name]);frame.insert(0,'run_id',run_id)
            frame.to_parquet(target/f'{name}.parquet',index=False)
            readback=pd.read_parquet(target/f'{name}.parquet')
            checks[name+'.readback']=readback.equals(frame)
        if not all(checks.values()):raise ValueError('Parquet readback differs')
        counts={name:len(rows) for name,rows in tables.items()}
        quality={name:{kind:sum(e['source_table']==name and e['disposition']==kind for e in ledger)
                 for kind in ['accepted','quarantined','duplicate']} for name in sources}
        manifest.update(status='SUCCESS',counts=counts,quality=quality,checks=checks,warnings=warnings,
            zero_exam_patients=sum(r['exam_count']==0 for r in tables['ads_patient_imaging_profile']),
            elapsed_seconds=round(time.perf_counter()-started,4),
            artifacts={p.name:sha(p.read_bytes()) for p in sorted(target.iterdir()) if p.name!='manifest.json'})
        write_json(target/'manifest.json',manifest)
        pending=root/f'.current-{run_id}.json';write_json(pending,{'run_id':run_id})
        os.replace(pending,root/'current.json')
        return manifest
    except Exception as exc:
        manifest.update(status='FAILED',error_type=type(exc).__name__,error=str(exc))
        write_json(target/'manifest.json',manifest)
        raise


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive',default='data/external/synthea_csv.zip')
    parser.add_argument('--output-root',default='output/synthea')
    parser.add_argument('--run-id')
    result=run(**vars(parser.parse_args()))
    print(json.dumps({k:result[k] for k in ['run_id','status','counts','quality','zero_exam_patients','elapsed_seconds']},indent=2))

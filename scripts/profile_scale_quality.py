"""Summarize scale-test quality dispositions without altering source or rules."""
import argparse,csv,json
from collections import Counter
from datetime import datetime,timezone,timedelta
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--population',type=int,required=True);a=p.parse_args();n=a.population
root=Path(f'output/scale-v4/runs/p{n}-r1')
ledger=json.loads((root/'dispositions.json').read_text())
instances=json.loads((root/'dwd_imaging_instance.json').read_text())
with Path(f'data/generated/scale-v4-p{n}/csv/patients.csv').open(encoding='utf-8') as f:patients={r['Id']:r for r in csv.DictReader(f)}
with Path(f'data/generated/scale-v4-p{n}/csv/encounters.csv').open(encoding='utf-8') as f:encounters=list(csv.DictReader(f))
reasons=Counter(reason for r in ledger if r['disposition']=='quarantined' for reason in r['reasons'])
studies=Counter(r['study_key'] for r in instances)
matching=0
for r in ledger:
 if r['source_table']=='encounters' and 'encounter_before_birth' in r['reasons']:
  e=encounters[r['source_row']-1]
  localdate=datetime.fromisoformat(e['START'].replace('Z','+00:00')).astimezone(timezone(timedelta(hours=8))).date().isoformat()
  matching+=localdate>=patients[e['PATIENT']]['BIRTHDATE']
result={'population_requested':n,'quarantine_reasons':dict(reasons),'birth_flags_resolved_by_UTC_plus_8_date_comparison':matching,'interpretation':'Diagnostic comparison only; pipeline rules and exported data were not modified. Consistent with source default-zone date versus UTC timestamp mismatch.', 'largest_study_instance_counts':[v for _,v in studies.most_common(5)]}
Path(f'docs/stage-e/quality-p{n}.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result))

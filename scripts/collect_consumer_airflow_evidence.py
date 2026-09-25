"""Copy completed mixed-worker Airflow evidence without replacing prior evidence."""
import argparse
import json
from pathlib import Path
import re
import shutil

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--session',required=True)
parser.add_argument('--stage',choices=('stage-l','stage-m'),default='stage-l')
args=parser.parse_args()
if not re.fullmatch(r'[A-Za-z0-9_-]{1,80}',args.session):raise ValueError('Invalid session')
project=Path('/mnt/f/project/medical-etl-fhir-platform')
native=Path('/opt/medical-etl-airflow/runs')/args.session
shared=project/'output'/args.session
report=json.loads((shared/'acceptance.json').read_text(encoding='utf-8'))
if report['status']=='RUNNING':raise ValueError('Acceptance still running')
target=project/'docs'/args.stage/args.session
target.mkdir(parents=True,exist_ok=False)

def copy(source,destination):
    if source.is_file():
        destination.parent.mkdir(parents=True,exist_ok=True)
        with source.open('rb') as reader,destination.open('xb') as writer:shutil.copyfileobj(reader,writer)

for path in shared.glob('*.json'):copy(path,target/path.name)
for path in shared.glob('*.log'):copy(path,target/path.name)
for pattern in ('manifest.json','airflow-try-*.json'):
    for path in (shared/'results').rglob(pattern):copy(path,target/path.relative_to(shared))
for name in ('cli.log','scheduler.log','windows-probe.log'):copy(native/name,target/name)
copy(native.with_suffix('.log'),target/'acceptance.log')
for path in (native/'airflow/logs').rglob('attempt=*.log'):copy(path,target/path.relative_to(native))
test_logs = ('consumer-workflow-tests-r1.log','consumer-workflow-tests-r2.log','consumer-workflow-tests-r3.log') if args.stage == 'stage-l' else ('full-consumers-scale-tests-r1.log',)
for name in test_logs:
    copy(project/'output'/name,target/name)
print(target)

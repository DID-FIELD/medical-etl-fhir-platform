"""Collect completed full Spark Airflow evidence without overwriting prior receipts."""
import argparse
import json
from pathlib import Path
import shutil

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--session', required=True)
args = parser.parse_args()
if not __import__('re').fullmatch(r'[A-Za-z0-9_-]{1,80}', args.session):
    raise ValueError('Invalid session name')
runs = Path('/opt/medical-etl-airflow/runs')
source = runs / args.session
report = json.loads((source / 'acceptance.json').read_text())
if report['status'] == 'RUNNING':
    raise ValueError('Acceptance still running')
target = Path('/mnt/f/project/medical-etl-fhir-platform/docs/stage-j') / args.session
target.mkdir(parents=True, exist_ok=False)
files = [source / 'acceptance.json', source / 'cli.log', source / 'scheduler.log']
files += list((source / 'results').rglob('manifest.json'))
files += list((source / 'results').rglob('airflow-try-*.json'))
files += list((source / 'airflow/logs').rglob('attempt=*.log'))
for path in files:
    if path.is_file():
        destination = target / path.relative_to(source)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with path.open('rb') as reader, destination.open('xb') as writer:
            shutil.copyfileobj(reader, writer)
log = runs / (args.session + '.log')
if log.is_file():
    with log.open('rb') as reader, (target / 'acceptance.log').open('xb') as writer:
        shutil.copyfileobj(reader, writer)
edge_log = Path('/mnt/f/project/medical-etl-fhir-platform/docs/stage-i/tests-wsl-r5.log')
if not edge_log.exists():
    with (runs / 'spark-full-tests-r5.log').open('rb') as reader, edge_log.open('xb') as writer:
        shutil.copyfileobj(reader, writer)
print(target)

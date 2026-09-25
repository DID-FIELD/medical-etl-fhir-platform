"""Copy completed Stage I receipts to the workspace, without overwriting evidence."""
from pathlib import Path
import shutil

source = Path('/opt/medical-etl-airflow/runs')
target = Path('/mnt/f/project/medical-etl-fhir-platform/docs/stage-i')
pairs = [(source / ('spark-full-tests-r' + n + '.log'), target / ('tests-wsl-r' + n + '.log')) for n in ['2', '3', '4']]
for population in ['p1000', 'p10000']:
    for relative, label in [('measurement.json', 'measurement'), ('snapshot/manifest.json', 'manifest')]:
        pairs.append((source / ('spark-full-' + population + '-r1') / relative, target / (label + '-' + population + '-r1.json')))
for src, dst in pairs:
    if src.exists() and not dst.exists():
        if src.suffix == '.json':
            import json
            if json.loads(src.read_text())['status'] == 'RUNNING':
                continue
        with src.open('rb') as reader, dst.open('xb') as writer:
            shutil.copyfileobj(reader, writer)
        print(dst)

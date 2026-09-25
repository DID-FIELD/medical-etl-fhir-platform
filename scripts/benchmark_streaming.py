"""Measure complete streaming file ETL under a 500 MiB sampled RSS guard."""
import argparse
import json
import shutil
import re
import sys

from scripts.benchmark_synthea import ROOT, available, measured, save
from src.synthea_pipeline import COLUMNS


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--population', type=int, choices=[1000, 10000], required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--birth-date-offset', default='+00:00')
    parser.add_argument('--compare-legacy', action='store_true')
    args = parser.parse_args()
    if not re.fullmatch('[A-Za-z0-9_-]{1,100}', args.run_id):
        parser.error('run-id must contain only letters, digits, underscores or hyphens')
    if args.compare_legacy and (args.population != 1000 or args.birth_date_offset != '+00:00'):
        parser.error('Legacy comparison requires the 1000 population and +00:00 contract')
    if shutil.disk_usage(ROOT).free < 15*1024**3 or available() < 1024**3:
        raise RuntimeError('Require 15 GiB disk and 1 GiB available RAM')
    root = ROOT / 'output/scale-stream-final'
    target = root / 'runs' / args.run_id
    evidence = ROOT / 'docs/stage-e' / (args.run_id + '.json')
    if target.exists() or evidence.exists():
        raise FileExistsError(args.run_id)
    archive = ROOT / f'data/generated/scale-v4-p{args.population}/source.zip'
    log = archive.parent / (args.run_id + '.log')
    command = [sys.executable, '-m', 'src.streaming_pipeline', '--archive', str(archive),
               '--output-root', str(root), '--run-id', args.run_id,
               '--birth-date-offset', args.birth_date_offset]
    measurement = measured(command, log, rss_limit_bytes=500*1024**2)
    path = target / 'manifest.json'
    manifest = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
    if measurement['exit_code'] != 0 or measurement['stop_reason']:
        if manifest.get('status') != 'FAILED':
            manifest.update(status='FAILED', error_type='ExternalProcessTermination')
        manifest.update(exit_code=measurement['exit_code'], stop_reason=measurement['stop_reason'])
        if path.exists():
            save(path, manifest)
    result = {'population_requested': args.population, 'rss_limit_bytes': 500*1024**2,
              'measurement': measurement, 'manifest': manifest}
    result['passed'] = (manifest.get('status') == 'SUCCESS' and measurement['exit_code'] == 0
                        and measurement['sampled_peak_tree_rss_bytes'] <= 500*1024**2)
    if args.compare_legacy and result['passed']:
        legacy = ROOT / 'output/scale-v4/runs/p1000-r1'
        original = json.loads((legacy / 'manifest.json').read_text(encoding='utf-8'))
        checks = {'counts': original['counts'] == manifest['counts'],
                  'quality': original['quality'] == manifest['quality'],
                  'source': original['source'] == manifest['source']}
        for name in [*COLUMNS, 'dispositions']:
            checks[name] = json.loads((legacy / (name+'.json')).read_text(encoding='utf-8')) == json.loads((target / (name+'.json')).read_text(encoding='utf-8'))
        result['legacy_equivalence'] = checks
        result['passed'] &= all(checks.values())
    save(evidence, result)
    print(json.dumps({'passed': result['passed'], 'measurement': measurement,
                      'legacy_equivalence': result.get('legacy_equivalence'),
                      'counts': manifest.get('counts'), 'quality': manifest.get('quality')}, indent=2))
    if not result['passed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()

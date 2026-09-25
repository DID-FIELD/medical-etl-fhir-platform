"""Export an existing snapshot under the sampled 500 MiB guard."""
import argparse
import json
import sys
from pathlib import Path
from scripts.benchmark_synthea import measured, save


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--evidence', required=True)
    args = parser.parse_args()
    output, evidence = Path(args.output).resolve(), Path(args.evidence).resolve()
    if output.exists() or evidence.exists():
        raise FileExistsError('Output or evidence already exists')
    evidence.parent.mkdir(parents=True, exist_ok=True)
    measurement = measured([sys.executable, '-m', 'src.fhir.synthea_export',
        '--snapshot', args.snapshot, '--output', args.output], evidence.with_suffix('.log'),
        rss_limit_bytes=500*1024**2)
    manifest_path = output / 'manifest.json'
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    if measurement['exit_code'] != 0 or measurement['stop_reason']:
        manifest.update(status='FAILED', error_type='ExternalProcessFailure',
                        stop_reason=measurement['stop_reason'])
        if manifest_path.exists():
            save(manifest_path, manifest)
    passed = manifest.get('status') == 'SUCCESS' and measurement['exit_code'] == 0 and not measurement['stop_reason']
    save(evidence, dict(passed=passed, measurement=measurement, manifest=manifest))
    print(json.dumps(dict(passed=passed, measurement=measurement, counts=manifest.get('counts')), indent=2))
    if not passed:
        raise SystemExit(1)


if __name__ == '__main__':
    main()

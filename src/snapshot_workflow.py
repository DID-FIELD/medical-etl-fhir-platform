"""Retry-safe isolated snapshot tasks shared by Airflow and local validation."""
import json
from pathlib import Path
from src.stream_io import file_hash


def verify_snapshot(snapshot):
    snapshot = Path(snapshot)
    manifest = json.loads((snapshot / 'manifest.json').read_text(encoding='utf-8'))
    if manifest['status'] != 'SUCCESS':
        raise ValueError('Snapshot must be successful')
    for name, expected in manifest['artifacts'].items():
        path = (snapshot / name).resolve()
        if path.parent != snapshot.resolve():
            raise ValueError('Artifact must be a direct snapshot child')
        if file_hash(path) != expected:
            raise ValueError('Snapshot artifact hash mismatch')
    return manifest['run_id']


def run_component(snapshot, output, component):
    snapshot, output = Path(snapshot), Path(output)
    if component not in ('fhir', 'spark'):
        raise ValueError('Unknown component')
    verify_snapshot(snapshot)
    source_hash = file_hash(snapshot / 'manifest.json')
    if output.exists():
        report = json.loads((output / 'manifest.json').read_text(encoding='utf-8'))
        if report.get('status') != 'SUCCESS' or report.get('source_manifest_sha256') != source_hash:
            raise ValueError('Existing result is failed, incomplete, or belongs to another snapshot; use a new output')
        if component == 'fhir':
            expected_names = {k + '.ndjson' for k in ('Patient', 'Encounter', 'ImagingStudy')}
            if set(report.get('artifacts', {})) != expected_names:
                raise ValueError('Incomplete FHIR artifact inventory')
            for name, digest in report['artifacts'].items():
                if file_hash(output / name) != digest:
                    raise ValueError('Existing result artifact hash mismatch')
        elif (set(report.get('checks', {})) != {'dws_patient_imaging_summary', 'dws_imaging_daily_modality', 'ads_patient_imaging_profile'}
              or any(v['missing'] or v['extra'] or v['expected_count'] != v['actual_count']
                     for v in report['checks'].values())):
            raise ValueError('Existing Spark comparison failed')
        return str(output / 'manifest.json')
    if component == 'fhir':
        from src.fhir.synthea_export import export_snapshot
        export_snapshot(snapshot, output)
    else:
        from src.spark.synthea_compare import compare_snapshot
        compare_snapshot(snapshot, output)
    return str(output / 'manifest.json')


def attempt_directory(output_root, component, run_id, try_number):
    from hashlib import sha256
    if component not in ('fhir', 'spark') or not run_id or type(try_number) is not int or try_number < 1:
        raise ValueError('Invalid task identity')
    # Airflow run IDs may contain colons, slashes, or user-supplied text.
    run_key = sha256(run_id.encode('utf-8')).hexdigest()
    return Path(output_root) / run_key / component / f'attempt-{try_number:04d}'


def run_attempt(snapshot, output_root, component, run_id, try_number):
    target = attempt_directory(output_root, component, run_id, try_number)
    # Each task attempt owns a new directory. Failed/incomplete attempts remain intact.
    # A successful earlier export can survive an Airflow worker failure after export.
    for number in range(try_number, 0, -1):
        candidate = attempt_directory(output_root, component, run_id, number)
        manifest = candidate / 'manifest.json'
        if manifest.is_file():
            try:
                report = json.loads(manifest.read_text(encoding='utf-8'))
            except (ValueError, UnicodeError):
                if number == try_number:
                    raise ValueError('Current attempt has an invalid manifest') from None
                continue
            if report.get('status') == 'SUCCESS':
                # A corrupted SUCCESS is an error, never silently recompute over it.
                return run_component(snapshot, candidate, component)
    return run_component(snapshot, target, component)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', required=True)
    parser.add_argument('--output-root', required=True)
    parser.add_argument('--component', choices=('fhir', 'spark'), required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--try-number', required=True, type=int)
    args = parser.parse_args()
    result = run_attempt(args.snapshot, args.output_root, args.component, args.run_id, args.try_number)
    print('WORKFLOW_RESULT=' + json.dumps(result))

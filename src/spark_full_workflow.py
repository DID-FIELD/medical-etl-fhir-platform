"""Retry-safe full CSV Spark workflow, isolated from published snapshots."""
import argparse
from hashlib import sha256
import json
from pathlib import Path

from src.snapshot_workflow import verify_snapshot
from src.stream_io import file_hash

TABLES = {'dim_patient', 'dwd_encounter', 'dwd_imaging_study', 'dwd_imaging_series',
          'dwd_imaging_instance', 'bridge_study_modality', 'dws_patient_imaging_summary',
          'dws_imaging_daily_modality', 'ads_patient_imaging_profile'}
DATASETS = TABLES | {'dispositions', 'warnings', 'ods_patients', 'ods_encounters', 'ods_imaging_studies'}


def attempt_directory(output_root, run_id, try_number):
    if not isinstance(run_id, str) or not run_id or type(try_number) is not int or try_number < 1:
        raise ValueError('Invalid task identity')
    return Path(output_root) / sha256(run_id.encode()).hexdigest() / 'spark-full' / f'attempt-{try_number:04d}'


def verify_inputs(archive, baseline, offset):
    verify_snapshot(baseline)
    report = json.loads((Path(baseline) / 'manifest.json').read_text(encoding='utf-8'))
    archive_hash = file_hash(archive)
    if report.get('archive_sha256') != archive_hash or report.get('birth_date_offset', '+00:00') != offset:
        raise ValueError('Baseline archive or birth date offset mismatch')
    return archive_hash, file_hash(Path(baseline) / 'manifest.json')


def verify_result(output, run_id, archive_hash, baseline_hash, offset):
    output = Path(output).resolve()
    report = json.loads((output / 'manifest.json').read_text(encoding='utf-8'))
    if (report.get('status') != 'SUCCESS' or report.get('backend') != 'spark-full-csv-etl'
            or report.get('run_id') != run_id or report.get('archive_sha256') != archive_hash
            or report.get('birth_date_offset') != offset
            or report.get('baseline', {}).get('source_manifest_sha256') != baseline_hash):
        raise ValueError('Existing result identity or status mismatch')
    comparisons = report['baseline'].get('checks', {})
    if set(comparisons) != DATASETS or any(v != {'missing': 0, 'extra': 0} for v in comparisons.values()):
        raise ValueError('Existing result baseline comparison incomplete')
    checks = report.get('checks', {})
    expected_checks = ({n + '.unique_key' for n in TABLES}
        | {n + '.input_balance' for n in ('patients', 'encounters', 'imaging_studies')}
        | {'encounter_patient_fk', 'study_patient_fk', 'study_parent_consistency', 'series_study_fk',
           'instance_series_fk', 'bridge_study_fk', 'daily_bridge_balance'}
        | {n + '.' + c for n in ('dws_patient_imaging_summary', 'ads_patient_imaging_profile')
           for c in ('patient_coverage', 'exam_balance')}
        | {n + '.accepted_lineage' for n in ('dim_patient', 'dwd_encounter', 'dwd_imaging_instance')})
    if set(checks) != expected_checks or any(v is not True for v in checks.values()):
        raise ValueError('Existing result reconciliation incomplete')
    expected_code = {p.name: file_hash(p) for p in (Path(__file__).parent / 'spark').glob('synthea_*.py')}
    if report.get('code_sha256') != expected_code:
        raise ValueError('Existing result code mismatch; use a new run ID')
    artifacts = report.get('artifacts', {})
    actual = set()
    for path in output.rglob('*'):
        if path.is_symlink():
            raise ValueError('Artifact symlinks are not allowed')
        if path.is_file() and path != output / 'manifest.json':
            actual.add(path.relative_to(output).as_posix())
    if not artifacts or set(artifacts) != actual:
        raise ValueError('Existing result artifact inventory mismatch')
    for name, digest in artifacts.items():
        path = (output / name).resolve()
        if not path.is_relative_to(output) or file_hash(path) != digest:
            raise ValueError('Existing result artifact hash mismatch')
    for dataset in DATASETS:
        if not any(name.startswith(dataset + '.parquet/part-') and name.endswith('.parquet') for name in artifacts):
            raise ValueError('Existing result dataset missing: ' + dataset)
    return str(output / 'manifest.json')


def run_attempt(archive, baseline, output_root, run_id, try_number, offset='+08:00'):
    target = attempt_directory(output_root, run_id, try_number)
    archive_hash, baseline_hash = verify_inputs(archive, baseline, offset)
    for number in range(try_number, 0, -1):
        candidate = attempt_directory(output_root, run_id, number)
        manifest = candidate / 'manifest.json'
        if manifest.is_file():
            try:
                report = json.loads(manifest.read_text(encoding='utf-8'))
            except (ValueError, UnicodeError):
                raise ValueError('Existing attempt has an invalid manifest') from None
            if report.get('status') == 'SUCCESS':
                return verify_result(candidate, run_id, archive_hash, baseline_hash, offset)
    # The ETL runner refuses existing targets and preserves every failed attempt.
    from src.spark.synthea_full import run
    run(archive, target, run_id, offset, baseline)
    return verify_result(target, run_id, archive_hash, baseline_hash, offset)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', required=True)
    parser.add_argument('--baseline', required=True)
    parser.add_argument('--output-root', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--try-number', required=True, type=int)
    parser.add_argument('--birth-date-offset', default='+08:00')
    args = parser.parse_args()
    result = run_attempt(args.archive, args.baseline, args.output_root, args.run_id,
                         args.try_number, args.birth_date_offset)
    print('WORKFLOW_RESULT=' + json.dumps(result))

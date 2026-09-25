from pathlib import Path, PurePosixPath
import argparse, json, sys
root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root))
from src.consumer_workflow import inventory
from src.spark_full_workflow import verify_result

parser = argparse.ArgumentParser(description='Verify archived Stage M receipts and retained artifacts.')
parser.add_argument('--result', type=Path)
args = parser.parse_args()
if not __debug__:
    raise RuntimeError('Run without Python optimization; evidence checks use assertions')

evidence = root / 'docs/stage-m/full-consumers-p10000-r6'
read = lambda p: json.loads(p.read_text(encoding='utf-8'))
from src.stream_io import file_hash as hash_file
r = read(evidence / 'acceptance.json')
assert r['status'] == 'SUCCESS' and r['population'] == 10000
assert r['spark_driver_memory'] == '2g'
assert r['spark_submit_args'] == '--driver-memory 2g pyspark-shell'
assert r['dagbag'] == 'PASSED' and r['dag_test']['state'] == 'success'
assert r['scheduler_run']['state'] == 'success'
assert {t['task_id']: (t['state'], t['try_number']) for t in r['scheduler_run']['tasks']} == {
    'verify_inputs': ('success', 1), 'full_spark_etl': ('success', 1),
    'export_fhir': ('success', 2), 'load_database': ('success', 2)}
assert r['file_publication_unchanged'] and r['formal_database_unchanged']
assert r['database_before']['formal_database_run_id'] == r['database_after']['formal_database_run_id'] == 'stage-c-verified'
assert r['file_publication_before']['run_id'] == 'stage-c-verified'
assert r['database_after']['api_requests'] == 132 and all(r['database_after']['api_checks'].values())
assert {k: v['rows'] for k,v in r['fhir_comparisons'].items()} == dict(Patient=11476, Encounter=677836, ImagingStudy=19435)
assert all(v['missing'] == v['extra'] == 0 for v in r['fhir_comparisons'].values())
for name,digest in r['code_sha256'].items():
    assert hash_file(root / name) == digest, name
for component, receipt in r['consumer_retries'].items():
    relative = PurePosixPath(receipt['manifest']).relative_to(PurePosixPath(r['shared_output']))
    manifest = evidence.joinpath(*relative.parts)
    assert receipt['reused'] and hash_file(manifest) == receipt['manifest_sha256']
    d = read(manifest)
    assert d['status'] == 'SUCCESS' and d['run_id'] == r['scheduler_run']['run_id']
    for name,digest in d['code_sha256'].items():
        assert hash_file(root / 'src' / name) == digest, name
    live_directory = root / 'output' / r['session'] / Path(*relative.parts).parent
    assert inventory(live_directory) == d['artifacts']
    upstream = live_directory.parent.parent / 'spark-full/attempt-0001/manifest.json'
    assert hash_file(upstream) == d['source_manifest_sha256']
    if component == 'fhir':
        exported = read(live_directory / 'fhir/manifest.json')
        assert exported['status'] == 'SUCCESS'
        assert exported['counts'] == dict(Patient=11476, Encounter=677836, ImagingStudy=19435)
        assert all(exported['checks'].values())

    attempts = [read(manifest.parent.parent / f'airflow-try-{i}.json') for i in (1,2)]
    assert all(a['manifest'] == receipt['manifest'] and a['manifest_sha256'] == receipt['manifest_sha256'] for a in attempts)
    assert not (root / 'output' / r['session'] / Path(*relative.parts).parent.parent / 'attempt-0002').exists()
    if component == 'database':
        comparisons = d['verification']['comparisons']
        assert len(comparisons) == 11 and all(v['missing'] == v['extra'] == 0 for v in comparisons.values())
        assert comparisons['source_records']['rows'] == comparisons['row_dispositions']['rows'] == 1725660
production_consumers = set()
for component in ('fhir', 'database'):
    for manifest in (evidence / 'results').glob(f'*/{component}/attempt-*/manifest.json'):
        d = read(manifest)
        if d['run_id'] != r['dag_test']['run_id'] or d['status'] != 'SUCCESS':
            continue
        production_consumers.add(component)
        live_directory = root / 'output' / r['session'] / manifest.relative_to(evidence).parent
        assert inventory(live_directory) == d['artifacts']
        for name, digest in d['code_sha256'].items():
            assert hash_file(root / 'src' / name) == digest
        if component == 'database':
            comparisons = d['verification']['comparisons']
            assert len(comparisons) == 11
            assert all(v['missing'] == v['extra'] == 0 for v in comparisons.values())
        else:
            exported = read(live_directory / 'fhir/manifest.json')
            assert exported['status'] == 'SUCCESS' and all(exported['checks'].values())
            assert exported['counts'] == dict(Patient=11476, Encounter=677836, ImagingStudy=19435)
assert production_consumers == {'fhir', 'database'}
successful_spark_runs = set()
failed_spark_attempts = []
for p in (evidence / 'results').glob('*/spark-full/attempt-*/manifest.json'):
    d=read(p)
    if d['status'] == 'FAILED':
        failed_spark_attempts.append(str(p.relative_to(evidence)))
        continue
    successful_spark_runs.add(d['run_id'])
    assert d['status'] == 'SUCCESS' and len(d['checks']) == 26 and all(d['checks'].values())
    assert len(d['baseline']['checks']) == 14 and all(v == {'missing':0,'extra':0} for v in d['baseline']['checks'].values())
    live_directory = root / 'output' / r['session'] / p.relative_to(evidence).parent
    assert hash_file(live_directory / 'manifest.json') == hash_file(p)
    verify_result(live_directory, d['run_id'], r['archive_sha256'], r['source_manifest_sha256'], '+08:00')
assert successful_spark_runs == {r['dag_test']['run_id'], r['scheduler_run']['run_id']}
result = dict(failed_spark_attempts=failed_spark_attempts, status='PASSED', population=10000,
    fhir_resources=708747, database_comparisons=11, api_requests=132,
    acceptance_sha256=hash_file(evidence / 'acceptance.json'),
    verifier_sha256=hash_file(Path(__file__)), spark_artifact_inventories_verified=2,
    scheduler_status_lock_retries=r.get('scheduler_status_lock_retries', 0))
if args.result:
    with args.result.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2)
print(json.dumps(result))

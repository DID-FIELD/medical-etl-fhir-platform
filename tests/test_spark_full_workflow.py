"""Full Spark retry contract; reuse tests use the recorded real-run manifest."""
import json
from pathlib import Path
import pytest
from src import spark_full_workflow as workflow
from src.stream_io import file_hash


@pytest.fixture
def successful(tmp_path, monkeypatch):
    report = json.loads((Path(__file__).parents[1] / 'docs/stage-i/manifest-p1000-r1.json').read_text())
    report['run_id'] = 'manual:test'
    output = tmp_path / 'results'
    target = workflow.attempt_directory(output, report['run_id'], 1)
    target.mkdir(parents=True)
    # Small files stand in for Parquet bytes; the reuse path must hash, never decode them.
    for name in report['artifacts']:
        path = target / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(name.encode())
        report['artifacts'][name] = file_hash(path)
    (target / 'manifest.json').write_text(json.dumps(report))
    monkeypatch.setattr(workflow, 'verify_inputs', lambda *args: (
        report['archive_sha256'], report['baseline']['source_manifest_sha256']))
    return output, target, report


def retry(output, number=2, run_id='manual:test'):
    return workflow.run_attempt('archive.zip', 'baseline', output, run_id, number)


def test_reuse_does_not_create_new_attempt(successful):
    output, target, report = successful
    before = (target / 'manifest.json').read_bytes()
    assert retry(output) == str((target / 'manifest.json').resolve())
    assert (target / 'manifest.json').read_bytes() == before
    assert not workflow.attempt_directory(output, report['run_id'], 2).exists()


@pytest.mark.parametrize('damage', ['changed', 'missing', 'extra', 'checks', 'baseline', 'identity', 'code', 'inventory', 'dataset'])
def test_corrupt_success_never_recomputed(successful, damage):
    output, target, report = successful
    part = next(name for name in report['artifacts'] if name.startswith('dim_patient.parquet/part-'))
    if damage == 'changed':
        (target / part).write_bytes(b'corrupt')
    elif damage == 'missing':
        (target / part).unlink()
    elif damage == 'extra':
        (target / 'unexpected').write_bytes(b'extra')
    elif damage == 'checks':
        report['checks'].pop(next(iter(report['checks'])))
    elif damage == 'baseline':
        report['baseline']['checks'].pop('warnings')
    elif damage == 'identity':
        report['run_id'] = 'another-run'
    elif damage == 'code':
        report['code_sha256'] = {}
    elif damage == 'inventory':
        report['artifacts'] = {}
    elif damage == 'dataset':
        for name in list(report['artifacts']):
            if name.startswith('dim_patient.parquet/'):
                (target / name).unlink()
                del report['artifacts'][name]
    (target / 'manifest.json').write_text(json.dumps(report))
    with pytest.raises(ValueError):
        retry(output)
    assert not workflow.attempt_directory(output, 'manual:test', 2).exists()


def test_failed_attempt_preserved_new_attempt_then_reused(successful, monkeypatch):
    output, target, report = successful
    failed = {'status': 'FAILED'}
    (target / 'manifest.json').write_text(json.dumps(failed))
    before = (target / 'manifest.json').read_bytes()
    import src.spark.synthea_full as runner
    import shutil
    calls = []
    def create(archive, new, run_id, offset, baseline):
        calls.append(new)
        shutil.copytree(target, new)
        (new / 'manifest.json').write_text(json.dumps(report))
    monkeypatch.setattr(runner, 'run', create)
    result = retry(output)
    assert retry(output, 3) == result
    assert len(calls) == 1
    assert (target / 'manifest.json').read_bytes() == before


def test_failed_current_is_not_overwritten(successful):
    output, target, _ = successful
    (target / 'manifest.json').write_text('{"status":"FAILED"}')
    with pytest.raises(FileExistsError):
        retry(output, 1)
    assert (target / 'manifest.json').read_text() == '{"status":"FAILED"}'


def test_identity_and_try_number(tmp_path):
    one = workflow.attempt_directory(tmp_path, '../../one', 1)
    two = workflow.attempt_directory(tmp_path, '../../two', 1)
    assert one != two and one.resolve().is_relative_to(tmp_path.resolve())
    for invalid in (0, -1, True, '1'):
        with pytest.raises(ValueError):
            workflow.attempt_directory(tmp_path, 'run', invalid)


def test_source_mismatch_rejected(tmp_path):
    archive = tmp_path / 'archive.zip'
    archive.write_bytes(b'input')
    baseline = tmp_path / 'baseline'
    baseline.mkdir()
    report = dict(status='SUCCESS', run_id='base', artifacts={}, archive_sha256=file_hash(archive), birth_date_offset='+08:00')
    (baseline / 'manifest.json').write_text(json.dumps(report))
    assert workflow.verify_inputs(archive, baseline, '+08:00')[0] == file_hash(archive)
    with pytest.raises(ValueError):
        workflow.verify_inputs(archive, baseline, '+00:00')
    archive.write_bytes(b'changed')
    with pytest.raises(ValueError):
        workflow.verify_inputs(archive, baseline, '+08:00')

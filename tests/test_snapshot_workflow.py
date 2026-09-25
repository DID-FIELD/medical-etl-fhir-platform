import json
import pytest
from src.snapshot_workflow import verify_snapshot, run_component
from src.synthea_pipeline import run
from test_synthea_pipeline import fixture_sources, archive


def test_retry_verifies_artifacts_and_preserves_publication(tmp_path):
    root = tmp_path / 'warehouse'
    run(archive(tmp_path, fixture_sources()), root, 'source')
    snapshot = root / 'runs/source'
    pointer = (root / 'current.json').read_bytes()
    assert verify_snapshot(snapshot) == 'source'
    output = tmp_path / 'fhir'
    first = run_component(snapshot, output, 'fhir')
    before = (output / 'manifest.json').read_bytes()
    assert run_component(snapshot, output, 'fhir') == first
    assert (output / 'manifest.json').read_bytes() == before
    assert (root / 'current.json').read_bytes() == pointer
    (output / 'Patient.ndjson').write_text('')
    with pytest.raises(ValueError, match='hash mismatch'):
        run_component(snapshot, output, 'fhir')


def test_failed_result_is_not_replaced(tmp_path):
    root = tmp_path / 'warehouse'
    run(archive(tmp_path, fixture_sources()), root, 'source')
    output = tmp_path / 'fhir'
    output.mkdir()
    manifest = output / 'manifest.json'
    manifest.write_text(json.dumps({'status': 'FAILED'}))
    before = manifest.read_bytes()
    with pytest.raises(ValueError, match='Existing result'):
        run_component(root / 'runs/source', output, 'fhir')
    assert manifest.read_bytes() == before


@pytest.fixture
def snapshot(tmp_path):
    root = tmp_path / 'warehouse'
    run(archive(tmp_path, fixture_sources()), root, 'source')
    return root / 'runs/source'


def test_new_attempt_preserves_failed_and_reuses_success(snapshot, tmp_path):
    from src.snapshot_workflow import attempt_directory, run_attempt
    output = tmp_path / 'attempts'
    failed = attempt_directory(output, 'fhir', 'manual:one', 1)
    failed.mkdir(parents=True)
    (failed / 'manifest.json').write_text(json.dumps({'status': 'FAILED'}))
    before = (failed / 'manifest.json').read_bytes()
    result = run_attempt(snapshot, output, 'fhir', 'manual:one', 2)
    assert result == str(attempt_directory(output, 'fhir', 'manual:one', 2) / 'manifest.json')
    manifest = __import__('pathlib').Path(result)
    success = manifest.read_bytes()
    assert run_attempt(snapshot, output, 'fhir', 'manual:one', 3) == result
    assert manifest.read_bytes() == success
    assert (failed / 'manifest.json').read_bytes() == before
    assert not attempt_directory(output, 'fhir', 'manual:one', 3).exists()


def test_retry_never_hides_corrupted_success(snapshot, tmp_path):
    from pathlib import Path
    from src.snapshot_workflow import attempt_directory, run_attempt
    output = tmp_path / 'attempts'
    manifest = Path(run_attempt(snapshot, output, 'fhir', 'manual:one', 1))
    (manifest.parent / 'Patient.ndjson').write_text('')
    with pytest.raises(ValueError, match='hash mismatch'):
        run_attempt(snapshot, output, 'fhir', 'manual:one', 2)
    assert not attempt_directory(output, 'fhir', 'manual:one', 2).exists()


def test_run_identity_is_isolated_and_path_safe(snapshot, tmp_path):
    from src.snapshot_workflow import attempt_directory, run_attempt
    output = tmp_path / 'attempts'
    one = run_attempt(snapshot, output, 'fhir', '../../one:1', 1)
    two = run_attempt(snapshot, output, 'fhir', '../../two:1', 1)
    assert one != two
    assert attempt_directory(output, 'fhir', '../../one:1', 1).resolve().is_relative_to(output.resolve())
    with pytest.raises(ValueError, match='Invalid task identity'):
        attempt_directory(output, '../bad', 'one', 1)
    with pytest.raises(ValueError, match='Invalid task identity'):
        attempt_directory(output, 'fhir', 'one', 0)


def test_separate_worker_python_runs_real_export_and_retry(snapshot, tmp_path, monkeypatch):
    import sys
    from pathlib import Path
    from src.airflow_tasks import execute_component
    monkeypatch.setenv('SYNTHEA_WORKER_PYTHON', sys.executable)
    monkeypatch.setenv('SYNTHEA_SNAPSHOT', str(snapshot))
    monkeypatch.setenv('SYNTHEA_VALIDATION_OUTPUT', str(tmp_path / 'worker'))
    first = execute_component('fhir', 'manual:worker', 1)
    before = Path(first).read_bytes()
    assert execute_component('fhir', 'manual:worker', 2) == first
    assert Path(first).read_bytes() == before


def test_empty_or_incomplete_spark_checks_are_rejected(snapshot, tmp_path):
    from src.stream_io import file_hash
    output = tmp_path / 'spark'
    output.mkdir()
    report = dict(status='SUCCESS', source_manifest_sha256=file_hash(snapshot / 'manifest.json'),
                  checks={'dws_patient_imaging_summary':dict(missing=0, extra=0, expected_count=2, actual_count=2)})
    (output / 'manifest.json').write_text(json.dumps(report))
    with pytest.raises(ValueError, match='comparison failed'):
        run_component(snapshot, output, 'spark')

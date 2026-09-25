"""Acceptance must reject mixed-scale or changed reference evidence before running DAGs."""
import json
import pytest
from scripts.airflow.full_consumers_acceptance import parse_args, verify_reference
from src.stream_io import file_hash


def reference_fixture(tmp_path):
    snapshot = tmp_path / 'snapshot'
    reference = tmp_path / 'fhir'
    snapshot.mkdir()
    reference.mkdir()
    (snapshot / 'manifest.json').write_text(json.dumps({'run_id': 'baseline'}))
    artifacts = {}
    for kind in ('Patient', 'Encounter', 'ImagingStudy'):
        path = reference / (kind + '.ndjson')
        path.write_text('{}\n')
        artifacts[path.name] = file_hash(path)
    manifest = dict(status='SUCCESS', source_run_id='baseline',
                    source_manifest_sha256=file_hash(snapshot / 'manifest.json'),
                    counts=dict(Patient=11476, Encounter=677836, ImagingStudy=19435),
                    artifacts=artifacts, checks={'source_hashes': True})
    (reference / 'manifest.json').write_text(json.dumps(manifest))
    return snapshot, reference, manifest


def test_reference_binding_and_mutation(tmp_path):
    snapshot, reference, manifest = reference_fixture(tmp_path)
    receipt = verify_reference(reference, snapshot, 10000)
    assert sum(receipt['counts'].values()) == 708747
    with pytest.raises(ValueError, match='baseline/population'):
        verify_reference(reference, snapshot, 1000)
    (reference / 'Patient.ndjson').write_text('{"changed":true}\n')
    with pytest.raises(ValueError, match='artifact hash'):
        verify_reference(reference, snapshot, 10000)


@pytest.mark.parametrize('change', ['run_id', 'source_hash', 'status', 'checks', 'counts'])
def test_reference_rejects_invalid_evidence(tmp_path, change):
    snapshot, reference, manifest = reference_fixture(tmp_path)
    if change == 'run_id':
        manifest['source_run_id'] = 'another-run'
    elif change == 'source_hash':
        (snapshot / 'manifest.json').write_text(json.dumps({'run_id': 'baseline', 'changed': True}))
    elif change == 'status':
        manifest['status'] = 'FAILED'
    elif change == 'checks':
        manifest['checks']['source_hashes'] = False
    else:
        manifest['counts']['Patient'] -= 1
    (reference / 'manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='baseline/population'):
        verify_reference(reference, snapshot, 10000)


def test_explicit_configuration_and_timeout():
    argv = ['--session', 'test-run', '--population', '10000', '--archive', 'source.zip',
            '--baseline', 'baseline', '--fhir-reference', 'fhir']
    assert parse_args(argv).scheduler_timeout == 7200
    for suffix in (['--scheduler-timeout', '0'], ['--population', '100'], ['--session', '../bad']):
        with pytest.raises(SystemExit):
            parse_args(argv + suffix)
    with pytest.raises(SystemExit):
        parse_args(['--session', 'test-run'])


def test_explicit_spark_heap_configuration():
    argv = ['--session', 'test-run', '--population', '10000', '--archive', 'source.zip',
            '--baseline', 'baseline', '--fhir-reference', 'fhir']
    assert parse_args(argv).spark_driver_memory == '2g'
    assert parse_args(argv + ['--spark-driver-memory', '1536m']).spark_driver_memory == '1536m'
    for invalid in ['0g', '-1g', '2', '2.5g', '2g --conf spark.master=local[8]', '']:
        with pytest.raises(SystemExit):
            parse_args(argv + ['--spark-driver-memory', invalid])


def test_scheduler_poll_recovers_real_sqlite_lock(tmp_path):
    import sqlite3
    OperationalError = pytest.importorskip("sqlalchemy.exc").OperationalError
    from scripts.airflow.full_consumers_acceptance import poll_scheduler_snapshot
    from types import SimpleNamespace
    path = tmp_path / 'metadata.db'
    writer = sqlite3.connect(path)
    writer.execute('CREATE TABLE state(value TEXT)')
    writer.execute("INSERT INTO state VALUES ('running')")
    writer.commit()
    writer.execute('BEGIN EXCLUSIVE')
    locks = []
    def read():
        reader = sqlite3.connect(path, timeout=0)
        try:
            return reader.execute('SELECT value FROM state').fetchone()[0]
        except sqlite3.OperationalError as exc:
            raise OperationalError('SELECT', {}, exc) from exc
        finally:
            reader.close()
    try:
        result = poll_scheduler_snapshot(read, SimpleNamespace(poll=lambda: None), 10,
            lambda: locks.append(True), monotonic=lambda: 0, sleep=lambda _: writer.rollback())
        assert result == 'running'
        assert locks == [True]
    finally:
        writer.close()


@pytest.mark.parametrize('scenario', ['deadline', 'exited', 'other_error', 'persistent_lock'])
def test_scheduler_poll_does_not_hide_failures(scenario):
    import sqlite3
    OperationalError = pytest.importorskip("sqlalchemy.exc").OperationalError
    from scripts.airflow.full_consumers_acceptance import poll_scheduler_snapshot
    from types import SimpleNamespace
    now = [0]
    def read():
        error = sqlite3.OperationalError('locked' if scenario == 'persistent_lock' else 'disk I/O error')
        error.sqlite_errorcode = sqlite3.SQLITE_BUSY if scenario == 'persistent_lock' else sqlite3.SQLITE_IOERR
        raise OperationalError('SELECT', {}, error)
    def sleep(seconds):
        now[0] += seconds
    with pytest.raises(OperationalError if scenario == 'other_error' else RuntimeError):
        poll_scheduler_snapshot(read, SimpleNamespace(poll=lambda: 1 if scenario == 'exited' else None),
            0 if scenario == 'deadline' else 3, lambda: None, monotonic=lambda: now[0], sleep=sleep)
    assert now[0] <= 3

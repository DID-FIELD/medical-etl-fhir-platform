"""Retry, corrupt-success and committed-before-receipt recovery contracts."""
import json
from pathlib import Path
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from src import consumer_workflow as workflow
from src.stream_io import file_hash
from test_snapshot_reader import snapshots
from test_streaming_database import isolated


@pytest.fixture
def source(snapshots):
    _, root = snapshots
    path=root/'warnings.parquet/part-00000.parquet'
    path.parent.mkdir()
    pq.write_table(pa.table({'warning':pa.array([],type=pa.string())}),path)
    manifest=root/'manifest.json';report=json.loads(manifest.read_text())
    report['artifacts'][path.relative_to(root).as_posix()]=file_hash(path)
    manifest.write_text(json.dumps(report))
    return manifest


def test_fhir_reuse_and_corruption(source,tmp_path):
    root=tmp_path/'consumers'
    result=Path(workflow.run_attempt(source,root,'spark','fhir',1))
    before=result.read_bytes()
    assert Path(workflow.run_attempt(source,root,'spark','fhir',2))==result
    assert result.read_bytes()==before
    assert not workflow.attempt_directory(root,'spark','fhir',2).exists()
    (result.parent/'fhir/Patient.ndjson').write_text('')
    with pytest.raises(ValueError,match='hash mismatch'):
        workflow.run_attempt(source,root,'spark','fhir',3)


def test_upstream_run_binding_and_identity(source,tmp_path):
    with pytest.raises(ValueError,match='DAG run'):
        workflow.run_attempt(source,tmp_path/'out','other','fhir',1)
    for number in [0,-1,True]:
        with pytest.raises(ValueError):
            workflow.attempt_directory(tmp_path,'spark','fhir',number)
    assert workflow.database_schema('../../a') != workflow.database_schema('../../b')
    assert workflow.database_schema('spark').startswith('synthea_airflow_')


def test_failed_attempt_preserved(source,tmp_path,monkeypatch):
    from src.fhir import streaming_export
    original=streaming_export.export_streaming
    def fail(*args):raise RuntimeError('injected')
    monkeypatch.setattr(streaming_export,'export_streaming',fail)
    root=tmp_path/'out'
    with pytest.raises(RuntimeError):workflow.run_attempt(source,root,'spark','fhir',1)
    receipt=workflow.attempt_directory(root,'spark','fhir',1)/'manifest.json'
    before=receipt.read_bytes()
    assert json.loads(before)['status']=='FAILED'
    monkeypatch.setattr(streaming_export,'export_streaming',original)
    workflow.run_attempt(source,root,'spark','fhir',2)
    assert receipt.read_bytes()==before


def test_database_success_reuse_and_same_count_tamper(source,tmp_path,isolated,monkeypatch):
    from src.database import synthea_store as store
    from psycopg2 import sql
    monkeypatch.setattr(workflow,'database_schema',lambda run_id:isolated)
    root=tmp_path/'out'
    first=Path(workflow.run_attempt(source,root,'spark','database',1))
    assert Path(workflow.run_attempt(source,root,'spark','database',2))==first
    conn=store.get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("UPDATE {} SET gender='altered' WHERE run_id=%s").format(store.qualified('dim_patient')),('spark',))
    finally:conn.close()
    with pytest.raises(ValueError,match='content mismatch'):
        workflow.run_attempt(source,root,'spark','database',3)
    assert not workflow.attempt_directory(root,'spark','database',3).exists()


def test_commit_then_worker_failure_recovers_without_reload(source,tmp_path,isolated,monkeypatch):
    from src.database import synthea_store as store
    monkeypatch.setattr(workflow,'database_schema',lambda run_id:isolated)
    original=store.load_run
    def committed_then_failed(*args,**kwargs):
        original(*args,**kwargs)
        raise RuntimeError('after commit before receipt')
    monkeypatch.setattr(store,'load_run',committed_then_failed)
    root=tmp_path/'out'
    with pytest.raises(RuntimeError):workflow.run_attempt(source,root,'spark','database',1)
    monkeypatch.setattr(store,'load_run',original)
    result=Path(workflow.run_attempt(source,root,'spark','database',2))
    report=json.loads(result.read_text())
    assert report['load']['status']=='ALREADY_LOADED'
    assert len(report['verification']['comparisons'])==11
    assert json.loads((workflow.attempt_directory(root,'spark','database',1)/'manifest.json').read_text())['status']=='FAILED'


def test_adapter_rejects_other_run_before_worker(tmp_path,monkeypatch):
    from src.airflow_tasks import execute_consumer
    monkeypatch.setenv('SYNTHEA_FULL_OUTPUT',str(tmp_path/'results'))
    with pytest.raises(ValueError,match='upstream result'):
        execute_consumer('fhir',str(tmp_path/'other/manifest.json'),'run',1)


def test_corrupt_receipt_and_code_change_fail_closed(source,tmp_path,monkeypatch):
    root=tmp_path/'out'
    result=Path(workflow.run_attempt(source,root,'spark','fhir',1))
    original=result.read_bytes()
    report=json.loads(original)
    report['source_manifest_sha256']='wrong'
    result.write_text(json.dumps(report))
    with pytest.raises(ValueError,match='binding mismatch'):
        workflow.run_attempt(source,root,'spark','fhir',2)
    result.write_bytes(original)
    monkeypatch.setattr(workflow,'code_hashes',lambda:{'changed':'code'})
    with pytest.raises(ValueError,match='binding mismatch'):
        workflow.run_attempt(source,root,'spark','fhir',2)
    assert not workflow.attempt_directory(root,'spark','fhir',2).exists()


def test_adapter_receipt_alias_returns_configured_path(tmp_path):
    from src.airflow_tasks import validate_consumer_result_path
    receipt = tmp_path / 'manifest.json'
    receipt.write_text('{}')
    alias = tmp_path / 'mount-alias.json'
    alias.hardlink_to(receipt)
    assert validate_consumer_result_path(alias, {receipt}) == receipt
    other = tmp_path / 'other.json'
    other.write_text('{}')
    for invalid in (other, tmp_path / 'missing.json'):
        with pytest.raises(ValueError, match='Invalid consumer result path'):
            validate_consumer_result_path(invalid, {receipt})

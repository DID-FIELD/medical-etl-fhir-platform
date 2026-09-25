"""Full Spark thousand-patient Airflow acceptance with isolated metadata and injected retry."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--session', required=True)
    args = parser.parse_args()
    if sys.platform != 'linux':
        raise RuntimeError('Linux runtime required; no Windows Airflow emulation')
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,80}', args.session):
        raise ValueError('Invalid session name')
    base = Path('/opt/medical-etl-airflow')
    project = base / 'project'
    snapshot = project / 'output/scale-stream-final/runs/p1000-stream-local-r1'
    archive = project / 'data/generated/scale-v4-p1000/source.zip'
    options = subprocess.check_output(['findmnt', '-n', '-o', 'OPTIONS', '--target', str(snapshot)], text=True)
    if 'ro' not in options.strip().split(','):
        raise RuntimeError('The source snapshot must be on a read-only mount')
    session = base / 'runs' / args.session
    session.mkdir(parents=True, exist_ok=False)
    home = session / 'airflow'
    dags = home / 'dags'
    dags.mkdir(parents=True)
    for source in (project / 'dags/synthea_full_spark.py',
                   project / 'scripts/airflow/full_spark_retry_dag.py'):
        shutil.copy2(source, dags / source.name)
    airflow_bin = base / 'runtime/airflow-venv/bin'
    worker_bin = base / 'runtime/worker-venv/bin'
    os.environ['PATH'] = str(airflow_bin) + ':' + str(worker_bin) + ':' + os.environ.get('PATH', '')
    os.environ.update(AIRFLOW_HOME=str(home), AIRFLOW__CORE__LOAD_EXAMPLES='False',
        AIRFLOW__CORE__EXECUTOR='SequentialExecutor', AIRFLOW__CORE__DAGS_FOLDER=str(dags),
        AIRFLOW__DATABASE__SQL_ALCHEMY_CONN='sqlite:///' + str(home / 'metadata.db'),
        AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION='True',
        AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL='5',
        AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC='2',
        AIRFLOW__CORE__PARALLELISM='1', PYTHONDONTWRITEBYTECODE='1',
        PYTHONPATH=str(project), SYNTHEA_FULL_BASELINE=str(snapshot), SYNTHEA_FULL_ARCHIVE=str(archive), SYNTHEA_BIRTH_DATE_OFFSET='+08:00',
        SYNTHEA_FULL_OUTPUT=str(session / 'results'),
        SYNTHEA_WORKER_PYTHON=str(base / 'runtime/worker-venv/bin/python'),
        PYSPARK_PYTHON=str(base / 'runtime/worker-venv/bin/python'), SPARK_LOCAL_IP='127.0.0.1')
    sys.path.insert(0, str(project))
    os.chdir(session)
    from src.stream_io import file_hash
    from src.snapshot_workflow import verify_snapshot
    from src.spark_full_workflow import attempt_directory, verify_inputs, verify_result
    report = dict(status='RUNNING', session=args.session, source_run_id=verify_snapshot(snapshot),
                  source_manifest_sha256=file_hash(snapshot / 'manifest.json'),
                  source_read_only=True, runtime=sys.version, population=1000,
                  archive_sha256=file_hash(archive))
    verify_inputs(archive, snapshot, '+08:00')
    formal = project / 'output/synthea/current.json'
    publication_hash = file_hash(formal)
    report['file_publication_before'] = json.loads(formal.read_text())
    scheduler = None
    def save():
        (session / 'acceptance.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    def cli(*arguments):
        with (session / 'cli.log').open('a', encoding='utf-8') as log:
            subprocess.run([str(Path(sys.executable).with_name('airflow')), *arguments], check=True,
                           stdout=log, stderr=subprocess.STDOUT, timeout=1800)
    save()
    try:
        cli('db', 'migrate')
        from airflow.models import DagBag, DagRun, TaskInstance
        from airflow.utils.session import create_session
        import airflow
        import pendulum
        report['airflow_version'] = airflow.__version__
        bag = DagBag(dag_folder=str(dags), include_examples=False)
        if bag.import_errors:
            report['import_errors'] = bag.import_errors
            raise RuntimeError('DagBag import errors')
        if set(bag.dags) != {'synthea_full_spark', 'synthea_full_spark_retry_acceptance'}:
            raise RuntimeError('Unexpected DAG inventory')
        dag = bag.get_dag('synthea_full_spark')
        if (set(dag.task_ids) != {'verify_inputs', 'full_spark_etl'}
                or dag.get_task('full_spark_etl').upstream_task_ids != {'verify_inputs'}):
            raise RuntimeError('Unexpected DAG dependencies')
        report['dagbag'] = 'PASSED'
        save()
        direct = dag.test(execution_date=pendulum.now('UTC'))
        if direct.state != 'success':
            raise RuntimeError('dag.test failed')
        report['dag_test'] = dict(state=direct.state, run_id=direct.run_id)
        save()
        cli('dags', 'reserialize', '-S', str(dags))
        cli('dags', 'unpause', 'synthea_full_spark_retry_acceptance')
        retry_id = 'manual__' + args.session
        cli('dags', 'trigger', '-r', retry_id, 'synthea_full_spark_retry_acceptance')
        with (session / 'scheduler.log').open('w', encoding='utf-8') as log:
            scheduler = subprocess.Popen([str(Path(sys.executable).with_name('airflow')), 'scheduler'],
                                         stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            deadline = time.monotonic() + 1800
            while True:
                with create_session() as db:
                    run = db.query(DagRun).filter_by(dag_id='synthea_full_spark_retry_acceptance', run_id=retry_id).one()
                    state = run.state
                    tasks = db.query(TaskInstance).filter_by(dag_id='synthea_full_spark_retry_acceptance', run_id=retry_id).all()
                    report['scheduler_run'] = dict(run_id=retry_id, state=state,
                        tasks=[dict(task_id=t.task_id, state=t.state, try_number=t.try_number) for t in tasks])
                save()
                if state == 'success':
                    break
                if state == 'failed' or scheduler.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError('Scheduler acceptance failed or timed out')
                time.sleep(2)
        directory = attempt_directory(session / 'results', retry_id, 1).parent
        receipts = [json.loads((directory / f'airflow-try-{i}.json').read_text()) for i in (1, 2)]
        if receipts[0]['manifest'] != receipts[1]['manifest'] or receipts[0]['manifest_sha256'] != receipts[1]['manifest_sha256']:
            raise RuntimeError('Successful output was not reused on retry')
        if (directory / 'attempt-0002').exists():
            raise RuntimeError('Retry unexpectedly generated a second full ETL')
        report['retry_reused_success'] = True
        manifest = Path(receipts[1]['manifest'])
        result = json.loads(manifest.read_text())
        verify_result(manifest.parent, retry_id, report['archive_sha256'],
                      report['source_manifest_sha256'], '+08:00')
        report['full_spark_result'] = dict(manifest=str(manifest), counts=result['counts'],
            baseline=result['baseline'], checks=result['checks'])
        verify_snapshot(snapshot)
        if file_hash(formal) != publication_hash:
            raise RuntimeError('Formal file publication changed')
        report.update(status='SUCCESS', file_publication_unchanged=True)
        save()
    except Exception as exc:
        report.update(status='FAILED', error_type=type(exc).__name__)
        save()
        raise
    finally:
        if scheduler is not None and scheduler.poll() is None:
            os.killpg(scheduler.pid, signal.SIGTERM)
            try:
                scheduler.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(scheduler.pid, signal.SIGKILL)
                scheduler.wait(timeout=15)
    print(session / 'acceptance.json')


if __name__ == '__main__':
    main()


"""Full Spark plus FHIR/PostgreSQL acceptance; native WSL and Windows workers."""
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


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--session', required=True)
    parser.add_argument('--population', type=int, choices=(1000, 10000), required=True)
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--fhir-reference', type=Path, required=True)
    parser.add_argument('--scheduler-timeout', type=int, default=7200)
    parser.add_argument('--spark-driver-memory', default='2g',
                        help='Explicit local Spark JVM heap, e.g. 2g or 1536m (not a process RSS limit)')
    args = parser.parse_args(argv)
    if not re.fullmatch(r'[1-9][0-9]*[mg]', args.spark_driver_memory):
        parser.error('--spark-driver-memory must be a positive integer followed by m or g')
    if args.scheduler_timeout <= 0:
        parser.error('--scheduler-timeout must be positive')
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,80}', args.session):
        parser.error('Invalid session name')
    return args


def verify_reference(reference, snapshot, population):
    from src.stream_io import file_hash
    expected = {1000: dict(Patient=1159, Encounter=70229, ImagingStudy=2239),
                10000: dict(Patient=11476, Encounter=677836, ImagingStudy=19435)}[population]
    source = json.loads((snapshot / 'manifest.json').read_text(encoding='utf-8'))
    manifest = json.loads((reference / 'manifest.json').read_text(encoding='utf-8'))
    if (manifest.get('status') != 'SUCCESS'
            or manifest.get('source_run_id') != source['run_id']
            or manifest.get('source_manifest_sha256') != file_hash(snapshot / 'manifest.json')
            or manifest.get('counts') != expected
            or not manifest.get('checks') or not all(manifest['checks'].values())):
        raise ValueError('FHIR reference does not match the selected baseline/population')
    for kind in expected:
        name = kind + '.ndjson'
        if file_hash(reference / name) != manifest['artifacts'].get(name):
            raise ValueError('FHIR reference artifact hash mismatch')
    return dict(path=str(reference), manifest_sha256=file_hash(reference / 'manifest.json'),
                counts=expected, artifacts=manifest['artifacts'])


def poll_scheduler_snapshot(read_snapshot, scheduler, deadline, on_lock, *,
                            monotonic=time.monotonic, sleep=time.sleep):
    """Retry only SQLite lock contention, closing each failed read's session."""
    import sqlite3
    from sqlalchemy.exc import OperationalError
    while True:
        if scheduler.poll() is not None or monotonic() >= deadline:
            raise RuntimeError('Scheduler exited or acceptance timed out during status read')
        try:
            return read_snapshot()
        except OperationalError as exc:
            original = exc.orig
            code = getattr(original, 'sqlite_errorcode', None)
            if (not isinstance(original, sqlite3.OperationalError)
                    or code is None or (code & 255) not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)):
                raise
            on_lock()
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise RuntimeError('Scheduler status remained locked until acceptance timeout') from exc
            sleep(min(2, remaining))


def main():
    args = parse_args()
    if sys.platform != 'linux':
        raise RuntimeError('Linux runtime required; no Windows Airflow emulation')
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,80}', args.session):
        raise ValueError('Invalid session name')
    base = Path('/opt/medical-etl-airflow')
    project = base / 'project'
    snapshot = args.baseline.resolve()
    archive = args.archive.resolve()
    reference = args.fhir_reference.resolve()
    options = subprocess.check_output(['findmnt', '-n', '-o', 'OPTIONS', '--target', str(snapshot)], text=True)
    if 'ro' not in options.strip().split(','):
        raise RuntimeError('The source snapshot must be on a read-only mount')
    sys.path.insert(0, str(project))
    reference_receipt = verify_reference(reference, snapshot, args.population)
    session = base / 'runs' / args.session
    session.mkdir(parents=True, exist_ok=False)
    windows_project = Path('/mnt/f/project/medical-etl-fhir-platform')
    shared = windows_project / 'output' / args.session
    shared.mkdir(parents=True, exist_ok=False)
    home = session / 'airflow'
    dags = home / 'dags'
    dags.mkdir(parents=True)
    for source in (project / 'dags/synthea_full_spark.py',
                   project / 'scripts/airflow/full_consumers_retry_dag.py'):
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
        SYNTHEA_FULL_OUTPUT=str(shared / 'results'),
        SYNTHEA_DATABASE_WORKER_PYTHON=str(windows_project / '.venv/Scripts/python.exe'),
        SYNTHEA_DB_WINDOWS_WORKER='1', SYNTHEA_WINDOWS_PROJECT=str(windows_project),
        SYNTHEA_WORKER_PYTHON=str(base / 'runtime/worker-venv/bin/python'),
        PYSPARK_PYTHON=str(base / 'runtime/worker-venv/bin/python'), SPARK_LOCAL_IP='127.0.0.1',
        PYSPARK_SUBMIT_ARGS=f'--driver-memory {args.spark_driver_memory} pyspark-shell')
    sys.path.insert(0, str(project))
    os.chdir(session)
    from src.stream_io import file_hash
    from src.snapshot_workflow import verify_snapshot
    from src.spark_full_workflow import attempt_directory as spark_attempt, verify_inputs, verify_result
    from src.consumer_workflow import attempt_directory, database_schema
    report = dict(status='RUNNING', session=args.session, source_run_id=verify_snapshot(snapshot),
                  source_manifest_sha256=file_hash(snapshot / 'manifest.json'),
                  source_read_only=True, runtime=sys.version, population=args.population, shared_output=str(shared),
                  fhir_reference=reference_receipt, scheduler_timeout_seconds=args.scheduler_timeout,
                  spark_driver_memory=args.spark_driver_memory,
                  spark_submit_args=os.environ['PYSPARK_SUBMIT_ARGS'],
                  database_worker='Windows project venv via WSL interop; PostgreSQL remains localhost-only',
                  archive_sha256=file_hash(archive))
    verify_inputs(archive, snapshot, '+08:00')
    formal = project / 'output/synthea/current.json'
    publication_hash = file_hash(formal)
    report['file_publication_before'] = json.loads(formal.read_text())
    scheduler = None
    def save():
        (session / 'acceptance.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
        (shared / 'acceptance.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    def cli(*arguments):
        with (session / 'cli.log').open('a', encoding='utf-8') as log:
            subprocess.run([str(Path(sys.executable).with_name('airflow')), *arguments], check=True,
                           stdout=log, stderr=subprocess.STDOUT, timeout=1800)
    def windows_probe(label, manifest=None, run_id=None):
        output = shared / (label + '.json')
        def win(path):
            return subprocess.check_output(['wslpath','-w',str(path)],text=True).strip()
        command = [str(windows_project / '.venv/Scripts/python.exe'), '-m',
                   'scripts.airflow.windows_acceptance_probe','--output',win(output)]
        if manifest is not None:
            command += ['--manifest',win(manifest),'--run-id',run_id]
        env = os.environ.copy()
        env.pop('PYTHONPATH',None)
        with (session / 'windows-probe.log').open('a',encoding='utf-8') as log:
            subprocess.run(command,check=True,cwd=windows_project,env=env,
                           stdout=log,stderr=subprocess.STDOUT,timeout=900)
        return json.loads(output.read_text(encoding='utf-8'))
    save()
    try:
        report['database_before'] = windows_probe('database-before')
        save()
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
        if set(bag.dags) != {'synthea_full_spark', 'synthea_full_consumers_retry_acceptance'}:
            raise RuntimeError('Unexpected DAG inventory')
        dag = bag.get_dag('synthea_full_spark')
        if (set(dag.task_ids) != {'verify_inputs', 'full_spark_etl', 'export_fhir', 'load_database'}
                or dag.get_task('full_spark_etl').upstream_task_ids != {'verify_inputs'}
                or any(dag.get_task(t).upstream_task_ids != {'full_spark_etl'} for t in ['export_fhir','load_database'])):
            raise RuntimeError('Unexpected DAG dependencies')
        report['dagbag'] = 'PASSED'
        save()
        direct = dag.test(execution_date=pendulum.now('UTC'))
        if direct.state != 'success':
            raise RuntimeError('dag.test failed')
        report['dag_test'] = dict(state=direct.state, run_id=direct.run_id)
        save()
        cli('dags', 'reserialize', '-S', str(dags))
        cli('dags', 'unpause', 'synthea_full_consumers_retry_acceptance')
        retry_id = 'manual__' + args.session
        cli('dags', 'trigger', '-r', retry_id, 'synthea_full_consumers_retry_acceptance')
        with (session / 'scheduler.log').open('w', encoding='utf-8') as log:
            scheduler = subprocess.Popen([str(Path(sys.executable).with_name('airflow')), 'scheduler'],
                                         stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            deadline = time.monotonic() + args.scheduler_timeout
            def read_snapshot():
                with create_session() as db:
                    run = db.query(DagRun).filter_by(dag_id='synthea_full_consumers_retry_acceptance', run_id=retry_id).one()
                    tasks = db.query(TaskInstance).filter_by(dag_id='synthea_full_consumers_retry_acceptance', run_id=retry_id).all()
                    return dict(run_id=retry_id, state=run.state,
                        tasks=[dict(task_id=t.task_id, state=t.state, try_number=t.try_number) for t in tasks])
            def record_lock():
                report['scheduler_status_lock_retries'] = report.get('scheduler_status_lock_retries', 0) + 1
                save()
            while True:
                report['scheduler_run'] = poll_scheduler_snapshot(
                    read_snapshot, scheduler, deadline, record_lock)
                state = report['scheduler_run']['state']
                save()
                if state == 'success':
                    break
                if state == 'failed' or scheduler.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError('Scheduler acceptance failed or timed out')
                time.sleep(2)
        report['consumer_retries'] = {}
        for component in ('fhir','database'):
            directory = attempt_directory(shared / 'results', retry_id, component, 1).parent
            receipts = [json.loads((directory / f'airflow-try-{i}.json').read_text()) for i in (1,2)]
            if receipts[0]['manifest'] != receipts[1]['manifest'] or receipts[0]['manifest_sha256'] != receipts[1]['manifest_sha256']:
                raise RuntimeError('Consumer success was not reused')
            if (directory / 'attempt-0002').exists():
                raise RuntimeError('Consumer retry unexpectedly recreated output')
            manifest = Path(receipts[1]['manifest'])
            if file_hash(manifest) != receipts[1]['manifest_sha256']:
                raise RuntimeError('Consumer manifest changed')
            result = json.loads(manifest.read_text())
            if result['status'] != 'SUCCESS' or result['run_id'] != retry_id:
                raise RuntimeError('Invalid consumer receipt')
            report['consumer_retries'][component] = dict(reused=True,manifest=str(manifest),manifest_sha256=file_hash(manifest))
        expected_tries = {'verify_inputs':1,'full_spark_etl':1,'export_fhir':2,'load_database':2}
        if {t['task_id']:t['try_number'] for t in report['scheduler_run']['tasks']} != expected_tries:
            raise RuntimeError('Unexpected task attempts')
        spark_manifest = spark_attempt(shared / 'results', retry_id, 1) / 'manifest.json'
        verify_result(spark_manifest.parent,retry_id,report['archive_sha256'],report['source_manifest_sha256'],'+08:00')
        report['database_after'] = windows_probe('database-after',spark_manifest,retry_id)
        if report['database_after']['formal_database_run_id'] != report['database_before']['formal_database_run_id']:
            raise RuntimeError('Formal database publication changed')
        report['formal_database_unchanged'] = True
        fhir = Path(report['consumer_retries']['fhir']['manifest']).parent / 'fhir'
        import sqlite3
        db = sqlite3.connect(session / 'fhir-comparison.sqlite')
        try:
            db.execute('CREATE TABLE resources(kind TEXT,id TEXT,body TEXT,seen INTEGER DEFAULT 0,PRIMARY KEY(kind,id))')
            comparisons = {}
            for kind in ('Patient','Encounter','ImagingStudy'):
                with (reference / (kind+'.ndjson')).open(encoding='utf-8') as stream:
                    for line in stream:
                        row=json.loads(line)
                        db.execute('INSERT INTO resources(kind,id,body) VALUES (?,?,?)',
                            (kind,row['id'],json.dumps(row,sort_keys=True,separators=(',',':'))))
                count=0
                with (fhir / (kind+'.ndjson')).open(encoding='utf-8') as stream:
                    for line in stream:
                        row=json.loads(line)
                        body=json.dumps(row,sort_keys=True,separators=(',',':'))
                        if db.execute('UPDATE resources SET seen=1 WHERE kind=? AND id=? AND body=? AND seen=0',(kind,row['id'],body)).rowcount!=1:
                            raise RuntimeError('FHIR content mismatch')
                        count+=1
                comparisons[kind] = dict(rows=count,missing=0,extra=0)
                if count != reference_receipt['counts'][kind]:
                    raise RuntimeError('Unexpected FHIR resource count')
            if db.execute('SELECT 1 FROM resources WHERE seen=0 LIMIT 1').fetchone():
                raise RuntimeError('FHIR missing resource')
            db.commit()
            report['fhir_comparisons'] = comparisons
        finally:
            db.close()
        report['schemas'] = [database_schema(direct.run_id),database_schema(retry_id)]
        report['code_sha256'] = {name:file_hash(project/name) for name in [
            'src/consumer_workflow.py','src/airflow_tasks.py','dags/synthea_full_spark.py',
            'scripts/airflow/full_consumers_retry_dag.py','scripts/airflow/full_consumers_acceptance.py']}
        verify_snapshot(snapshot)
        if verify_reference(reference, snapshot, args.population) != reference_receipt:
            raise RuntimeError('FHIR reference changed during acceptance')
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


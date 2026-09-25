"""Small operator adapters; worker dependencies stay out of Airflow's constrained venv."""
import json
import os
from pathlib import Path
import subprocess
from src.snapshot_workflow import attempt_directory


def execute_component(component, run_id, try_number):
    python = Path(os.environ['SYNTHEA_WORKER_PYTHON'])
    if not python.is_file():
        raise ValueError('Worker Python does not exist')
    # Validate identity before launching the worker; never interpolate a shell command.
    attempt_directory(os.environ['SYNTHEA_VALIDATION_OUTPUT'], component, run_id, try_number)
    command = [str(python), '-m', 'src.snapshot_workflow',
               '--snapshot', os.environ['SYNTHEA_SNAPSHOT'],
               '--output-root', os.environ['SYNTHEA_VALIDATION_OUTPUT'],
               '--component', component, '--run-id', run_id, '--try-number', str(try_number)]
    result = subprocess.run(command, check=True, text=True, encoding='utf-8', stdout=subprocess.PIPE,
                            timeout=1800)
    lines = result.stdout.splitlines()
    receipts = [line[len('WORKFLOW_RESULT='):] for line in lines if line.startswith('WORKFLOW_RESULT=')]
    if len(receipts) != 1:
        raise ValueError('Worker did not return exactly one result')
    manifest = Path(json.loads(receipts[0])).resolve()
    root = Path(os.environ['SYNTHEA_VALIDATION_OUTPUT']).resolve()
    if not manifest.is_relative_to(root) or not manifest.is_file():
        raise ValueError('Worker returned an invalid result path')
    return str(manifest)


def execute_full_spark(run_id, try_number):
    from src.spark_full_workflow import attempt_directory as full_attempt_directory
    python = Path(os.environ['SYNTHEA_WORKER_PYTHON'])
    if not python.is_file():
        raise ValueError('Worker Python does not exist')
    root = Path(os.environ['SYNTHEA_FULL_OUTPUT']).resolve()
    full_attempt_directory(root, run_id, try_number)
    command = [str(python), '-m', 'src.spark_full_workflow',
               '--archive', os.environ['SYNTHEA_FULL_ARCHIVE'],
               '--baseline', os.environ['SYNTHEA_FULL_BASELINE'],
               '--output-root', str(root), '--run-id', run_id, '--try-number', str(try_number),
               '--birth-date-offset', os.environ.get('SYNTHEA_BIRTH_DATE_OFFSET', '+08:00')]
    result = subprocess.run(command, check=True, text=True, encoding='utf-8',
                            stdout=subprocess.PIPE, timeout=1800)
    receipts = [line[len('WORKFLOW_RESULT='):] for line in result.stdout.splitlines()
                if line.startswith('WORKFLOW_RESULT=')]
    if len(receipts) != 1:
        raise ValueError('Worker did not return exactly one result')
    manifest = Path(json.loads(receipts[0])).resolve()
    allowed = {full_attempt_directory(root, run_id, n).resolve() / 'manifest.json'
               for n in range(1, try_number + 1)}
    if manifest not in allowed or not manifest.is_file():
        raise ValueError('Worker returned an invalid result path')
    return str(manifest)


def execute_consumer(component, manifest, run_id, try_number):
    from src.consumer_workflow import attempt_directory as consumer_attempt
    from src.spark_full_workflow import verify_inputs, verify_result
    root = Path(os.environ['SYNTHEA_FULL_OUTPUT']).resolve()
    manifest = Path(manifest).resolve()
    expected_parent = root / __import__('hashlib').sha256(run_id.encode()).hexdigest() / 'spark-full'
    if manifest.name != 'manifest.json' or manifest.parent.parent != expected_parent or not manifest.parent.name.startswith('attempt-'):
        raise ValueError('Consumer input is not this run upstream result')
    archive_hash, baseline_hash = verify_inputs(os.environ['SYNTHEA_FULL_ARCHIVE'],
        os.environ['SYNTHEA_FULL_BASELINE'], os.environ.get('SYNTHEA_BIRTH_DATE_OFFSET', '+08:00'))
    verify_result(manifest.parent, run_id, archive_hash, baseline_hash,
                  os.environ.get('SYNTHEA_BIRTH_DATE_OFFSET', '+08:00'))
    consumer_attempt(root,run_id,component,try_number)
    windows = component == 'database' and os.environ.get('SYNTHEA_DB_WINDOWS_WORKER') == '1'
    python = os.environ.get('SYNTHEA_DATABASE_WORKER_PYTHON', os.environ['SYNTHEA_WORKER_PYTHON']) if component == 'database' else os.environ['SYNTHEA_WORKER_PYTHON']
    def translate(path, flag):
        return subprocess.check_output(['wslpath',flag,str(path)],text=True,encoding='utf-8').strip()
    source_arg = translate(manifest,'-w') if windows else str(manifest)
    root_arg = translate(root,'-w') if windows else str(root)
    command = [python,'-m','src.consumer_workflow','--manifest',source_arg,'--output-root',root_arg,
               '--run-id',run_id,'--component',component,'--try-number',str(try_number)]
    environment = os.environ.copy()
    cwd = None
    if windows:
        cwd = os.environ['SYNTHEA_WINDOWS_PROJECT']
        environment.pop('PYTHONPATH',None)
    result = subprocess.run(command,check=True,text=True,encoding='utf-8',stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,timeout=1800,cwd=cwd,env=environment)
    receipts = [line[len('WORKFLOW_RESULT='):] for line in result.stdout.splitlines() if line.startswith('WORKFLOW_RESULT=')]
    if len(receipts) != 1:
        raise ValueError('Consumer did not return exactly one result')
    result_path = json.loads(receipts[0])
    result_path = Path(translate(result_path,'-u') if windows else result_path).resolve()
    allowed = {consumer_attempt(root,run_id,component,n).resolve() / 'manifest.json' for n in range(1,try_number+1)}
    return str(validate_consumer_result_path(result_path, allowed))


def validate_consumer_result_path(result_path, allowed):
    """Resolve bind-mount aliases back to the configured writable attempt path."""
    result_path = Path(result_path)
    if result_path.is_file():
        for candidate in allowed:
            if candidate.is_file() and result_path.samefile(candidate):
                return candidate
    raise ValueError('Invalid consumer result path')

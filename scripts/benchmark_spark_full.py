"""Linux Spark acceptance measurement; no data generation or publication."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def tree_rss(root):
    processes = {}
    for path in Path('/proc').glob('[0-9]*/status'):
        try:
            fields = dict(line.split(':', 1) for line in path.read_text().splitlines() if ':' in line)
            processes[int(path.parent.name)] = (int(fields['PPid']), int(fields.get('VmRSS', '0 kB').split()[0]) * 1024)
        except (OSError, ValueError, KeyError):
            continue
    family = {root}
    while True:
        expanded = family | {pid for pid, (parent, _) in processes.items() if parent in family}
        if expanded == family:
            break
        family = expanded
    return sum(processes.get(pid, (0, 0))[1] for pid in family)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', required=True)
    parser.add_argument('--baseline', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args()
    root = Path(args.output).resolve()
    root.mkdir(parents=True, exist_ok=False)
    command = [sys.executable, '-m', 'src.spark.synthea_full', '--archive', args.archive,
               '--baseline', args.baseline, '--output', str(root / 'snapshot'),
               '--run-id', args.run_id, '--birth-date-offset', '+08:00']
    report = dict(status='RUNNING', command=command,
                  memory_method='Sum of driver and descendants VmRSS from /proc every 250 ms; shared pages may be counted multiple times. Includes Spark JVM and Python workers; no 500 MiB claim.')
    def save():
        (root / 'measurement.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    save()
    start, peak = time.monotonic(), 0
    with (root / 'run.log').open('x', encoding='utf-8') as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            while process.poll() is None:
                peak = max(peak, tree_rss(process.pid))
                if time.monotonic() - start > 3600:
                    raise TimeoutError('Spark run exceeded one hour')
                time.sleep(.25)
            report.update(status='SUCCESS' if process.returncode == 0 else 'FAILED', exit_code=process.returncode)
        except BaseException as exc:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=30)
            report.update(status='FAILED', error_type=type(exc).__name__)
            raise
        finally:
            report.update(elapsed_seconds=time.monotonic() - start, sampled_peak_tree_rss_bytes=peak)
            save()
    if process.returncode:
        raise SystemExit(process.returncode)


if __name__ == '__main__':
    main()

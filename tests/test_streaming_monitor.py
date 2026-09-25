import json
import sys

import pytest

from scripts import benchmark_streaming as runner
from scripts import benchmark_synthea as monitor


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows process-tree monitor')
def test_monitor_stops_workload_at_configured_rss_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(monitor, 'ROOT', tmp_path)
    result = monitor.measured([sys.executable, '-c', 'import time; time.sleep(30)'],
                              tmp_path / 'guard.log', rss_limit_bytes=1)
    assert result['stop_reason'] == 'process_tree_rss_above_1_bytes'
    assert result['exit_code'] != 0
    assert result['sampled_peak_tree_rss_bytes'] > 1


def test_external_termination_marks_running_manifest_failed(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'ROOT', tmp_path)
    monkeypatch.setattr(runner, 'available', lambda: 2*1024**3)
    (tmp_path / 'docs/stage-e').mkdir(parents=True)
    def terminated(command, log, **kwargs):
        assert kwargs['rss_limit_bytes'] == 500*1024**2
        target = tmp_path / 'output/scale-stream-final/runs/interrupted'
        target.mkdir(parents=True)
        (target / 'manifest.json').write_text(json.dumps({'run_id': 'interrupted', 'status': 'RUNNING'}))
        return {'exit_code': 15, 'stop_reason': 'time_limit', 'sampled_peak_tree_rss_bytes': 1234}
    monkeypatch.setattr(runner, 'measured', terminated)
    monkeypatch.setattr(sys, 'argv', ['benchmark', '--population', '1000', '--run-id', 'interrupted'])
    with pytest.raises(SystemExit):
        runner.main()
    result = json.loads((tmp_path / 'docs/stage-e/interrupted.json').read_text())
    assert result['manifest']['status'] == 'FAILED'
    assert result['manifest']['error_type'] == 'ExternalProcessTermination'
    assert not result['passed']
    assert not (tmp_path / 'output/scale-stream-final/current.json').exists()

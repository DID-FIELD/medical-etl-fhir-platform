"""Opt-in real Spark checks, including zero studies and detected aggregation drift."""
import json
import os
import pytest
from src.synthea_pipeline import run
from src.stream_io import file_hash
from src.spark.synthea_compare import compare_snapshot
from test_synthea_pipeline import fixture_sources, archive

pytestmark = pytest.mark.skipif(os.getenv('RUN_SPARK_TESTS') != '1', reason='Requires Spark and JDK 21')

@pytest.mark.parametrize('imaging', [True, False])
def test_spark_equivalence_and_drift(tmp_path, imaging):
    sources = fixture_sources()
    if not imaging:
        sources['imaging_studies'] = []
    root = tmp_path / 'warehouse'
    run(archive(tmp_path, sources), root, 'source')
    snapshot = root / 'runs/source'
    report = compare_snapshot(snapshot, tmp_path / 'good')
    assert report['status'] == 'SUCCESS'
    assert all(v['missing'] == v['extra'] == 0 for v in report['checks'].values())
    import pyarrow.parquet as pq
    import pyarrow as pa
    target = snapshot / 'dws_patient_imaging_summary.parquet'
    table = pq.read_table(target)
    rows = table.to_pylist()
    rows[0]['exam_count'] += 1
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), target)
    manifest_path = snapshot / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    manifest['artifacts'][target.name] = file_hash(target)
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='equivalence failed'):
        compare_snapshot(snapshot, tmp_path / 'bad')
    assert json.loads((tmp_path / 'bad/manifest.json').read_text())['status'] == 'FAILED'

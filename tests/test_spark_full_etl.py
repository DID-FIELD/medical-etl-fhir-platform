"""Real Spark differential checks against the independent reference transform."""
import json
import os
from collections import Counter

import pytest

from src.spark.synthea_ingest import stage_archive
from src.spark.synthea_etl import transform_frames
from src.synthea_pipeline import transform, reconcile
from spark_fixtures import fixture_sources, archive


def canonical(rows):
    return Counter(json.dumps(row, sort_keys=True, ensure_ascii=True) for row in rows)


@pytest.fixture(scope='module')
def spark():
    if os.getenv('RUN_SPARK_TESTS') != '1':
        pytest.skip('Requires Spark and JDK 21')
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.master('local[2]').appName('full-etl-tests')
               .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '2')
               .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
    session.sparkContext.setLogLevel('ERROR')
    yield session
    session.stop()


@pytest.mark.parametrize('case', ['normal', 'duplicate', 'conflict', 'orphan', 'series_conflict', 'empty_imaging', 'timezone', 'multiline', 'invalid', 'parent_conflict', 'reverse', 'blank_key', 'study_metadata', 'series_metadata', 'encounter_conflict'])
def test_full_transform_matches_reference(tmp_path, spark, case):
    sources = fixture_sources()
    offset = '+08:00'
    if case == 'duplicate':
        sources['patients'].append(sources['patients'][0].copy())
        sources['imaging_studies'].append(sources['imaging_studies'][0].copy())
    elif case == 'conflict':
        sources['imaging_studies'] += [sources['imaging_studies'][0].copy(),
            {**sources['imaging_studies'][0], 'SOP_CODE': 'different'}]
    elif case == 'orphan':
        sources['encounters'][0]['PATIENT'] = 'missing'
    elif case == 'series_conflict':
        sources['imaging_studies'].append({**sources['imaging_studies'][0], 'Id': 's2', 'INSTANCE_UID': 'i4'})
    elif case == 'empty_imaging':
        sources['imaging_studies'] = []
    elif case == 'timezone':
        sources['patients'][0]['BIRTHDATE'] = '2024-01-01'
        sources['encounters'][0]['START'] = '2023-12-31T20:00:00Z'
        for row in sources['imaging_studies']:
            row['DATE'] = '2024-01-03T01:00:00Z'
        sources['imaging_studies'][0]['SOP_CODE'] = ''
        sources['imaging_studies'].append(sources['imaging_studies'][1].copy())
    elif case == 'study_metadata':
        sources['imaging_studies'][1]['DATE'] = '2024-01-01T02:00:00Z'
    elif case == 'series_metadata':
        sources['imaging_studies'][1]['BODYSITE_DESCRIPTION'] = 'different'
    elif case == 'encounter_conflict':
        sources['encounters'].append({**sources['encounters'][0], 'CODE': 'different'})
    elif case == 'invalid':
        sources['patients'][0]['BIRTHDATE'] = 'bad'
        sources['encounters'][0]['START'] = 'bad'
        sources['imaging_studies'][0]['DATE'] = 'bad'
    elif case == 'parent_conflict':
        sources['patients'] += [sources['patients'][0].copy(), {**sources['patients'][0], 'GENDER': 'F'}]
    elif case == 'reverse':
        sources['imaging_studies'].append(sources['imaging_studies'][0].copy())
        for rows in sources.values():
            rows.reverse()
    elif case == 'blank_key':
        sources['imaging_studies'][0]['INSTANCE_UID'] = '\t'
    elif case == 'multiline':
        sources['encounters'][0]['DESCRIPTION'] = '检查\n"quoted",中文'
    staged = tmp_path / 'staged'
    inventory = stage_archive(archive(tmp_path, sources), staged)
    expected, ledger, warnings = transform(sources, inventory, offset)
    tables, actual_ledger, actual_warnings, persisted = transform_frames(spark, staged, inventory, offset)
    try:
        actual = {name: [r.asDict(recursive=True) for r in frame.collect()] for name, frame in tables.items()}
        entries = [r.asDict(recursive=True) for r in actual_ledger.collect()]
        for name in tables:
            assert canonical(actual[name]) == canonical(expected[name]), name
        assert canonical(entries) == canonical(ledger)
        assert canonical([r.asDict() for r in actual_warnings.collect()]) == canonical(warnings)
        reconcile(actual, entries, inventory)
    finally:
        for frame in persisted:
            frame.unpersist()


def test_runner_readback_failure_and_no_overwrite(tmp_path, spark):
    from src.spark.synthea_full import run
    source = archive(tmp_path, fixture_sources())
    output = tmp_path / 'run'
    result = run(source, output, 'test-run', '+08:00')
    assert result['status'] == 'SUCCESS'
    assert all(result['checks'].values())
    with pytest.raises(FileExistsError):
        run(source, output, 'test-run', '+08:00')
    sources = fixture_sources()
    sources['patients'] = []
    empty = archive(tmp_path, sources, 'empty.zip')
    failed = tmp_path / 'failed'
    with pytest.raises(ValueError, match='Empty patient'):
        run(empty, failed, 'empty')
    assert json.loads((failed / 'manifest.json').read_text())['status'] == 'FAILED'
    assert not (tmp_path / 'current.json').exists()

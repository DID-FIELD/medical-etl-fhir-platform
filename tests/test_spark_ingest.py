"""CSV framing and schema failures independent of the Spark runtime."""
import csv
import hashlib
import io
import json
import zipfile

import pyarrow.parquet as pq
import pytest

from src.spark.synthea_ingest import stage_archive
from src.synthea_pipeline import REQUIRED
from spark_fixtures import fixture_sources


def make_zip(path, bodies):
    with zipfile.ZipFile(path, 'w') as archive:
        for name, body in bodies.items():
            archive.writestr(name + '.csv', body)
    return path


def bodies():
    result = {}
    for name, rows in fixture_sources().items():
        stream = io.StringIO(newline='')
        writer = csv.DictWriter(stream, fieldnames=REQUIRED[name])
        writer.writeheader()
        writer.writerows(rows)
        result[name] = stream.getvalue()
    return result


def test_lossless_extra_fields_unicode_newlines_and_signatures(tmp_path):
    source = bodies()
    fields = REQUIRED['encounters'] + ['EXTRA']
    row = {**fixture_sources()['encounters'][0], 'DESCRIPTION': 'line1\r\nline2, "中文"', 'EXTRA': ''}
    stream = io.StringIO(newline='')
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    writer.writerows([row, row])
    source['encounters'] = '\ufeff' + stream.getvalue()
    output = tmp_path / 'staged'
    inventory = stage_archive(make_zip(tmp_path / 'source.zip', source), output)
    actual = pq.read_table(output / 'encounters.parquet').to_pylist()
    assert [r['source_row'] for r in actual] == [1, 2]
    assert all(dict(r['raw']) == row for r in actual)
    assert actual[0]['signature'] == hashlib.sha256(json.dumps(row, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    assert inventory['encounters']['sha256'] == hashlib.sha256(source['encounters'].encode()).hexdigest()
    with pytest.raises(FileExistsError):
        stage_archive(tmp_path / 'source.zip', output)


@pytest.mark.parametrize('body, match', [
    ('Id\nx\n', 'missing required'),
    ('Id,BIRTHDATE,DEATHDATE,GENDER,Id\np,1980-01-01,,M,p\n', 'duplicate CSV'),
    ('Id,BIRTHDATE,DEATHDATE,GENDER\np,1980-01-01,\n', 'inconsistent CSV'),
    ('Id,BIRTHDATE,DEATHDATE,GENDER\np,1980-01-01,,M,extra\n', 'inconsistent CSV'),
])
def test_rejects_bad_schema_and_width(tmp_path, body, match):
    source = bodies()
    source['patients'] = body
    with pytest.raises(ValueError, match=match):
        stage_archive(make_zip(tmp_path / 'source.zip', source), tmp_path / 'staged')
    assert (tmp_path / 'staged').is_dir()

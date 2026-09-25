"""Bounded readers for legacy JSON snapshots and full Spark Parquet directories."""
import json
from pathlib import Path
from src.json_stream import records
from src.stream_io import file_hash


class SnapshotReader:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.manifest_bytes = (self.root / 'manifest.json').read_bytes()
        self.manifest = json.loads(self.manifest_bytes)
        if self.manifest.get('status') != 'SUCCESS':
            raise ValueError('Source snapshot is not successful')
        self.spark = self.manifest.get('backend') == 'spark-full-csv-etl'

    def verify(self, names):
        if (self.root / 'manifest.json').read_bytes() != self.manifest_bytes:
            raise ValueError('Source manifest changed during consumption')
        artifacts = self.manifest.get('artifacts', {})
        if self.spark:
            actual = set()
            for path in self.root.rglob('*'):
                if path.is_symlink():
                    raise ValueError('Artifact symlinks are not allowed')
                if path.is_file() and path != self.root / 'manifest.json':
                    actual.add(path.relative_to(self.root).as_posix())
            if not artifacts or set(artifacts) != actual:
                raise ValueError('Spark artifact inventory mismatch')
            for name in names:
                if not self.parts(name):
                    raise ValueError('Missing Parquet dataset: ' + name)
            selected = artifacts
        else:
            selected = {name + '.json': artifacts.get(name + '.json') for name in names}
        for name, expected in selected.items():
            path = (self.root / name).resolve()
            if not path.is_relative_to(self.root) or expected is None or file_hash(path) != expected:
                raise ValueError('Source artifact hash mismatch: ' + name)

    def parts(self, name):
        return sorted(self.root / p for p in self.manifest.get('artifacts', {})
                      if p.startswith(name + '.parquet/part-') and p.endswith('.parquet'))

    def rows(self, name):
        if not self.spark:
            yield from records(self.root / (name + '.json'))
            return
        import pyarrow.parquet as pq
        from src.synthea_pipeline import COLUMNS
        count = 0
        for path in self.parts(name):
            parquet = pq.ParquetFile(path)
            for batch in parquet.iter_batches(batch_size=500):
                for row in batch.to_pylist():
                    if name in COLUMNS:
                        if row.pop('run_id', None) != self.manifest['run_id']:
                            raise ValueError('Parquet row run_id mismatch')
                        if set(row) != set(COLUMNS[name]):
                            raise ValueError('Parquet model columns mismatch')
                    count += 1
                    yield row
        if name in COLUMNS and count != self.manifest['counts'][name]:
            raise ValueError('Parquet model count mismatch')

    def source_rows(self, name):
        for number, row in enumerate(self.rows('ods_' + name), 1):
            if self.spark:
                number, raw = row['source_row'], row['raw']
                if type(number) is not int or number < 1:
                    raise ValueError('Invalid ODS source_row')
                # Arrow maps decode to pairs; do not derive lineage from partition order.
                row = dict(raw)
            yield number, row

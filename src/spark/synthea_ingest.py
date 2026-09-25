"""Lossless bounded CSV adapter; all quality decisions are made by Spark."""
import csv
import hashlib
import io
import json
from pathlib import Path
import zipfile


def stage_archive(archive, output):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from src.synthea_pipeline import REQUIRED

    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    schema = pa.schema([('source_row', pa.int64()), ('signature', pa.string()),
                        ('raw', pa.map_(pa.string(), pa.string()))])
    inventory = {}
    with zipfile.ZipFile(archive) as zipped:
        for name, required in REQUIRED.items():
            matches = [n for n in zipped.namelist() if Path(n).name == name + '.csv']
            if len(matches) != 1:
                raise ValueError('Expected exactly one ' + name + '.csv')
            member = matches[0]
            digest = hashlib.sha256()
            with zipped.open(member) as source:
                for block in iter(lambda: source.read(1024 * 1024), b''):
                    digest.update(block)
            count = 0
            with zipped.open(member) as binary, io.TextIOWrapper(binary, encoding='utf-8-sig', newline='') as stream:
                reader = csv.DictReader(stream, strict=True)
                if not set(required).issubset(reader.fieldnames or []):
                    raise ValueError(name + ': missing required columns')
                if len(reader.fieldnames) != len(set(reader.fieldnames)):
                    raise ValueError(name + ': duplicate CSV columns')
                with pq.ParquetWriter(output / (name + '.parquet'), schema) as writer:
                    batch = []
                    batch_bytes = 0
                    for count, row in enumerate(reader, 1):
                        if None in row or any(value is None for value in row.values()):
                            raise ValueError(name + ': inconsistent CSV row width')
                        payload = json.dumps(row, sort_keys=True, separators=(',', ':')).encode()
                        batch.append(dict(source_row=count, signature=hashlib.sha256(payload).hexdigest(), raw=list(row.items())))
                        batch_bytes += len(payload)
                        if len(batch) >= 500 or batch_bytes >= 4 * 1024 * 1024:
                            writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                            batch.clear()
                            batch_bytes = 0
                    if batch:
                        writer.write_table(pa.Table.from_pylist(batch, schema=schema))
            inventory[name] = dict(file=member, rows=count, bytes=zipped.getinfo(member).file_size,
                                   sha256=digest.hexdigest(), columns=reader.fieldnames)
    return inventory

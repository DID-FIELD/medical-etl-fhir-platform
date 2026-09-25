"""Bounded readers and writers for snapshot artifacts."""
import hashlib
import json
from pathlib import Path


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def write_array(path, rows):
    with Path(path).open('w', encoding='utf-8', newline='\n') as stream:
        stream.write('[')
        first = True
        for row in rows:
            if not first:
                stream.write(',\n')
            stream.write(json.dumps(row, ensure_ascii=False, allow_nan=False))
            first = False
        stream.write(']\n')

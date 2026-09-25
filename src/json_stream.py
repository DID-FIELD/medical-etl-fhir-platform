"""Strict, bounded iteration over JSON arrays of records, without extra dependencies."""
import json
from pathlib import Path


def records(path, *, chunk_chars=65536, max_record_chars=4*1024**2):
    if chunk_chars < 1 or max_record_chars < 1:
        raise ValueError('JSON reader limits must be positive')
    def reject_constant(value):
        raise ValueError('Non-finite JSON number: ' + value)
    decoder = json.JSONDecoder(parse_constant=reject_constant)
    with Path(path).open(encoding='utf-8-sig') as stream:
        buffer = ''; position = 0; eof = False
        def refill():
            nonlocal buffer, position, eof
            buffer = buffer[position:]
            position = 0
            block = stream.read(chunk_chars)
            buffer += block
            eof = not block
        def peek():
            nonlocal position
            while True:
                while position < len(buffer) and buffer[position] in ' \t\r\n':
                    position += 1
                if position < len(buffer):
                    return buffer[position]
                if eof:
                    return ''
                refill()
        if peek() != '[':
            raise ValueError('Expected JSON array')
        position += 1
        if peek() != ']':
            while True:
                if peek() != '{':
                    raise ValueError('Expected JSON object record')
                while True:
                    try:
                        row, end = decoder.raw_decode(buffer, position)
                    except json.JSONDecodeError as exc:
                        if eof:
                            raise ValueError('Incomplete or invalid JSON record') from exc
                        if len(buffer)-position > max_record_chars:
                            raise ValueError('JSON record exceeds size budget') from exc
                        refill()
                    else:
                        if end-position > max_record_chars:
                            raise ValueError('JSON record exceeds size budget')
                        position = end
                        break
                separator = peek()
                if separator not in (',', ']'):
                    raise ValueError('Expected JSON array separator')
                yield row
                if separator == ']':
                    break
                position += 1
        position += 1
        if peek():
            raise ValueError('Unexpected trailing JSON content')


def batches(rows, *, max_rows=1000, max_bytes=2*1024**2):
    """Bound both record count and encoded input payload; one record may exceed max_bytes."""
    if max_rows < 1 or max_bytes < 1:
        raise ValueError('Batch limits must be positive')
    batch = []; size = 0
    for row in rows:
        row_size = len(json.dumps(row, ensure_ascii=False, allow_nan=False).encode('utf-8'))
        if batch and (len(batch) >= max_rows or size+row_size > max_bytes):
            yield batch
            batch = []; size = 0
        batch.append(row); size += row_size
    if batch:
        yield batch

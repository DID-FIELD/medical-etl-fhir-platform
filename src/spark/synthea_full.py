"""Independent Spark ETL runner; never publishes a current pointer."""
import argparse
import json
from pathlib import Path
import time

from src.stream_io import file_hash
from src.spark.synthea_ingest import stage_archive
from src.spark.synthea_etl import transform_frames


def difference(left, right):
    right = right.select(*left.columns)
    return dict(missing=right.exceptAll(left).count(), extra=left.exceptAll(right).count())


def validate(tables, ledger, inventory):
    from pyspark.sql import functions as F
    from src.synthea_pipeline import KEYS
    checks = {}
    counts = {name: frame.count() for name, frame in tables.items()}
    if not counts['dim_patient']:
        raise ValueError('Empty patient table')
    for name, frame in tables.items():
        checks[name + '.unique_key'] = frame.select(*KEYS[name]).distinct().count() == counts[name]
    for source, meta in inventory.items():
        entries = ledger.filter(F.col('source_table') == source)
        stats = entries.agg(F.count('*').alias('n'), F.countDistinct('source_row').alias('unique'),
                            F.min('source_row').alias('first'), F.max('source_row').alias('last')).first()
        checks[source + '.input_balance'] = (stats.n == stats.unique == meta['rows'] and
            (stats.n == 0 or (stats.first == 1 and stats.last == stats.n)))
    p, e, s, series, instances, bridge = [tables[name] for name in [
        'dim_patient', 'dwd_encounter', 'dwd_imaging_study', 'dwd_imaging_series', 'dwd_imaging_instance', 'bridge_study_modality']]
    def fk(name, child, parent, columns):
        checks[name] = child.join(parent.select(*columns).distinct(), columns, 'left_anti').limit(1).count() == 0
    fk('encounter_patient_fk', e, p, ['patient_key'])
    fk('study_patient_fk', s, p, ['patient_key'])
    fk('study_parent_consistency', s, e, ['encounter_key', 'patient_key'])
    fk('series_study_fk', series, s, ['study_key'])
    fk('instance_series_fk', instances, series, ['study_key', 'series_uid'])
    fk('bridge_study_fk', bridge, s, ['study_key'])
    for name in ['dws_patient_imaging_summary', 'ads_patient_imaging_profile']:
        checks[name + '.patient_coverage'] = not any(difference(tables[name].select('patient_key'), p.select('patient_key')).values())
        checks[name + '.exam_balance'] = tables[name].agg(F.sum('exam_count')).first()[0] == counts['dwd_imaging_study']
    checks['daily_bridge_balance'] = (tables['dws_imaging_daily_modality'].agg(F.sum('exam_count')).first()[0] or 0) == counts['bridge_study_modality']
    for name, source in [('dim_patient', 'patients'), ('dwd_encounter', 'encounters'), ('dwd_imaging_instance', 'imaging_studies')]:
        accepted = ledger.filter((F.col('source_table') == source) & (F.col('disposition') == 'accepted')).select('source_sha256', 'source_row')
        checks[name + '.accepted_lineage'] = not any(difference(tables[name].select('source_sha256', 'source_row'), accepted).values())
    if not all(checks.values()):
        raise ValueError('Reconciliation failed: ' + ', '.join(k for k, v in checks.items() if not v))
    return counts, checks


def compare_baseline(spark, output, baseline, inventory, offset, archive_hash):
    from pyspark.sql import functions as F
    from src.synthea_pipeline import COLUMNS
    from src.json_stream import records
    import pyarrow as pa
    import pyarrow.parquet as pq
    baseline = Path(baseline).resolve()
    manifest = json.loads((baseline / 'manifest.json').read_text(encoding='utf-8'))
    if manifest['status'] != 'SUCCESS' or manifest.get('birth_date_offset', '+00:00') != offset:
        raise ValueError('Baseline status or birth date offset mismatch')
    if manifest.get('archive_sha256') != archive_hash:
        raise ValueError('Baseline archive SHA mismatch')
    for name, meta in inventory.items():
        if any(manifest['source'][name][field] != meta[field] for field in ['rows', 'sha256', 'bytes']):
            raise ValueError('Baseline source mismatch: ' + name)
    comparisons = {}
    for name in [*COLUMNS, 'dispositions', 'warnings', *['ods_' + n for n in inventory]]:
        artifact = name + ('.parquet' if name in COLUMNS else '.json')
        if file_hash(baseline / artifact) != manifest['artifacts'][artifact]:
            raise ValueError('Baseline artifact SHA mismatch: ' + artifact)
        actual = spark.read.parquet((output / (name + '.parquet')).as_uri())
        if name in COLUMNS:
            expected = spark.read.parquet((baseline / artifact).as_uri()).select(*COLUMNS[name])
            actual = actual.select(*COLUMNS[name])
        else:
            # Bounded adapter for baseline JSON arrays, including empty arrays.
            # Canonical JSON makes raw maps order-independent and keeps every field.
            target = output / ('baseline_' + name + '.parquet')
            schema = pa.schema([('record', pa.string())])
            with pq.ParquetWriter(target, schema) as writer:
                batch = []
                for source_row, row in enumerate(records(baseline / artifact), 1):
                    if name.startswith('ods_'):
                        row = dict(source_row=source_row, raw=row)
                    batch.append({'record': json.dumps(row, sort_keys=True, separators=(',', ':'))})
                    if len(batch) == 500:
                        writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                        batch.clear()
                if batch:
                    writer.write_table(pa.Table.from_pylist(batch, schema=schema))
            expected = spark.read.parquet(target.as_uri())
            canonical = F.udf(lambda row: json.dumps(row.asDict(recursive=True) if hasattr(row, 'asDict') else row,
                               sort_keys=True, separators=(',', ':')), 'string')
            if name.startswith('ods_'):
                actual = actual.select(canonical(F.struct('source_row', 'raw')).alias('record'))
            else:
                actual = actual.select(canonical(F.struct(*actual.columns)).alias('record'))
        comparisons[name] = difference(actual, expected)
        if any(comparisons[name].values()):
            raise ValueError('Baseline equivalence failed: ' + name + ' ' + str(comparisons[name]))
    return dict(source_manifest_sha256=file_hash(baseline / 'manifest.json'), checks=comparisons)


def run(archive, output, run_id, birth_date_offset='+00:00', baseline=None):
    from pyspark.sql import SparkSession, functions as F
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    report = dict(status='RUNNING', run_id=run_id, backend='spark-full-csv-etl', birth_date_offset=birth_date_offset,
                  scope='Lossless Python CSV ingestion; Spark quality governance, joins and nine model tables',
                  code_sha256={p.name: file_hash(p) for p in Path(__file__).parent.glob('synthea_*.py')})
    def save():
        (output / 'manifest.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    save()
    spark = None
    try:
        report['archive_sha256'] = file_hash(archive)
        report['source'] = inventory = stage_archive(archive, output / 'staged')
        save()
        spark = (SparkSession.builder.master('local[2]').appName('synthea-full-etl')
                 .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '4')
                 .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        report['spark_version'] = spark.version
        tables, ledger, warnings, persisted = transform_frames(spark, output / 'staged', inventory, birth_date_offset)
        frames = {**tables, 'dispositions': ledger, 'warnings': warnings}
        frames.update({'ods_' + name: spark.read.parquet((output / 'staged' / (name + '.parquet')).as_uri()).select('source_row', 'raw') for name in inventory})
        for name, frame in frames.items():
            if name in tables:
                frame = frame.withColumn('run_id', F.lit(run_id)).select('run_id', *frame.columns)
            frame.write.mode('errorifexists').parquet((output / (name + '.parquet')).as_uri())
        for frame in persisted:
            frame.unpersist()
        # All reconciliation is against persisted, re-read artifacts.
        from src.synthea_pipeline import COLUMNS
        tables = {name: spark.read.parquet((output / (name + '.parquet')).as_uri()).select(*columns) for name, columns in COLUMNS.items()}
        ledger = spark.read.parquet((output / 'dispositions.parquet').as_uri())
        report['counts'], report['checks'] = validate(tables, ledger, inventory)
        report['quality'] = {name: {kind: 0 for kind in ['accepted', 'duplicate', 'quarantined']} for name in inventory}
        for row in ledger.groupBy('source_table', 'disposition').count().collect():
            report['quality'][row.source_table][row.disposition] = row['count']
        report['warning_count'] = spark.read.parquet((output / 'warnings.parquet').as_uri()).count()
        report['zero_exam_patients'] = tables['dws_patient_imaging_summary'].filter('exam_count = 0').count()
        save()
        if baseline is not None:
            report['baseline'] = compare_baseline(spark, output, baseline, inventory, birth_date_offset, report['archive_sha256'])
        report['artifacts'] = {p.relative_to(output).as_posix(): file_hash(p) for p in output.rglob('*')
                               if p.is_file() and p.name != 'manifest.json'}
        if any(file_hash(output / name) != digest for name, digest in report['artifacts'].items()):
            raise ValueError('Artifact hash verification failed')
        report.update(status='SUCCESS', elapsed_seconds=time.monotonic() - started)
        save()
        return report
    except BaseException as exc:
        report.update(status='FAILED', error_type=type(exc).__name__, elapsed_seconds=time.monotonic() - started)
        save()
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--birth-date-offset', default='+00:00')
    parser.add_argument('--baseline')
    args = parser.parse_args()
    print(json.dumps(run(args.archive, args.output, args.run_id, args.birth_date_offset, args.baseline), indent=2))

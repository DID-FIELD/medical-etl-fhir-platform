"""Recompute three Synthea DWS/ADS tables in Spark from a verified DIM/DWD snapshot.

This tests aggregation equivalence, not CSV cleaning or end-to-end Spark ETL.
"""
import argparse
import json
from pathlib import Path
from src.stream_io import file_hash

TABLE_NAMES = ('dim_patient', 'dwd_imaging_study', 'bridge_study_modality',
               'dws_patient_imaging_summary', 'dws_imaging_daily_modality',
               'ads_patient_imaging_profile')


def compare_snapshot(snapshot, output):
    from pyspark.sql import SparkSession, functions as F
    snapshot, output = Path(snapshot).resolve(), Path(output).resolve()
    if output.exists():
        raise FileExistsError('Spark comparison already exists')
    source = json.loads((snapshot / 'manifest.json').read_text(encoding='utf-8'))
    if source['status'] != 'SUCCESS':
        raise ValueError('Source snapshot is not successful')
    for name in TABLE_NAMES:
        artifact = name + '.parquet'
        if file_hash(snapshot / artifact) != source['artifacts'][artifact]:
            raise ValueError('Source artifact hash mismatch: ' + artifact)
    output.mkdir(parents=True, exist_ok=False)
    report = dict(status='RUNNING', source_run_id=source['run_id'],
                  source_manifest_sha256=file_hash(snapshot / 'manifest.json'),
                  code_sha256=file_hash(Path(__file__)), scope='DIM/DWD to three DWS/ADS tables')
    def save():
        (output / 'manifest.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    save()
    spark = None
    try:
        spark = (SparkSession.builder.master('local[2]').appName('synthea-equivalence')
                 .config('spark.sql.session.timeZone', 'UTC')
                 .config('spark.sql.shuffle.partitions', '2')
                 .config('spark.ui.enabled', 'false').getOrCreate())
        tables = {name: spark.read.parquet((snapshot / (name + '.parquet')).as_uri()) for name in TABLE_NAMES}
        patients, studies, bridge = (tables[n] for n in TABLE_NAMES[:3])
        counts = studies.groupBy('patient_key').agg(F.count('*').alias('exam_count'), F.max('started_at').alias('latest_exam_at'))
        modalities = studies.join(bridge, 'study_key').groupBy('patient_key').agg(F.countDistinct('modality_code').alias('modality_count'))
        summary = (patients.select('patient_key').join(counts, 'patient_key', 'left')
                   .join(modalities, 'patient_key', 'left').fillna({'exam_count': 0, 'modality_count': 0}))
        daily = (studies.join(bridge, 'study_key').withColumn('stat_date', F.substring('started_at', 1, 10))
                 .groupBy('stat_date', 'modality_code')
                 .agg(F.count('*').alias('exam_count'), F.countDistinct('patient_key').alias('patient_count')))
        profile = patients.select('patient_key', 'gender', 'birth_year').join(summary, 'patient_key')
        results = dict(dws_patient_imaging_summary=summary, dws_imaging_daily_modality=daily,
                       ads_patient_imaging_profile=profile)
        checks = {}
        for name, actual in results.items():
            expected = tables[name]
            actual = actual.withColumn('run_id', F.lit(source['run_id'])).select(*expected.columns)
            missing = expected.exceptAll(actual).count()
            extra = actual.exceptAll(expected).count()
            checks[name] = dict(expected_count=expected.count(), actual_count=actual.count(), missing=missing, extra=extra)
            if missing or extra:
                raise ValueError('Spark equivalence failed: ' + name)
        report.update(status='SUCCESS', spark_version=spark.version, checks=checks)
        save()
        return report
    except Exception as exc:
        report.update(status='FAILED', error_type=type(exc).__name__)
        save()
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    print(json.dumps(compare_snapshot(args.snapshot, args.output), indent=2))

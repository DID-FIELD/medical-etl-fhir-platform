"""Spark-native three-source quality governance and nine-table transformation.

The driver may import model contracts; worker UDFs depend only on synthea_rules.
No source or accepted-parent table is collected into driver memory.
"""
from src.spark import synthea_rules as rules


def transform_frames(spark, staged, inventory, birth_date_offset='+00:00'):
    from datetime import date
    from pathlib import Path
    from pyspark.sql import Window, functions as F, types as T
    from src.synthea_pipeline import COLUMNS

    rules.date_zone(birth_date_offset)
    empty = F.array().cast('array<string>')
    key = F.udf(rules.stable_key, 'string')
    utc = F.udf(rules.utc, 'string')
    optional_utc = F.udf(rules.optional_utc, 'string')
    patient_errors = F.udf(rules.patient_errors, 'array<string>')
    encounter_errors = F.udf(rules.encounter_errors, 'array<string>')
    imaging_validation = F.udf(rules.imaging_validation, T.StructType([
        T.StructField('reasons', T.ArrayType(T.StringType())), T.StructField('warning', T.BooleanType())]))
    offset = F.lit(birth_date_offset)
    persisted = []

    def hold(frame):
        frame = frame.persist()
        frame.count()
        persisted.append(frame)
        return frame

    raw = {name: spark.read.parquet((Path(staged) / (name + '.parquet')).resolve().as_uri())
           .withColumn('source_sha256', F.lit(meta['sha256'])) for name, meta in inventory.items()}

    def resolve(frame, identity, errors):
        frame = frame.withColumn('_business', F.col('raw')[identity])
        conflicts = frame.groupBy('_business').agg(F.countDistinct('signature').alias('_signatures'))
        frame = frame.join(conflicts, '_business').withColumn('_rank', F.row_number().over(
            Window.partitionBy('_business').orderBy('source_row')))
        # Python str.strip includes whitespace beyond Spark trim's ASCII space.
        missing = F.udf(lambda value: not value.strip(), 'boolean')
        frame = frame.withColumn('reasons', F.concat(
            F.when(missing('_business'), F.array(F.lit('missing_business_key'))).otherwise(empty),
            F.when(F.col('_signatures') > 1, F.array(F.lit('conflicting_business_key'))).otherwise(empty), errors))
        frame = frame.withColumn('disposition', F.when(F.size('reasons') > 0, 'quarantined')
                                 .when(F.col('_rank') > 1, 'duplicate').otherwise('accepted'))
        return hold(frame.withColumn('reasons', F.when(F.col('disposition') == 'duplicate',
                    F.array(F.lit('exact_duplicate'))).otherwise(F.col('reasons'))))

    patients = resolve(raw['patients'], 'Id', patient_errors('raw'))
    parent = patients.filter("disposition = 'accepted'").select(
        F.col('raw')['Id'].alias('_patient'), F.col('raw')['BIRTHDATE'].alias('_birth'))
    encounters = resolve(raw['encounters'].withColumn('_patient', F.col('raw')['PATIENT'])
        .join(parent, '_patient', 'left'), 'Id', encounter_errors('raw', '_birth', offset))
    encounter_parent = encounters.filter("disposition = 'accepted'").select(
        F.col('raw')['Id'].alias('_encounter'), F.col('raw')['PATIENT'].alias('_encounter_patient'),
        F.col('raw')['START'].alias('_start'), F.col('raw')['STOP'].alias('_stop'))
    images = (raw['imaging_studies'].withColumn('_patient', F.col('raw')['PATIENT'])
              .withColumn('_encounter', F.col('raw')['ENCOUNTER'])
              .join(parent, '_patient', 'left').join(encounter_parent, '_encounter', 'left')
              .withColumn('_validation', imaging_validation('raw', '_birth', '_encounter_patient', '_start', '_stop', offset)))
    images = resolve(images, 'INSTANCE_UID', F.col('_validation.reasons'))
    warnings = images.filter('_validation.warning').select(F.lit('imaging_outside_encounter').alias('rule'),
                       key(F.lit('instance'), F.col('raw')['INSTANCE_UID']).alias('source_instance_ref'))
    images = images.withColumn('_study', F.col('raw')['Id']).withColumn('_series', F.col('raw')['SERIES_UID'])
    study_conflicts = images.groupBy('_study').agg(F.countDistinct(F.struct(
        *[F.col('raw')[c] for c in ['PATIENT', 'ENCOUNTER', 'DATE']])).alias('_n')).filter('_n > 1').select('_study')
    series_conflicts = images.groupBy('_series').agg(F.countDistinct(F.struct(
        *[F.col('raw')[c] for c in ['Id', 'MODALITY_CODE', 'BODYSITE_CODE', 'BODYSITE_DESCRIPTION']]))
        .alias('_n')).filter('_n > 1').select('_series')
    bad_studies = (study_conflicts.union(images.join(series_conflicts, '_series').select('_study'))
                   .union(images.filter("disposition = 'quarantined'").select('_study')).distinct()
                   .withColumn('_bad', F.lit(True)))
    images = hold(images.join(bad_studies, '_study', 'left')
        .withColumn('disposition', F.when(F.col('_bad'), 'quarantined').otherwise(F.col('disposition')))
        .withColumn('reasons', F.when(F.col('_bad'), F.sort_array(F.array_distinct(F.concat(
            F.col('reasons'), F.array(F.lit('incomplete_or_conflicting_study')))))).otherwise(F.col('reasons'))))
    ledger = None
    for name, frame in [('patients', patients), ('encounters', encounters), ('imaging_studies', images)]:
        entries = frame.select(F.lit(name).alias('source_table'), 'source_row', 'source_sha256', 'disposition', 'reasons')
        ledger = entries if ledger is None else ledger.unionByName(entries)
    p, e, i = [frame.filter("disposition = 'accepted'") for frame in [patients, encounters, images]]
    i = i.withColumn('study_key', key(F.lit('study'), F.col('raw')['Id']))

    def field(name, alias):
        return F.col('raw')[name].alias(alias)

    def keyed(kind, column, alias):
        return key(F.lit(kind), F.col('raw')[column]).alias(alias)

    def first(frame, columns):
        return frame.withColumn('_first', F.row_number().over(Window.partitionBy(*columns).orderBy('source_row'))).filter('_first = 1')

    tables = {}
    year = F.udf(lambda value: date.fromisoformat(value).year, 'long')
    tables['dim_patient'] = p.select(keyed('patient', 'Id', 'patient_key'), field('GENDER', 'gender'),
        year(F.col('raw')['BIRTHDATE']).alias('birth_year'), 'source_sha256', 'source_row')
    tables['dwd_encounter'] = e.select(keyed('encounter', 'Id', 'encounter_key'), keyed('patient', 'PATIENT', 'patient_key'),
        utc(F.col('raw')['START']).alias('start_at'), optional_utc(F.col('raw')['STOP']).alias('stop_at'),
        field('ENCOUNTERCLASS', 'encounter_class'), field('CODE', 'code'), field('DESCRIPTION', 'description'), 'source_sha256', 'source_row')
    tables['dwd_imaging_study'] = hold(first(i, ['study_key']).select('study_key', keyed('patient', 'PATIENT', 'patient_key'),
        keyed('encounter', 'ENCOUNTER', 'encounter_key'), utc(F.col('raw')['DATE']).alias('started_at'), 'source_sha256', 'source_row'))
    tables['dwd_imaging_series'] = first(i, ['study_key', '_series']).select('study_key', field('SERIES_UID', 'series_uid'),
        field('MODALITY_CODE', 'modality_code'), field('BODYSITE_CODE', 'body_site_code'), field('BODYSITE_DESCRIPTION', 'body_site_description'))
    tables['dwd_imaging_instance'] = i.select(field('INSTANCE_UID', 'instance_uid'), 'study_key', field('SERIES_UID', 'series_uid'),
        field('SOP_CODE', 'sop_code'), 'source_sha256', 'source_row')
    bridge = tables['bridge_study_modality'] = hold(i.select('study_key', field('MODALITY_CODE', 'modality_code')).distinct())
    studies = tables['dwd_imaging_study']
    counts = studies.groupBy('patient_key').agg(F.count('*').alias('exam_count'), F.max('started_at').alias('latest_exam_at'))
    modalities = studies.join(bridge, 'study_key').groupBy('patient_key').agg(F.countDistinct('modality_code').alias('modality_count'))
    summary = (tables['dim_patient'].select('patient_key').join(counts, 'patient_key', 'left').join(modalities, 'patient_key', 'left')
               .fillna({'exam_count': 0, 'modality_count': 0}))
    tables['dws_patient_imaging_summary'] = summary
    tables['dws_imaging_daily_modality'] = (studies.join(bridge, 'study_key')
        .withColumn('stat_date', F.substring('started_at', 1, 10)).groupBy('stat_date', 'modality_code')
        .agg(F.count('*').alias('exam_count'), F.countDistinct('patient_key').alias('patient_count')))
    tables['ads_patient_imaging_profile'] = tables['dim_patient'].select('patient_key', 'gender', 'birth_year').join(summary, 'patient_key')
    return {name: frame.select(*COLUMNS[name]) for name, frame in tables.items()}, ledger, warnings, persisted

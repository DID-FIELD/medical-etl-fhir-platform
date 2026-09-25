"""Transactional PostgreSQL publication for Synthea snapshots."""
import argparse
import json
import os
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json, execute_values

from src.synthea_pipeline import COLUMNS, KEYS

ROOT=Path(__file__).resolve().parents[2]
SCHEMA='synthea_v1'


def get_connection():
    dsn=os.getenv('SYNTHEA_DATABASE_URL')
    if dsn:return psycopg2.connect(dsn,connect_timeout=5)
    config_path=Path(os.getenv('SYNTHEA_DB_CONFIG',str(ROOT/'output/local-postgres/connection.json')))
    return psycopg2.connect(**json.loads(config_path.read_text(encoding='utf-8')))


def qualified(name):return sql.Identifier(SCHEMA,name)


def initialize(cur):
    cur.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {}').format(sql.Identifier(SCHEMA)))
    cur.execute(sql.SQL('CREATE TABLE IF NOT EXISTS {} (run_id TEXT PRIMARY KEY, manifest JSONB NOT NULL)').format(qualified('pipeline_runs')))
    cur.execute(sql.SQL('CREATE TABLE IF NOT EXISTS {} (run_id TEXT NOT NULL REFERENCES {}(run_id), source_table TEXT NOT NULL, source_row INTEGER NOT NULL, source_sha256 TEXT NOT NULL, record JSONB NOT NULL, PRIMARY KEY(run_id,source_table,source_row))').format(qualified('source_records'),qualified('pipeline_runs')))
    cur.execute(sql.SQL('CREATE TABLE IF NOT EXISTS {} (run_id TEXT NOT NULL, source_table TEXT NOT NULL, source_row INTEGER NOT NULL, disposition TEXT NOT NULL CHECK(disposition IN (\'accepted\',\'quarantined\',\'duplicate\')), reasons JSONB NOT NULL, PRIMARY KEY(run_id,source_table,source_row), FOREIGN KEY(run_id,source_table,source_row) REFERENCES {}(run_id,source_table,source_row))').format(qualified('row_dispositions'),qualified('source_records')))
    parents={
      'dwd_encounter':[(['patient_key'],'dim_patient',['patient_key'])],
      'dwd_imaging_study':[(['patient_key'],'dim_patient',['patient_key']),(['encounter_key'],'dwd_encounter',['encounter_key'])],
      'dwd_imaging_series':[(['study_key'],'dwd_imaging_study',['study_key'])],
      'dwd_imaging_instance':[(['study_key','series_uid'],'dwd_imaging_series',['study_key','series_uid'])],
      'bridge_study_modality':[(['study_key'],'dwd_imaging_study',['study_key'])],
      'dws_patient_imaging_summary':[(['patient_key'],'dim_patient',['patient_key'])],
      'ads_patient_imaging_profile':[(['patient_key'],'dim_patient',['patient_key'])],
    }
    for name,columns in COLUMNS.items():
        definitions=[sql.SQL('run_id TEXT NOT NULL REFERENCES {}(run_id)').format(qualified('pipeline_runs'))]
        for col in columns:
            kind='INTEGER' if col in {'source_row','birth_year','exam_count','patient_count','modality_count'} else ('TIMESTAMPTZ' if col.endswith('_at') else ('DATE' if col=='stat_date' else 'TEXT'))
            nullable=col in {'stop_at','latest_exam_at'}
            definitions.append(sql.SQL('{} {} {}').format(sql.Identifier(col),sql.SQL(kind),sql.SQL('' if nullable else 'NOT NULL')))
        definitions.append(sql.SQL('PRIMARY KEY ({})').format(sql.SQL(',').join(map(sql.Identifier,['run_id']+KEYS[name]))))
        for local,parent,remote in parents.get(name,[]):
            definitions.append(sql.SQL('FOREIGN KEY ({}) REFERENCES {} ({})').format(
                sql.SQL(',').join(map(sql.Identifier,['run_id']+local)),qualified(parent),sql.SQL(',').join(map(sql.Identifier,['run_id']+remote))))
        cur.execute(sql.SQL('CREATE TABLE IF NOT EXISTS {} ({})').format(qualified(name),sql.SQL(',').join(definitions)))
    cur.execute(sql.SQL('CREATE TABLE IF NOT EXISTS {} (singleton INTEGER PRIMARY KEY CHECK(singleton=1), run_id TEXT NOT NULL REFERENCES {}(run_id))').format(qualified('current_snapshot'),qualified('pipeline_runs')))
    for index, table, columns in [
        ('study_patient_lookup','dwd_imaging_study',['run_id','patient_key','started_at','study_key']),
        ('instance_study_lookup','dwd_imaging_instance',['run_id','study_key','source_row']),
    ]:
        cur.execute(sql.SQL('CREATE INDEX IF NOT EXISTS {} ON {} ({})').format(
            sql.Identifier(index),qualified(table),sql.SQL(',').join(map(sql.Identifier,columns))))



def load_run(run_dir, *, connection_factory=None, batch_rows=1000):
    from src.database.streaming_load import load_run as streaming_load
    import sys
    return streaming_load(run_dir, store=sys.modules[__name__],
                          connection_factory=connection_factory, batch_rows=batch_rows)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-dir',required=True)
    print(json.dumps(load_run(**vars(p.parse_args())),indent=2))

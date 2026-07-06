import psycopg2
from psycopg2.extras import execute_batch

from src.config import DB_CONFIG


def get_db_conn():
    """Create a PostgreSQL connection."""
    return psycopg2.connect(**DB_CONFIG)


def init_tables():
    """Initialize ODS, DIM, DWD, DWS and ADS demo tables."""
    conn = get_db_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ods_emr_raw (
            patient_id VARCHAR(80),
            patient_name VARCHAR(120),
            gender VARCHAR(20),
            birth_date DATE,
            exam_type VARCHAR(80),
            study_date DATE,
            original_patient_id VARCHAR(80),
            birth_year INT
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ods_dicom_metadata (
            id SERIAL PRIMARY KEY,
            patient_id VARCHAR(80),
            patient_name VARCHAR(120),
            study_date DATE,
            modality VARCHAR(20),
            body_part VARCHAR(80),
            institution VARCHAR(120),
            rows INT,
            columns INT
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_patient (
            patient_id VARCHAR(80) PRIMARY KEY,
            patient_name VARCHAR(80),
            gender VARCHAR(20),
            birth_year INT
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dwd_exam_record_detail (
            exam_record_id SERIAL PRIMARY KEY,
            patient_id VARCHAR(80),
            study_date DATE,
            exam_type VARCHAR(80),
            body_part VARCHAR(80)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dws_patient_exam_summary (
            patient_id VARCHAR(80) PRIMARY KEY,
            exam_count INT,
            latest_exam_date DATE,
            exam_type_count INT
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ads_patient_360_view (
            patient_id VARCHAR(80) PRIMARY KEY,
            patient_name VARCHAR(80),
            gender VARCHAR(20),
            birth_year INT,
            exam_count INT,
            latest_exam_date DATE
        );
        """
    )

    conn.commit()
    cur.close()
    conn.close()
    print("Database tables initialized.")


def truncate_table(table_name: str):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;")
    conn.commit()
    cur.close()
    conn.close()


def batch_insert(table_name: str, data_list: list):
    if not data_list:
        print(f"No data to insert into {table_name}.")
        return

    conn = get_db_conn()
    cur = conn.cursor()
    columns = list(data_list[0].keys())
    placeholders = ", ".join([f"%({col})s" for col in columns])
    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    execute_batch(cur, sql, data_list)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {len(data_list)} rows into {table_name}.")

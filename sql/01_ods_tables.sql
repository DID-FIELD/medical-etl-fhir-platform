CREATE TABLE IF NOT EXISTS ods_emr_raw (
    patient_id VARCHAR(80),
    patient_name VARCHAR(120),
    gender VARCHAR(20),
    birth_date DATE,
    exam_type VARCHAR(80),
    study_date DATE,
    batch_date DATE,
    etl_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ods_dicom_metadata (
    id SERIAL PRIMARY KEY,
    patient_id VARCHAR(80),
    patient_name VARCHAR(120),
    study_date DATE,
    modality VARCHAR(20),
    body_part VARCHAR(80),
    institution VARCHAR(120),
    rows INT,
    columns INT,
    batch_date DATE,
    etl_time TIMESTAMP
);

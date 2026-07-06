CREATE TABLE IF NOT EXISTS dwd_exam_record_detail (
    exam_record_id SERIAL PRIMARY KEY,
    patient_id VARCHAR(80),
    study_date DATE,
    exam_type VARCHAR(80),
    body_part VARCHAR(80),
    source_system VARCHAR(40),
    batch_date DATE,
    etl_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dwd_fhir_observation_detail (
    observation_id VARCHAR(120) PRIMARY KEY,
    patient_id VARCHAR(80),
    status VARCHAR(40),
    code_text VARCHAR(120),
    effective_datetime TIMESTAMP,
    batch_date DATE
);

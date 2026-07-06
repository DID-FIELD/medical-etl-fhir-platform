CREATE TABLE IF NOT EXISTS dim_patient (
    patient_id VARCHAR(80) PRIMARY KEY,
    patient_name VARCHAR(80),
    gender VARCHAR(20),
    birth_year INT,
    effective_start_date DATE,
    effective_end_date DATE,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_exam_type (
    exam_type_code VARCHAR(40) PRIMARY KEY,
    exam_type_name VARCHAR(80),
    exam_category VARCHAR(80)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    week_of_year INT
);

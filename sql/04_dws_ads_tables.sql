CREATE TABLE IF NOT EXISTS dws_patient_exam_summary (
    patient_id VARCHAR(80) PRIMARY KEY,
    exam_count INT,
    latest_exam_date DATE,
    exam_type_count INT,
    batch_date DATE
);

CREATE TABLE IF NOT EXISTS dws_exam_type_daily_stats (
    stat_date DATE,
    exam_type VARCHAR(80),
    exam_count INT,
    patient_count INT,
    PRIMARY KEY (stat_date, exam_type)
);

CREATE TABLE IF NOT EXISTS ads_patient_360_view (
    patient_id VARCHAR(80) PRIMARY KEY,
    patient_name VARCHAR(80),
    gender VARCHAR(20),
    birth_year INT,
    exam_count INT,
    latest_exam_date DATE
);

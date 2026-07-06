import os


DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "dbname": os.getenv("POSTGRES_DB", "medical_etl"),
}


DATA_PATH = {
    "dicom_dir": os.getenv("DICOM_DIR", "./data/dicom"),
    "emr_csv": os.getenv("EMR_CSV", "./data/emr/patients.csv"),
}


FHIR_OUTPUT_DIR = os.getenv("FHIR_OUTPUT_DIR", "./output/fhir")


WAREHOUSE_PATH = os.getenv("WAREHOUSE_PATH", "./output/warehouse")
QUALITY_REPORT_DIR = os.getenv("QUALITY_REPORT_DIR", "./output/quality")

# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "已隐藏",
    "dbname": "medical_etl"
}

# 文件路径配置
DATA_PATH = {
    "dicom_dir": "./data/dicom",
    "emr_csv": "./data/emr/patients.csv"
}

# FHIR资源配置
FHIR_OUTPUT_DIR = "./output/fhir"
from datetime import datetime

from src.config import DATA_PATH, WAREHOUSE_PATH
from src.data_access.dicom_reader import extract_dicom_metadata
from src.data_access.emr_loader import load_emr_data
from src.database.db_manager import batch_insert, init_tables, truncate_table
from src.fhir.converter import (
    convert_to_fhir_observation,
    convert_to_fhir_patient,
    save_fhir_json,
)
from src.processing.anonymizer import anonymize_patient_data
from src.processing.cleaner import standardize_fields
from src.quality.report import save_quality_report
from src.quality.rules import run_emr_quality_checks


def run_full_etl():
    """Run the pandas/PostgreSQL demo ETL pipeline."""
    print("=" * 60)
    print("Starting medical ETL pipeline")
    print("=" * 60)

    init_tables()

    dicom_df = extract_dicom_metadata()
    emr_df = load_emr_data()
    print(f"Loaded DICOM metadata: {len(dicom_df)} rows; EMR: {len(emr_df)} rows")

    dicom_clean = standardize_fields(dicom_df, "dicom")
    emr_clean = standardize_fields(emr_df, "emr")

    quality_results = run_emr_quality_checks(emr_clean)
    report_path = save_quality_report(quality_results)
    print(f"Saved data quality report: {report_path}")

    failed_rules = [result for result in quality_results if result.status == "FAIL"]
    if failed_rules:
        for result in failed_rules:
            print(f"Quality warning: {result.rule_name} - {result.message}")

    dicom_anon = anonymize_patient_data(dicom_clean)
    emr_anon = anonymize_patient_data(emr_clean)

    truncate_table("ods_emr_raw")
    batch_insert("ods_emr_raw", emr_clean.to_dict("records"))

    if not dicom_anon.empty:
        truncate_table("ods_dicom_metadata")
        ods_columns = [
            "patient_id",
            "patient_name",
            "study_date",
            "modality",
            "body_part",
            "institution",
            "rows",
            "columns",
        ]
        batch_insert("ods_dicom_metadata", dicom_anon[ods_columns].to_dict("records"))

    truncate_table("dim_patient")
    dim_columns = ["patient_id", "patient_name", "gender", "birth_year"]
    batch_insert("dim_patient", emr_anon[dim_columns].drop_duplicates("patient_id").to_dict("records"))

    truncate_table("dwd_exam_record_detail")
    exam_data = emr_anon[["patient_id", "study_date", "exam_type"]].copy()
    exam_data["body_part"] = "UNKNOWN"
    batch_insert("dwd_exam_record_detail", exam_data.to_dict("records"))

    fhir_patients = [convert_to_fhir_patient(row) for _, row in emr_anon.iterrows()]
    fhir_observations = [
        convert_to_fhir_observation(row) for _, row in exam_data.reset_index().iterrows()
    ]
    save_fhir_json(fhir_patients, "Patient")
    save_fhir_json(fhir_observations, "Observation")

    print("=" * 60)
    print("Medical ETL pipeline completed")
    print("=" * 60)


def step_init_tables():
    init_tables()


def step_data_extract():
    import os

    os.makedirs("output/temp", exist_ok=True)
    extract_dicom_metadata().to_pickle("output/temp/dicom_raw.pkl")
    load_emr_data().to_pickle("output/temp/emr_raw.pkl")


def step_data_clean_and_anonymize():
    import pandas as pd

    dicom_df = pd.read_pickle("output/temp/dicom_raw.pkl")
    emr_df = pd.read_pickle("output/temp/emr_raw.pkl")

    dicom_anon = anonymize_patient_data(standardize_fields(dicom_df, "dicom"))
    emr_clean = standardize_fields(emr_df, "emr")
    quality_results = run_emr_quality_checks(emr_clean)
    save_quality_report(quality_results)
    emr_anon = anonymize_patient_data(emr_clean)

    dicom_anon.to_pickle("output/temp/dicom_anon.pkl")
    emr_anon.to_pickle("output/temp/emr_anon.pkl")


def step_ods_layer_load():
    import pandas as pd

    dicom_anon = pd.read_pickle("output/temp/dicom_anon.pkl")
    emr_anon = pd.read_pickle("output/temp/emr_anon.pkl")

    truncate_table("ods_emr_raw")
    batch_insert("ods_emr_raw", emr_anon.to_dict("records"))

    if not dicom_anon.empty:
        truncate_table("ods_dicom_metadata")
        ods_columns = [
            "patient_id",
            "patient_name",
            "study_date",
            "modality",
            "body_part",
            "institution",
            "rows",
            "columns",
        ]
        batch_insert("ods_dicom_metadata", dicom_anon[ods_columns].to_dict("records"))


def step_dwd_and_fhir_output():
    import pandas as pd

    emr_anon = pd.read_pickle("output/temp/emr_anon.pkl")
    truncate_table("dim_patient")
    batch_insert(
        "dim_patient",
        emr_anon[["patient_id", "patient_name", "gender", "birth_year"]]
        .drop_duplicates("patient_id")
        .to_dict("records"),
    )

    truncate_table("dwd_exam_record_detail")
    exam_data = emr_anon[["patient_id", "study_date", "exam_type"]].copy()
    exam_data["body_part"] = "UNKNOWN"
    batch_insert("dwd_exam_record_detail", exam_data.to_dict("records"))

    save_fhir_json([convert_to_fhir_patient(row) for _, row in emr_anon.iterrows()], "Patient")
    save_fhir_json([convert_to_fhir_observation(row) for _, row in exam_data.iterrows()], "Observation")


def step_spark_warehouse_build(batch_date: str | None = None):
    from src.spark.warehouse_job import run_warehouse_job

    run_warehouse_job(
        emr_csv=DATA_PATH["emr_csv"],
        warehouse_path=WAREHOUSE_PATH,
        batch_date=batch_date or datetime.now().strftime("%Y-%m-%d"),
    )

from src.data_access.dicom_reader import extract_dicom_metadata
from src.data_access.emr_loader import load_emr_data
from src.processing.cleaner import standardize_fields
from src.processing.anonymizer import anonymize_patient_data
from src.fhir.converter import convert_to_fhir_patient, convert_to_fhir_observation, save_fhir_json
from src.database.db_manager import init_tables, batch_insert, truncate_table

def run_full_etl():
    """执行完整的医疗数据ETL流水线"""
    print("=" * 50)
    print("🚀 开始执行医疗数据ETL全流程")
    print("=" * 50)

    # Step 1: 初始化数据库表
    print("\n[Step 1/8] 初始化数据库表结构")
    init_tables()

    # Step 2: 数据接入
    print("\n[Step 2/8] 加载原始数据")
    dicom_df = extract_dicom_metadata()
    emr_df = load_emr_data()
    print(f"加载DICOM数据 {len(dicom_df)} 条，EMR数据 {len(emr_df)} 条")

    # Step 3: 字段标准化清洗
    print("\n[Step 3/8] 数据清洗与字段标准化")
    dicom_clean = standardize_fields(dicom_df, "dicom")
    emr_clean = standardize_fields(emr_df, "emr")

    # Step 4: PHI敏感信息脱敏
    print("\n[Step 4/8] 敏感信息脱敏处理")
    dicom_anon = anonymize_patient_data(dicom_clean)
    emr_anon = anonymize_patient_data(emr_clean)

    # Step 5: 写入ODS原始层
    print("\n[Step 5/8] 写入ODS原始数据层")
    truncate_table("ods_dicom_metadata")
    ods_columns = ["patient_id", "patient_name", "study_date", "modality", "body_part", "institution", "rows", "columns"]
    batch_insert("ods_dicom_metadata", dicom_anon[ods_columns].to_dict("records"))

    # Step 6: FHIR标准化转换
    print("\n[Step 6/8] 转换为FHIR标准格式")
    fhir_patients = [convert_to_fhir_patient(row) for _, row in emr_anon.iterrows()]
    save_fhir_json(fhir_patients, "Patient")

    # Step 7: 写入DWD患者主表
    print("\n[Step 7/8] 写入DWD患者主表")
    truncate_table("dwd_patient")
    dwd_patient_columns = ["patient_id", "patient_name", "gender", "birth_year"]
    batch_insert("dwd_patient", emr_anon[dwd_patient_columns].to_dict("records"))

    # Step 8: 写入DWD检查记录表
    print("\n[Step 8/8] 写入DWD检查记录表")
    truncate_table("dwd_observation")
    obs_data = emr_anon[["patient_id", "study_date", "exam_type"]].copy()
    obs_data["body_part"] = "UNKNOWN"
    batch_insert("dwd_observation", obs_data.to_dict("records"))

    print("\n" + "=" * 50)
    print("🎉 ETL全流程执行完成")
    print("=" * 50)
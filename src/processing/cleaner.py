import pandas as pd

def standardize_fields(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """
    字段标准化：统一命名、日期格式、空值处理
    data_type: "dicom" 或 "emr"
    """
    df = df.copy()

    # 1. 统一日期格式为 YYYY-MM-DD
    if "study_date" in df.columns:
        # 先强制转字符串，再按8位日期格式解析，避免数字被当成时间戳
        df["study_date"] = pd.to_datetime(df["study_date"].astype(str), format="%Y%m%d", errors="coerce").dt.strftime("%Y-%m-%d")
    
    if "birth_date" in df.columns:
        df["birth_date"] = pd.to_datetime(df["birth_date"], format="%Y-%m-%d", errors="coerce").dt.strftime("%Y-%m-%d")

    # 2. 空值填充：非核心字段缺失填默认值
    if data_type == "dicom":
        df["body_part"] = df["body_part"].fillna("UNKNOWN")
        df["modality"] = df["modality"].fillna("UNKNOWN")
        df["institution"] = df["institution"].fillna("UNKNOWN")

    # 3. 统一患者ID字段名
    if "PatientID" in df.columns:
        df = df.rename(columns={"PatientID": "patient_id"})

    return df

# 本地单测
if __name__ == "__main__":
    from src.data_access.dicom_reader import extract_dicom_metadata
    from src.data_access.emr_loader import load_emr_data

    dicom_df = extract_dicom_metadata()
    dicom_clean = standardize_fields(dicom_df, "dicom")
    print("=== DICOM清洗后结果 ===")
    print(dicom_clean[["patient_id", "study_date", "body_part"]])

    emr_df = load_emr_data()
    emr_clean = standardize_fields(emr_df, "emr")
    print("\n=== EMR清洗后结果 ===")
    print(emr_clean[["patient_id", "birth_date", "study_date"]])
    print("✅ 数据清洗完成")
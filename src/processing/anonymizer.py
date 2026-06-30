import pandas as pd

def anonymize_patient_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    PHI敏感信息脱敏：
    1. 姓名替换为匿名ID
    2. 患者ID掩码处理
    3. 出生日期模糊化（只保留年份）
    """
    df = df.copy()

    # 1. 姓名匿名化：用Patient_后缀ID替代
    if "patient_name" in df.columns:
        df["patient_name"] = df["patient_id"].apply(
            lambda x: f"Patient_{str(x)[-4:]}" if str(x).strip() else "Patient_Unknown"
        )

    # 2. 原始ID掩码：保留前后各2位，中间用*替换
    if "patient_id" in df.columns:
        df["original_patient_id"] = df["patient_id"]
        df["patient_id"] = df["patient_id"].apply(
            lambda x: x[:2] + "****" + x[-2:] if len(str(x)) > 6 else str(x)
        )

    # 3. 出生日期模糊化：只保留出生年份
    if "birth_date" in df.columns:
        df["birth_year"] = pd.to_datetime(df["birth_date"]).dt.year
        df = df.drop(columns=["birth_date"])

    return df

# 本地单测
if __name__ == "__main__":
    from src.data_access.emr_loader import load_emr_data
    from src.processing.cleaner import standardize_fields

    emr_df = load_emr_data()
    emr_clean = standardize_fields(emr_df, "emr")
    emr_anon = anonymize_patient_data(emr_clean)

    print("=== 脱敏后患者数据 ===")
    print(emr_anon[["patient_id", "patient_name", "birth_year"]])
    print("✅ 敏感信息脱敏完成")
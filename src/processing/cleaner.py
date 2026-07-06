import pandas as pd


def standardize_fields(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """Standardize common fields for DICOM metadata and EMR records."""
    df = df.copy()

    if "PatientID" in df.columns:
        df = df.rename(columns={"PatientID": "patient_id"})

    if "study_date" in df.columns:
        df["study_date"] = pd.to_datetime(
            df["study_date"].astype(str), format="%Y%m%d", errors="coerce"
        ).dt.strftime("%Y-%m-%d")

    if "birth_date" in df.columns:
        df["birth_date"] = pd.to_datetime(
            df["birth_date"], format="%Y-%m-%d", errors="coerce"
        ).dt.strftime("%Y-%m-%d")

    if "patient_id" in df.columns:
        df["patient_id"] = df["patient_id"].fillna("UNKNOWN").astype(str).str.strip()

    if data_type == "dicom":
        for column in ["body_part", "modality", "institution"]:
            if column in df.columns:
                df[column] = df[column].fillna("UNKNOWN")

    if data_type == "emr":
        for column in ["gender", "exam_type"]:
            if column in df.columns:
                df[column] = df[column].fillna("UNKNOWN")

    return df


if __name__ == "__main__":
    from src.data_access.emr_loader import load_emr_data

    emr_df = load_emr_data()
    print(standardize_fields(emr_df, "emr"))

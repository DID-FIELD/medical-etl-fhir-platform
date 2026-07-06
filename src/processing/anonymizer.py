import hashlib

import pandas as pd


def mask_patient_id(patient_id: str) -> str:
    """Create a deterministic masked patient id for analytics use."""
    raw_value = str(patient_id).strip() or "UNKNOWN"
    digest = hashlib.sha256(raw_value.encode("utf-8")).hexdigest()[:10].upper()
    return f"PID-{digest}"


def anonymize_patient_data(df: pd.DataFrame) -> pd.DataFrame:
    """Remove or generalize PHI fields used by the demo pipeline."""
    df = df.copy()

    if "patient_id" in df.columns:
        df["original_patient_id"] = df["patient_id"]
        df["patient_id"] = df["patient_id"].apply(mask_patient_id)

    if "patient_name" in df.columns:
        df["patient_name"] = df["patient_id"].apply(lambda value: f"Patient_{value[-4:]}")

    if "birth_date" in df.columns:
        birth_dates = pd.to_datetime(df["birth_date"], errors="coerce")
        df["birth_year"] = birth_dates.dt.year
        df = df.drop(columns=["birth_date"])

    return df


if __name__ == "__main__":
    from src.data_access.emr_loader import load_emr_data
    from src.processing.cleaner import standardize_fields

    emr_df = standardize_fields(load_emr_data(), "emr")
    print(anonymize_patient_data(emr_df))

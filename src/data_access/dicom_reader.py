import os

import pandas as pd
import pydicom

from src.config import DATA_PATH


def extract_dicom_metadata(dicom_dir: str | None = None) -> pd.DataFrame:
    """Read DICOM files and return core metadata as a DataFrame."""
    dicom_dir = dicom_dir or DATA_PATH["dicom_dir"]
    columns = [
        "patient_id",
        "patient_name",
        "study_date",
        "modality",
        "body_part",
        "institution",
        "rows",
        "columns",
    ]

    if not os.path.isdir(dicom_dir):
        return pd.DataFrame(columns=columns)

    metadata_list = []
    for root, _, files in os.walk(dicom_dir):
        for filename in files:
            if not filename.lower().endswith(".dcm"):
                continue

            file_path = os.path.join(root, filename)
            try:
                ds = pydicom.dcmread(file_path, stop_before_pixels=True)
                metadata_list.append(
                    {
                        "patient_id": getattr(ds, "PatientID", "UNKNOWN"),
                        "patient_name": str(getattr(ds, "PatientName", "UNKNOWN")),
                        "study_date": getattr(ds, "StudyDate", ""),
                        "modality": getattr(ds, "Modality", "UNKNOWN"),
                        "body_part": getattr(ds, "BodyPartExamined", "UNKNOWN"),
                        "institution": getattr(ds, "InstitutionName", "UNKNOWN"),
                        "rows": getattr(ds, "Rows", 0),
                        "columns": getattr(ds, "Columns", 0),
                    }
                )
            except Exception as exc:
                print(f"Failed to read DICOM file {filename}: {exc}")

    return pd.DataFrame(metadata_list, columns=columns)


if __name__ == "__main__":
    df = extract_dicom_metadata()
    print("=== DICOM metadata ===")
    print(df)
    print(f"Extracted {len(df)} records.")

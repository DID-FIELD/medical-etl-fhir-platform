import pandas as pd

from src.processing.anonymizer import anonymize_patient_data, mask_patient_id
from src.processing.cleaner import standardize_fields


def test_standardize_fields_formats_dates():
    df = pd.DataFrame(
        [
            {
                "patient_id": " P001 ",
                "gender": "M",
                "birth_date": "1990-05-15",
                "study_date": "20240110",
                "exam_type": "Chest CT",
            }
        ]
    )

    result = standardize_fields(df, "emr")

    assert result.loc[0, "patient_id"] == "P001"
    assert result.loc[0, "study_date"] == "2024-01-10"
    assert result.loc[0, "birth_date"] == "1990-05-15"


def test_anonymize_patient_data_masks_id_and_birth_date():
    df = pd.DataFrame(
        [
            {
                "patient_id": "P001",
                "patient_name": "Zhang San",
                "gender": "M",
                "birth_date": "1990-05-15",
            }
        ]
    )

    result = anonymize_patient_data(df)

    assert result.loc[0, "patient_id"] == mask_patient_id("P001")
    assert result.loc[0, "patient_name"].startswith("Patient_")
    assert result.loc[0, "birth_year"] == 1990
    assert "birth_date" not in result.columns

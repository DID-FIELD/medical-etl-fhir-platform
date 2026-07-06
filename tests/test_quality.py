import pandas as pd

from src.quality.rules import run_emr_quality_checks


def test_emr_quality_checks_pass_for_valid_rows():
    df = pd.DataFrame(
        [
            {"patient_id": "P001", "gender": "M", "study_date": "2024-01-10", "exam_type": "CT"},
            {"patient_id": "P002", "gender": "F", "study_date": "2024-01-11", "exam_type": "XRAY"},
        ]
    )

    results = run_emr_quality_checks(df)

    assert all(result.status == "PASS" for result in results)


def test_emr_quality_checks_find_invalid_gender():
    df = pd.DataFrame(
        [
            {"patient_id": "P001", "gender": "X", "study_date": "2024-01-10", "exam_type": "CT"},
        ]
    )

    results = run_emr_quality_checks(df)
    gender_rule = next(result for result in results if result.rule_name == "allowed_values:gender")

    assert gender_rule.status == "FAIL"
    assert gender_rule.failed_count == 1

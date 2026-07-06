from dataclasses import dataclass

import pandas as pd


@dataclass
class QualityCheckResult:
    rule_name: str
    status: str
    failed_count: int
    total_count: int
    message: str

    def as_dict(self) -> dict:
        return {
            "rule_name": self.rule_name,
            "status": self.status,
            "failed_count": self.failed_count,
            "total_count": self.total_count,
            "message": self.message,
        }


def check_not_null(df: pd.DataFrame, columns: list[str]) -> QualityCheckResult:
    total_count = len(df)
    missing = df[columns].isna().any(axis=1) if total_count else pd.Series(dtype=bool)
    failed_count = int(missing.sum()) if total_count else 0
    return QualityCheckResult(
        rule_name=f"not_null:{','.join(columns)}",
        status="PASS" if failed_count == 0 else "FAIL",
        failed_count=failed_count,
        total_count=total_count,
        message=f"{failed_count} records have null values in required columns.",
    )


def check_unique(df: pd.DataFrame, columns: list[str]) -> QualityCheckResult:
    total_count = len(df)
    failed_count = int(df.duplicated(subset=columns, keep=False).sum()) if total_count else 0
    return QualityCheckResult(
        rule_name=f"unique:{','.join(columns)}",
        status="PASS" if failed_count == 0 else "FAIL",
        failed_count=failed_count,
        total_count=total_count,
        message=f"{failed_count} records violate uniqueness.",
    )


def check_allowed_values(
    df: pd.DataFrame, column: str, allowed_values: set[str]
) -> QualityCheckResult:
    total_count = len(df)
    normalized = df[column].fillna("UNKNOWN").astype(str).str.upper()
    failed_count = int((~normalized.isin(allowed_values)).sum()) if total_count else 0
    return QualityCheckResult(
        rule_name=f"allowed_values:{column}",
        status="PASS" if failed_count == 0 else "FAIL",
        failed_count=failed_count,
        total_count=total_count,
        message=f"{failed_count} records contain values outside {sorted(allowed_values)}.",
    )


def check_date_parseable(df: pd.DataFrame, column: str) -> QualityCheckResult:
    total_count = len(df)
    parsed = pd.to_datetime(df[column], errors="coerce") if total_count else pd.Series(dtype="datetime64[ns]")
    failed_count = int(parsed.isna().sum()) if total_count else 0
    return QualityCheckResult(
        rule_name=f"date_parseable:{column}",
        status="PASS" if failed_count == 0 else "FAIL",
        failed_count=failed_count,
        total_count=total_count,
        message=f"{failed_count} records have invalid dates in {column}.",
    )


def run_emr_quality_checks(df: pd.DataFrame) -> list[QualityCheckResult]:
    return [
        check_not_null(df, ["patient_id", "gender", "study_date"]),
        check_unique(df, ["patient_id", "study_date", "exam_type"]),
        check_allowed_values(df, "gender", {"M", "F", "UNKNOWN"}),
        check_date_parseable(df, "study_date"),
    ]


def results_to_dataframe(results: list[QualityCheckResult]) -> pd.DataFrame:
    return pd.DataFrame([result.as_dict() for result in results])

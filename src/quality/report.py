import os
from datetime import datetime

import pandas as pd

from src.config import QUALITY_REPORT_DIR
from src.quality.rules import results_to_dataframe


def save_quality_report(results, report_dir: str | None = None) -> str:
    report_dir = report_dir or QUALITY_REPORT_DIR
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(
        report_dir, f"quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    results_to_dataframe(results).to_csv(report_path, index=False, encoding="utf-8")
    return report_path

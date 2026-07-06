import pandas as pd

from src.config import DATA_PATH


def load_emr_data(csv_path: str | None = None) -> pd.DataFrame:
    """Load EMR records from a CSV file."""
    csv_path = csv_path or DATA_PATH["emr_csv"]
    return pd.read_csv(csv_path)


if __name__ == "__main__":
    df = load_emr_data()
    print("=== EMR load result ===")
    print(df)
    print(f"Loaded {len(df)} records.")

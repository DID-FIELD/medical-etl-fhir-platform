import pandas as pd
from src.config import DATA_PATH

def load_emr_data(csv_path: str = None) -> pd.DataFrame:
    """
    加载电子病历CSV数据，返回DataFrame
    """
    csv_path = csv_path or DATA_PATH["emr_csv"]
    df = pd.read_csv(csv_path)
    return df

# 本地单测
if __name__ == "__main__":
    df = load_emr_data()
    print("=== 电子病历加载结果 ===")
    print(df)
    print(f"✅ 共成功加载 {len(df)} 条记录")
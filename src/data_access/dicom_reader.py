import os
import pydicom
import pandas as pd
from src.config import DATA_PATH

# Set default dicom directory
dicom_dir = DATA_PATH["dicom_dir"]

if not os.path.isdir(dicom_dir):
    raise NotADirectoryError(f"指定路径不是有效文件夹：{dicom_dir}")


def extract_dicom_metadata(dicom_dir: str = None) -> pd.DataFrame:
    """
    批量读取DICOM文件，提取核心元数据，返回结构化DataFrame
    """
    dicom_dir = dicom_dir or DATA_PATH["dicom_dir"]
    metadata_list = []

    # Traverse all subdirectories and files under target directory
    for root, _, files in os.walk(dicom_dir):
        # root: current subdirectory path
        # files: all files inside current subdirectory
        for filename in files:
            if filename.lower().endswith(".dcm"):
                file_path = os.path.join(root, filename)
                try:
                    ds = pydicom.dcmread(file_path)
                    record = {
                        "patient_id": getattr(ds, "PatientID", "UNKNOWN"),
                        "patient_name": str(getattr(ds, "PatientName", "UNKNOWN")),
                        "study_date": getattr(ds, "StudyDate", ""),
                        "modality": getattr(ds, "Modality", "UNKNOWN"),
                        "body_part": getattr(ds, "BodyPartExaminated", "UNKNOWN"),
                        "institution": getattr(ds, "InstitutionName", "UNKNOWN"),
                        "rows": getattr(ds, "Rows", 0),
                        "columns": getattr(ds, "Columns", 0)
                    }
                    metadata_list.append(record)
                except Exception as e:
                    print(f"读取文件 {filename} 失败: {e}")

    return pd.DataFrame(metadata_list)


# Local unit test: execute directly to verify function
if __name__ == "__main__":
    df = extract_dicom_metadata()
    print("=== DICOM元数据提取结果 ===")
    print(df)
    print(f"✅ 共成功提取 {len(df)} 条记录")
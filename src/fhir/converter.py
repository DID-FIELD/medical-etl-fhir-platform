import json
import os
from fhir.resources.patient import Patient
from fhir.resources.observation import Observation
from fhir.resources.codeableconcept import CodeableConcept
from src.config import FHIR_OUTPUT_DIR

def convert_to_fhir_patient(patient_row: dict) -> dict:
    """单条患者数据转换为FHIR Patient资源"""
    patient = Patient(
        id=str(patient_row["patient_id"]),
        gender=patient_row.get("gender", "unknown"),
        active=True
    )
    return patient.dict()

def convert_to_fhir_observation(obs_row: dict) -> dict:
    """单条检查记录转换为FHIR Observation资源"""
    observation = Observation(
        id=str(obs_row.get("obs_id", "obs_001")),
        status="final",
        code=CodeableConcept(text=obs_row.get("exam_type", "imaging study")),
        subject={"reference": f"Patient/{obs_row['patient_id']}"},
        effectiveDateTime=obs_row.get("study_date", "")
    )
    return observation.dict()

def save_fhir_json(data_list: list, resource_type: str):
    """批量保存FHIR资源为JSON文件"""
    os.makedirs(FHIR_OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(FHIR_OUTPUT_DIR, f"{resource_type.lower()}.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data_list, f, ensure_ascii=False, indent=2)
    print(f"✅ FHIR {resource_type} 已保存到 {output_path}")

# 本地单测
if __name__ == "__main__":
    test_patient = {"patient_id": "P001", "gender": "M"}
    fhir_p = convert_to_fhir_patient(test_patient)
    print("=== FHIR Patient 示例 ===")
    print(json.dumps(fhir_p, indent=2, ensure_ascii=False))
    save_fhir_json([fhir_p], "Patient")
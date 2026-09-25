import json
import os
import hashlib

from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.observation import Observation
from fhir.resources.R4B.patient import Patient

from src.config import FHIR_OUTPUT_DIR


def convert_to_fhir_patient(patient_row: dict) -> dict:
    """Convert one patient row to a FHIR R4B Patient resource."""
    gender = str(patient_row.get("gender", "unknown")).lower()
    gender_map = {"m": "male", "male": "male", "f": "female", "female": "female"}
    patient = Patient(
        id=str(patient_row["patient_id"]),
        gender=gender_map.get(gender, "unknown"),
        active=True,
    )
    return patient.model_dump(mode="json", exclude_none=True)


def convert_to_fhir_observation(obs_row: dict) -> dict:
    """Convert one exam row to a FHIR Observation resource."""
    observation = Observation(
        id=str(obs_row.get("obs_id") or "OBS-" + hashlib.sha256(
            json.dumps([str(obs_row.get(k, "")) for k in
                        ("patient_id", "study_date", "exam_type")]).encode()
        ).hexdigest()[:40]),
        status="final",
        code=CodeableConcept(text=obs_row.get("exam_type", "imaging study")),
        subject={"reference": f"Patient/{obs_row['patient_id']}"},
        effectiveDateTime=obs_row.get("study_date"),
    )
    return observation.model_dump(mode="json", exclude_none=True)


def save_fhir_json(data_list: list, resource_type: str):
    """Save FHIR resources to a JSON file."""
    os.makedirs(FHIR_OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(FHIR_OUTPUT_DIR, f"{resource_type.lower()}.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data_list, f, ensure_ascii=False, indent=2)
    print(f"Saved FHIR {resource_type} resources to {output_path}")


if __name__ == "__main__":
    test_patient = {"patient_id": "PID_DEMO", "gender": "M"}
    print(json.dumps(convert_to_fhir_patient(test_patient), indent=2))

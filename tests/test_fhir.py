from src.fhir.converter import convert_to_fhir_patient


def test_convert_to_fhir_patient_maps_gender():
    resource = convert_to_fhir_patient({"patient_id": "PID-001", "gender": "M"})

    assert resource["resourceType"] == "Patient"
    assert resource["id"] == "PID-001"
    assert resource["gender"] == "male"

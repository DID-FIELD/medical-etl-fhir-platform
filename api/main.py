from fastapi import FastAPI, HTTPException

from src.database.db_manager import get_db_conn

app = FastAPI(
    title="Medical ETL and FHIR Data Platform API",
    description="Query patient profiles, exam records and FHIR resources from the layered warehouse.",
    version="2.0.0",
)


@app.get("/api/patient/{patient_id}", summary="Get patient profile")
def get_patient_info(patient_id: str):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT patient_id, patient_name, gender, birth_year
        FROM dim_patient
        WHERE patient_id = %s
        """,
        (patient_id,),
    )
    result = cur.fetchone()
    cur.close()
    conn.close()

    if not result:
        raise HTTPException(status_code=404, detail="Patient not found")

    return {
        "patient_id": result[0],
        "patient_name": result[1],
        "gender": result[2],
        "birth_year": result[3],
    }


@app.get("/api/patient/{patient_id}/observations", summary="Get patient exam records")
def get_patient_observations(patient_id: str):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT exam_record_id, study_date, exam_type, body_part
        FROM dwd_exam_record_detail
        WHERE patient_id = %s
        ORDER BY study_date DESC
        """,
        (patient_id,),
    )
    results = cur.fetchall()
    cur.close()
    conn.close()

    observations = [
        {
            "exam_record_id": row[0],
            "study_date": str(row[1]),
            "exam_type": row[2],
            "body_part": row[3],
        }
        for row in results
    ]

    return {"patient_id": patient_id, "total": len(observations), "observations": observations}


@app.get("/api/ads/patient-360/{patient_id}", summary="Get ADS patient 360 view")
def get_patient_360(patient_id: str):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT patient_id, patient_name, gender, birth_year, exam_count, latest_exam_date
        FROM ads_patient_360_view
        WHERE patient_id = %s
        """,
        (patient_id,),
    )
    result = cur.fetchone()
    cur.close()
    conn.close()

    if not result:
        raise HTTPException(status_code=404, detail="Patient 360 view not found")

    return {
        "patient_id": result[0],
        "patient_name": result[1],
        "gender": result[2],
        "birth_year": result[3],
        "exam_count": result[4],
        "latest_exam_date": str(result[5]),
    }


@app.get("/api/fhir/patient/{patient_id}", summary="Get FHIR Patient resource")
def get_fhir_patient(patient_id: str):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT patient_id, gender FROM dim_patient WHERE patient_id = %s", (patient_id,))
    result = cur.fetchone()
    cur.close()
    conn.close()

    if not result:
        raise HTTPException(status_code=404, detail="Patient not found")

    from src.fhir.converter import convert_to_fhir_patient

    return convert_to_fhir_patient({"patient_id": result[0], "gender": result[1]})

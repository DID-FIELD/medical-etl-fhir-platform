from fastapi import FastAPI, HTTPException
from src.database.db_manager import get_db_conn

app = FastAPI(
    title="医疗多源数据ETL与FHIR标准化平台 API",
    description="支持患者信息、检查记录查询及FHIR标准格式输出",
    version="1.0.0"
)


@app.get("/api/patient/{patient_id}", summary="查询患者基础信息")
def get_patient_info(patient_id: str):
    """根据脱敏后的患者ID查询基础信息"""
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT patient_id, patient_name, gender, birth_year
        FROM dwd_patient
        WHERE patient_id = %s
    """, (patient_id,))
    result = cur.fetchone()
    cur.close()
    conn.close()

    if not result:
        raise HTTPException(status_code=404, detail="患者信息未找到")

    return {
        "patient_id": result[0],
        "patient_name": result[1],
        "gender": result[2],
        "birth_year": result[3]
    }


@app.get("/api/patient/{patient_id}/observations", summary="查询患者检查记录")
def get_patient_observations(patient_id: str):
    """查询指定患者的所有检查记录"""
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT obs_id, study_date, exam_type, body_part
        FROM dwd_observation
        WHERE patient_id = %s
        ORDER BY study_date DESC
    """, (patient_id,))
    results = cur.fetchall()
    cur.close()
    conn.close()

    observations = [
        {
            "obs_id": row[0],
            "study_date": str(row[1]),
            "exam_type": row[2],
            "body_part": row[3]
        }
        for row in results
    ]

    return {
        "patient_id": patient_id,
        "total": len(observations),
        "observations": observations
    }


@app.get("/api/fhir/patient/{patient_id}", summary="获取FHIR格式患者数据")
def get_fhir_patient(patient_id: str):
    """实时生成并返回标准FHIR R4格式的患者资源"""
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT patient_id, gender
        FROM dwd_patient
        WHERE patient_id = %s
    """, (patient_id,))
    result = cur.fetchone()
    cur.close()
    conn.close()

    if not result:
        raise HTTPException(status_code=404, detail="患者信息未找到")

    from fhir.resources.patient import Patient
    patient = Patient(
        id=str(result[0]),
        gender=result[1],
        active=True
    )
    return patient.dict()
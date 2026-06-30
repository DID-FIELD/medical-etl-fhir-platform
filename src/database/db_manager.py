import psycopg2
from psycopg2.extras import execute_batch
from src.config import DB_CONFIG

def get_db_conn():
    """获取PostgreSQL数据库连接"""
    return psycopg2.connect(**DB_CONFIG)

def init_tables():
    """初始化ODS层和DWD层数据表，项目首次运行执行"""
    conn = get_db_conn()
    cur = conn.cursor()

    # ODS层：原始DICOM元数据表
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ods_dicom_metadata (
        id SERIAL PRIMARY KEY,
        patient_id VARCHAR(50),
        patient_name VARCHAR(100),
        study_date DATE,
        modality VARCHAR(20),
        body_part VARCHAR(50),
        institution VARCHAR(100),
        rows INT,
        columns INT
    );
    """)

    # DWD层：患者主表（脱敏后明细层）
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dwd_patient (
        patient_id VARCHAR(50) PRIMARY KEY,
        patient_name VARCHAR(50),
        gender VARCHAR(10),
        birth_year INT
    );
    """)

    # DWD层：检查记录表
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dwd_observation (
        obs_id SERIAL PRIMARY KEY,
        patient_id VARCHAR(50),
        study_date DATE,
        exam_type VARCHAR(50),
        body_part VARCHAR(50)
    );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ 数据表初始化完成")

def truncate_table(table_name: str):
    """清空指定表的所有数据并重置自增ID，用于全量覆盖写入"""
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;")
    conn.commit()
    cur.close()
    conn.close()

def batch_insert(table_name: str, data_list: list):
    """
    批量写入数据
    table_name: 表名
    data_list: 字典列表，key对应表字段
    """
    if not data_list:
        print("⚠️  无数据可写入")
        return

    conn = get_db_conn()
    cur = conn.cursor()

    columns = list(data_list[0].keys())
    placeholders = ", ".join([f"%({col})s" for col in columns])
    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

    execute_batch(cur, sql, data_list)
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ 成功写入 {len(data_list)} 条数据到 {table_name}")

# 本地单测：建表 + 写入测试数据
if __name__ == "__main__":
    init_tables()
    truncate_table("dwd_patient")
    test_data = [{"patient_id": "TEST01", "patient_name": "Patient_Test", "gender": "M", "birth_year": 1990}]
    batch_insert("dwd_patient", test_data)
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


EMR_SCHEMA = StructType(
    [
        StructField("patient_id", StringType(), False),
        StructField("patient_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("exam_type", StringType(), True),
        StructField("study_date", StringType(), True),
    ]
)


def build_spark(app_name: str = "medical-warehouse-etl") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def mask_patient_id(column):
    return F.concat(F.lit("PID-"), F.upper(F.substring(F.sha2(column.cast("string"), 256), 1, 10)))


def run_warehouse_job(emr_csv: str, warehouse_path: str, batch_date: str) -> None:
    spark = build_spark()
    base_path = Path(warehouse_path)

    raw_emr = (
        spark.read.option("header", True)
        .schema(EMR_SCHEMA)
        .csv(emr_csv)
        .withColumn("batch_date", F.lit(batch_date))
        .withColumn("etl_time", F.current_timestamp())
    )

    ods_emr = raw_emr.withColumn(
        "study_date_std", F.to_date("study_date", "yyyyMMdd")
    ).withColumn("birth_date_std", F.to_date("birth_date", "yyyy-MM-dd"))
    ods_emr.write.mode("overwrite").partitionBy("batch_date").parquet(str(base_path / "ods" / "ods_emr_raw"))

    dim_patient = (
        ods_emr.select(
            mask_patient_id(F.col("patient_id")).alias("patient_sk"),
            F.col("gender"),
            F.year("birth_date_std").alias("birth_year"),
            F.col("batch_date"),
            F.col("etl_time"),
        )
        .dropDuplicates(["patient_sk"])
    )
    dim_patient.write.mode("overwrite").parquet(str(base_path / "dim" / "dim_patient"))

    dwd_exam = ods_emr.select(
        mask_patient_id(F.col("patient_id")).alias("patient_sk"),
        F.col("exam_type"),
        F.col("study_date_std").alias("study_date"),
        F.col("batch_date"),
        F.col("etl_time"),
    ).withColumn("exam_record_id", F.sha2(F.concat_ws("|", "patient_sk", "exam_type", F.col("study_date").cast("string")), 256))
    dwd_exam.write.mode("overwrite").partitionBy("batch_date").parquet(str(base_path / "dwd" / "dwd_exam_record_detail"))

    dws_patient_exam_summary = dwd_exam.groupBy("patient_sk").agg(
        F.count("*").alias("exam_count"),
        F.max("study_date").alias("latest_exam_date"),
        F.countDistinct("exam_type").alias("exam_type_count"),
    )
    dws_patient_exam_summary.write.mode("overwrite").parquet(
        str(base_path / "dws" / "dws_patient_exam_summary")
    )

    ads_patient_360 = dim_patient.join(dws_patient_exam_summary, "patient_sk", "left").fillna(
        {"exam_count": 0, "exam_type_count": 0}
    )
    ads_patient_360.write.mode("overwrite").parquet(str(base_path / "ads" / "ads_patient_360_view"))

    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build layered medical data warehouse with PySpark.")
    parser.add_argument("--emr-csv", required=True)
    parser.add_argument("--warehouse-path", required=True)
    parser.add_argument("--batch-date", required=True)
    args = parser.parse_args()
    run_warehouse_job(args.emr_csv, args.warehouse_path, args.batch_date)


if __name__ == "__main__":
    main()

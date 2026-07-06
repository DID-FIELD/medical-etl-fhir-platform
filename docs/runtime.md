# Runtime Notes

## Spark

Use Java 11 or Java 17 for the PySpark warehouse job. Java 25 can start the JVM but fails inside Hadoop file-system initialization with `getSubject is not supported`.

Recommended check:

```bash
java -version
```

Recommended Spark command:

```bash
python -m src.spark.warehouse_job --emr-csv data/emr/patients.csv --warehouse-path output/warehouse --batch-date 2026-07-06
```

## PostgreSQL

The Pandas/PostgreSQL ETL expects a local PostgreSQL database configured through environment variables:

```bash
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=medical_etl
```

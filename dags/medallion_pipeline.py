"""
Medallion Architecture Pipeline — Airflow 3.x DAG
==================================================
Bronze (MinIO Raw) → Silver (Iceberg/Nessie) → Gold (ClickHouse)

Triggered daily at 06:00 UTC.
Each task submits a PySpark script to the Spark cluster.

Airflow 3.x Changes:
  - schedule_interval → schedule (renamed)
  - DAG Versioning is automatic
  - catchup defaults to False
  - Task SDK replaces direct DB access
"""

from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

# ── Default Arguments ─────────────────────────────────────────────
default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Shared Spark Config ───────────────────────────────────────────
SPARK_CONF = {
    "spark.driver.memory": "512m",
    "spark.executor.memory": "768m",
    "spark.executor.cores": "1",
    "spark.sql.adaptive.enabled": "true",
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": "s3a://spark-logs/event-logs",
}

# ── DAG Definition (Airflow 3.x syntax) ──────────────────────────
with DAG(
    dag_id="medallion_pipeline",
    default_args=default_args,
    description="Bronze → Silver (Iceberg) → Gold (ClickHouse) pipeline",
    schedule="0 6 * * *",            # Airflow 3.x: 'schedule' replaces 'schedule_interval'
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spark", "medallion", "iceberg", "clickhouse", "nessie"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ── Bronze Layer: Ingest raw data into MinIO ──────────────────
    bronze_ingest = SparkSubmitOperator(
        task_id="bronze_ingest",
        application="/opt/airflow/scripts/bronze_ingest.py",
        conn_id="spark_default",
        name="bronze_ingest_{{ ds_nodash }}",
        conf=SPARK_CONF,
        execution_timeout=timedelta(minutes=30),
    )

    # ── Silver Layer: Clean & write to Iceberg ────────────────────
    silver_transform = SparkSubmitOperator(
        task_id="silver_transform",
        application="/opt/airflow/scripts/silver_transform.py",
        conn_id="spark_default",
        name="silver_transform_{{ ds_nodash }}",
        conf=SPARK_CONF,
        execution_timeout=timedelta(minutes=45),
    )

    # ── Gold Layer: Aggregate & load into ClickHouse ──────────────
    gold_load = SparkSubmitOperator(
        task_id="gold_load_clickhouse",
        application="/opt/airflow/scripts/gold_load.py",
        conn_id="spark_default",
        name="gold_clickhouse_{{ ds_nodash }}",
        conf=SPARK_CONF,
        execution_timeout=timedelta(minutes=30),
    )

    end = EmptyOperator(task_id="end")

    # ── Pipeline Flow ─────────────────────────────────────────────
    start >> bronze_ingest >> silver_transform >> gold_load >> end

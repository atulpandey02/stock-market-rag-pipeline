"""
Stock Market Batch Pipeline DAG
fetch → consume → spark → snowflake → complete

Schedule: Daily
All dates use UTC {{ ds }} to ensure consistency across pipeline.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_minio_client():
    from minio import Minio
    return Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin',
        secure=False
    )


class MinIODataSensor(BaseSensorOperator):
    """Waits for files in MinIO under a given prefix + date partition."""

    @apply_defaults
    def __init__(self, bucket_name, prefix, min_files=1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.prefix      = prefix
        self.min_files   = min_files

    def poke(self, context):
        try:
            client           = get_minio_client()
            ds               = context['ds']
            year, month, day = ds.split("-")
            path             = (
                f"{self.prefix}/"
                f"year={year}/month={month}/day={day}"
            )
            self.log.info(f"Checking: {path}")
            objects    = list(client.list_objects(self.bucket_name, prefix=path, recursive=True))
            data_files = [o for o in objects if o.size > 0]
            self.log.info(f"Found {len(data_files)} files")
            return len(data_files) >= self.min_files
        except Exception as e:
            self.log.error(f"Sensor error: {e}")
            return False


# ── DAG Definition ────────────────────────────────────────────────────────────

default_args = {
    "owner":            "Atul",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

dag = DAG(
    dag_id           = "stock_market_batch_pipeline",
    default_args     = default_args,
    description      = "Stock Market Batch Pipeline: Alpha Vantage → Kafka → MinIO → Spark → Snowflake",
    schedule_interval= timedelta(days=1),
    start_date       = datetime(2026, 3, 27),
    catchup          = False,
    max_active_runs  = 1,
    tags             = ['batch', 'historical', 'snowflake']
)

# ── Task 1: Fetch Historical Data ─────────────────────────────────────────────
# ✅ No {{ ds }} — producer handles its own date logic internally
fetch_historical_data = BashOperator(
    task_id      = "fetch_historical_data",
    bash_command = "python /opt/airflow/dags/scripts/batch_data_producer.py",
    execution_timeout = timedelta(minutes=30),  # 10 stocks × 5s delay = ~1 min + buffer
    dag          = dag,
)

# ── Task 2: Consume Historical Data ──────────────────────────────────────────
# ✅ No {{ ds }} — consumer exits on 10 empty polls automatically
consume_historical_data = BashOperator(
    task_id      = "consume_historical_data",
    bash_command = "python /opt/airflow/dags/scripts/batch_data_consumer.py",
    execution_timeout = timedelta(minutes=10),
    dag          = dag,
)

# ── Task 3: Wait for Raw Data in MinIO ───────────────────────────────────────
wait_for_raw_data = MinIODataSensor(
    task_id       = "wait_for_raw_data",
    bucket_name   = "stock-market-data",
    prefix        = "raw/historical",
    min_files     = 10,   # 10 stocks minimum
    poke_interval = 15,
    timeout       = 300,
    dag           = dag,
)

# ── Task 4: Spark Processing ──────────────────────────────────────────────────
# ✅ Pass {{ ds }} to Spark so output path matches Snowflake load path
process_data = BashOperator(
    task_id      = "process_data",
    bash_command = """
        docker exec stockmarketdatapipeline-spark-client-1 \
            /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --conf spark.jars.ivy=/tmp/.ivy2 \
            --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
            /opt/spark/jobs/spark_batch_processor.py {{ ds }}
    """,
    execution_timeout = timedelta(minutes=20),
    dag          = dag,
)

# ── Task 5: Load to Snowflake ─────────────────────────────────────────────────
# ✅ Pass {{ ds }} — Snowflake loader reads from processed/historical/year=../month=../day=../
load_to_snowflake = BashOperator(
    task_id      = "load_historical_to_snowflake",
    bash_command = "python /opt/airflow/dags/scripts/load_to_snowflake.py {{ ds }}",
    execution_timeout = timedelta(minutes=15),
    dag          = dag,
)

# ── Task 6: Pipeline Complete ─────────────────────────────────────────────────
process_complete = BashOperator(
    task_id      = "process_complete",
    bash_command = 'echo "=== Batch pipeline for {{ ds }} completed successfully ==="',
    dag          = dag,  # ✅ FIX: was missing dag=dag
)

# ── Dependencies ──────────────────────────────────────────────────────────────
(
    fetch_historical_data
    >> consume_historical_data
    >> wait_for_raw_data       # ✅ NEW: sensor confirms data landed before Spark runs
    >> process_data
    >> load_to_snowflake
    >> process_complete
)
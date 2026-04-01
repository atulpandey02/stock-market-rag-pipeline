"""
Stock Market Streaming Analytics Pipeline
All dates use UTC throughout — matches consumer file paths and Airflow {{ ds }}.
"""

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


def get_minio_client():
    from minio import Minio
    return Minio('minio:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)


class MinIODataSensor(BaseSensorOperator):
    """
    Waits until MinIO has at least min_files in the Airflow execution date partition.
    Uses context['ds'] (UTC) — matches consumer which writes using UTC.
    """

    @apply_defaults
    def __init__(self, bucket_name, prefix, min_files=1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.prefix      = prefix
        self.min_files   = min_files

    def poke(self, context):
        try:
            client = get_minio_client()

            # ✅ Use Airflow execution date (UTC) — matches consumer UTC file paths
            ds             = context['ds']          # e.g. "2026-03-29"
            year, month, day = ds.split("-")

            path = (
                f"{self.prefix}/"
                f"year={year}/month={month}/day={day}"
            )

            self.log.info(f"Checking path: {path} (Airflow ds={ds} UTC)")

            objects    = list(client.list_objects(self.bucket_name, prefix=path, recursive=True))
            data_files = [o for o in objects if o.size > 0]

            symbol_folders = {
                part
                for o in objects
                for part in o.object_name.split('/')
                if part.startswith('symbol=')
            }

            self.log.info(
                f"Found {len(data_files)} files, "
                f"{len(symbol_folders)} symbol folders in {path}"
            )
            return len(data_files) >= self.min_files or len(symbol_folders) >= 1

        except Exception as e:
            self.log.error(f"MinIO check failed: {e}")
            return False


# ── Task Functions ────────────────────────────────────────────────────────────

def cleanup_processes(**context):
    """Kill any leftover producer/consumer processes from previous runs."""
    try:
        import psutil
        targets = ["stream_data_producer.py", "realtime_data_consumer.py"]
        killed  = 0
        for proc in psutil.process_iter(['pid', 'cmdline']):
            try:
                cmdline = ' '.join(proc.info['cmdline'] or [])
                for t in targets:
                    if t in cmdline:
                        proc.kill()
                        print(f"Killed {proc.pid}: {t}")
                        killed += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        print(f"Cleanup done — {killed} processes killed ✓")
    except ImportError:
        print("psutil not available — skipping process cleanup")


def collect_streaming_data(**context):
    """
    Run producer and consumer in parallel for 5 minutes.
    5 minutes gives enough data for 3-minute windows to complete.
    """
    import time

    scripts_path = "/opt/airflow/dags/scripts"
    timeout_secs = 300  # 5 minutes — enough for portfolio demo

    # Verify scripts exist
    for script in ["stream_data_producer.py", "realtime_data_consumer.py"]:
        path = f"{scripts_path}/{script}"
        if not os.path.exists(path):
            raise FileNotFoundError(f"Script not found: {path}")
        print(f"✓ Found: {path}")

    print(f"Starting producer and consumer for {timeout_secs // 60} minutes...")

    producer = subprocess.Popen(
        [sys.executable, f"{scripts_path}/stream_data_producer.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    consumer = subprocess.Popen(
        [sys.executable, f"{scripts_path}/realtime_data_consumer.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    print(f"Producer PID: {producer.pid}")
    print(f"Consumer PID: {consumer.pid}")

    # Check they didn't crash immediately
    import time
    time.sleep(10)
    if producer.poll() is not None:
        _, stderr = producer.communicate()
        raise Exception(f"Producer crashed!\n{stderr}")
    if consumer.poll() is not None:
        _, stderr = consumer.communicate()
        raise Exception(f"Consumer crashed!\n{stderr}")

    print("Both processes running cleanly — waiting for remaining time...")
    time.sleep(timeout_secs - 10)

    # Stop both cleanly
    for proc, name in [(producer, "Producer"), (consumer, "Consumer")]:
        try:
            proc.terminate()
            proc.wait(timeout=10)
            print(f"{name} stopped cleanly ✓")
        except subprocess.TimeoutExpired:
            proc.kill()
            print(f"{name} force killed")

    print("Data collection completed ✓")


def run_spark_processing(**context):
    """
    Run spark_stream_batch_processor.py via docker exec.
    Passes {{ ds }} (UTC) so Spark reads the correct date partition.
    """
    ds = context['ds']  # UTC date e.g. "2026-03-29"
    print(f"Running Spark processor for UTC date: {ds}")

    cmd = [
        "docker", "exec",
        "stockmarketdatapipeline-spark-client-1",
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--conf", "spark.jars.ivy=/tmp/.ivy2",
        "--driver-memory", "1g",
        "--executor-memory", "1g",
        "--executor-cores", "1",
        "--packages",
        "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901",
        "/opt/spark/jobs/spark_stream_batch_processor.py",
        ds   # ✅ pass UTC date — matches consumer file paths
    ]

    print(f"Command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False)

    if result.returncode == 0:
        print("Spark processing completed successfully ✓")
    else:
        raise Exception(f"Spark processing failed with exit code: {result.returncode}")


def load_to_snowflake(**context):
    """Load processed realtime data from MinIO to Snowflake."""
    snowflake_password = os.getenv("SNOWFLAKE_PASSWORD", "")

    result = subprocess.run(
        [sys.executable, "/opt/airflow/dags/scripts/load_stream_to_snowflake.py"],
        env={**os.environ, "SNOWFLAKE_PASSWORD": snowflake_password},
        capture_output=False
    )

    if result.returncode != 0:
        raise Exception(f"Snowflake load failed: exit code {result.returncode}")

    print("Snowflake load completed ✓")


def pipeline_summary(**context):
    """Print summary of what was processed."""
    ds             = context['ds']
    year, month, day = ds.split("-")

    client = get_minio_client()
    bucket = "stock-market-data"

    raw_prefix = f"raw/realtime/year={year}/month={month}/day={day}"
    raw_files  = [
        o for o in client.list_objects(bucket, prefix=raw_prefix, recursive=True)
        if o.object_name.endswith('.csv') and o.size > 0
    ]

    proc_prefix  = f"processed/realtime/year={year}/month={month}/day={day}"
    proc_objects = list(client.list_objects(bucket, prefix=proc_prefix, recursive=True))
    proc_parquet = [o for o in proc_objects if o.object_name.endswith('.parquet') and o.size > 0]

    symbol_folders = {
        part
        for o in proc_objects
        for part in o.object_name.split('/')
        if part.startswith('symbol=')
    }

    print("=" * 55)
    print("  PIPELINE EXECUTION SUMMARY")
    print("=" * 55)
    print(f"  Airflow Date (UTC) : {ds}")
    print(f"  Raw CSV files      : {len(raw_files)}")
    print(f"  Processed parquet  : {len(proc_parquet)}")
    print(f"  Symbols processed  : {sorted(symbol_folders)}")
    print("=" * 55)
    if raw_files and proc_parquet:
        print("  ✅ Pipeline completed successfully")
    else:
        print("  ⚠️  Check individual task logs")
    print("=" * 55)


def final_cleanup(**context):
    """Final cleanup."""
    try:
        import psutil
        for proc in psutil.process_iter(['pid', 'cmdline']):
            try:
                cmdline = ' '.join(proc.info['cmdline'] or [])
                for t in ["stream_data_producer.py", "realtime_data_consumer.py"]:
                    if t in cmdline:
                        proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    except ImportError:
        pass
    print("Final cleanup completed ✓")


# ── DAG Definition ────────────────────────────────────────────────────────────

default_args = {
    "owner":            "Atul",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=1),
}

with DAG(
    dag_id="stock_streaming_pipeline",
    default_args=default_args,
    description="Stock Market Streaming Analytics Pipeline (UTC)",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 9, 15),
    catchup=False,
    max_active_runs=1,
    tags=['streaming', 'analytics', 'snowflake']
) as dag:

    t1 = PythonOperator(task_id="cleanup_processes",      python_callable=cleanup_processes)
    t2 = PythonOperator(task_id="collect_streaming_data", python_callable=collect_streaming_data,
                        execution_timeout=timedelta(minutes=8))
    t3 = MinIODataSensor(task_id='wait_for_raw_data',     bucket_name='stock-market-data',
                         prefix='raw/realtime', min_files=1, poke_interval=15, timeout=300)
    t4 = PythonOperator(task_id="spark_analytics_processing", python_callable=run_spark_processing,
                        execution_timeout=timedelta(minutes=15))
    t5 = MinIODataSensor(task_id='validate_analytics_data',   bucket_name='stock-market-data',
                         prefix='processed/realtime', min_files=1, poke_interval=15, timeout=180)
    t6 = PythonOperator(task_id="load_to_snowflake",      python_callable=load_to_snowflake)
    t7 = PythonOperator(task_id="pipeline_summary",       python_callable=pipeline_summary)
    t8 = PythonOperator(task_id="final_cleanup",          python_callable=final_cleanup,
                        trigger_rule='all_done')

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8
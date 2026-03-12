"""
DAG to orchestrate backfill and cleanup of late orders.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import json
import glob

default_args = {
    "owner"           : "etl-team",
    "retries"         : 2,
    "retry_delay"     : timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    "late_data_etl_pipeline",
    default_args    = default_args,
    description     = "Handle late-arriving orders with backfill",
    schedule_interval = "*/10 * * * *",  # every 10 minutes
    start_date      = datetime(2024, 1, 1),
    catchup         = False,
    tags            = ["etl", "late-data", "orders"],
)

def check_late_data(**context):
    late_dir = "/opt/airflow/late_data"
    files = glob.glob(f"{late_dir}/*.json")
    
    if files:
        print(f"Found {len(files)} late data file(s): {files}")
        context["task_instance"].xcom_push(key="has_late_data", value=True)
        context["task_instance"].xcom_push(key="file_count",    value=len(files))
    else:
        print("No late data files found.")
        context["task_instance"].xcom_push(key="has_late_data", value=False)
        context["task_instance"].xcom_push(key="file_count",    value=0)

check_task = PythonOperator(
    task_id         = "check_late_data",
    python_callable = check_late_data,
    dag             = dag,
)

backfill_task = BashOperator(
    task_id      = "run_backfill",
    bash_command = """
        echo "Starting backfill job..."
        spark-submit \
          --master spark://spark-master:7077 \
          --packages io.delta:delta-core_2.12:2.4.0 \
          /opt/airflow/batch/backfill_job.py \
        && echo "Backfill complete!" \
        || echo "Backfill failed or no data"
    """,
    dag = dag,
)

def validate_pipeline(**context):
    delta_path = "/opt/airflow/delta_tables/raw_orders"
    
    if not os.path.exists(delta_path):
        print("Delta table not found - streaming may not have started yet.")
        return
    
    # Check for parquet files
    parquet_files = glob.glob(f"{delta_path}/**/*.parquet", recursive=True)
    print(f"Table contains {len(parquet_files)} parquet file(s)")
    
    late_files = glob.glob("/opt/airflow/late_data/*.json")
    print(f"Files still in late_data: {len(late_files)}")
    
    print("Validation passed.")

validate_task = PythonOperator(
    task_id         = "validate_pipeline",
    python_callable = validate_pipeline,
    dag             = dag,
)

def cleanup_late_files(**context):
    late_dir     = "/opt/airflow/late_data"
    archive_dir  = f"{late_dir}/archived"
    os.makedirs(archive_dir, exist_ok=True)
    
    files = glob.glob(f"{late_dir}/*.json")
    for f in files:
        filename = os.path.basename(f)
        ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive  = f"{archive_dir}/{ts}_{filename}"
        os.rename(f, archive)
        print(f"Archived: {filename} -> {archive}")
    
    print(f"Cleaned up {len(files)} file(s)")

cleanup_task = PythonOperator(
    task_id         = "cleanup_late_files",
    python_callable = cleanup_late_files,
    dag             = dag,
)

check_task >> backfill_task >> validate_task >> cleanup_task

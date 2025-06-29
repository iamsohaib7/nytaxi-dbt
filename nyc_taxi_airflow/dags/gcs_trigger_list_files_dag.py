from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectsWithPrefixExistenceSensor,
)
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import (
    CopyFromExternalStageToSnowflakeOperator,
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.python import get_current_context

from datetime import datetime, timedelta
import os
from pathlib import Path
from dotenv import load_dotenv
import json

# Load environment variables
BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

# Constants
BUCKET_NAME = os.getenv("BUCKET_NAME")
SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID")
GCP_CONN_ID = os.getenv("GCP_CONN_ID")
STAGE_PATH = os.getenv("STAGE_PATH")  # e.g. @gcs_stage
RAW_TABLE = "nyc_yellow_taxi_trips_raw_data"

# Checkpoint file for processed files
CHECKPOINT_FILE = os.path.join(
    os.environ.get("AIRFLOW_HOME", "/tmp"), "processed_files.json"
)


# Load checkpoint
def load_checkpoint(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return []


# Save checkpoint
def save_checkpoint(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# Filter new files
def filter_new_files(ti, **context):
    hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    all_files = hook.list(bucket_name=BUCKET_NAME, prefix="yellow_")

    processed = load_checkpoint(CHECKPOINT_FILE)
    new_files = [f for f in all_files if f not in processed]

    print(
        f"Total files: {len(all_files)} | Processed: {len(processed)} | New: {len(new_files)}"
    )

    if not new_files:
        raise AirflowSkipException("No new files to process.")

    ti.xcom_push(key="new_files", value=new_files)
    save_checkpoint(CHECKPOINT_FILE, processed + new_files)
    return new_files


def get_filtered_files():
    ti = get_current_context()["ti"]
    files = ti.xcom_pull(task_ids="filter_task", key="new_files") or []
    if not files:
        raise AirflowSkipException("No new files to load.")
    return files


# Default args
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# DAG definition
with DAG(
    dag_id="gcs_to_snowflake_incremental",
    schedule=timedelta(hours=1),
    catchup=False,
    default_args=default_args,
    render_template_as_native_obj=True,
    tags=["gcs", "snowflake", "incremental"],
    description="Incremental load from GCS to Snowflake using filtered files",
) as dag:

    wait_for_files = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_any_file",
        bucket=BUCKET_NAME,
        prefix="yellow_",
        google_cloud_conn_id=GCP_CONN_ID,
        poke_interval=30,
        timeout=300,
        mode="reschedule",
        deferrable=True,
    )

    filter_task = PythonOperator(
        task_id="filter_task",
        python_callable=filter_new_files,
    )

    load_filtered_files = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_filtered_files",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        table=RAW_TABLE,
        stage=STAGE_PATH,
        file_format="(TYPE = PARQUET)",
        files="{{ ti.xcom_pull(task_ids='filter_task', key='new_files') }}",
        copy_options="MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE",
    )

    wait_for_files >> filter_task >> load_filtered_files

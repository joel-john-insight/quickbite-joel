"""
QuickBite daily pipeline (Joel)

One run = one day's four files, chosen by the run's logical date ({{ ds }}).
Part 1 (this version): create raw tables if missing -> wait for files in Drive
                       -> copy to GCS landing -> load into BigQuery raw -> validate the load.
Later parts will add: Dataform run, data-quality check, archive, notification.
"""

import csv
import io
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.suite.sensors.drive import GoogleDriveFileExistenceSensor
from airflow.providers.google.cloud.transfers.gdrive_to_gcs import GoogleDriveToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# BigQuery region where joel_raw / joel_core / joel_marts live (same as the bucket).
REGION = "us-central1"

# The four files that arrive every day, and their columns (in file order).
FILES = {
    "restaurants": ["restaurant_id", "name", "city", "cuisine", "is_active", "onboarded_date"],
    "customers":   ["customer_id", "name", "email", "phone", "city", "signup_date", "updated_at"],
    "orders":      ["order_id", "customer_id", "restaurant_id", "order_ts", "status",
                    "payment_method", "updated_at"],
    "order_items": ["order_id", "line_no", "item_name", "quantity", "unit_price"],
}

# SQL that creates the four raw tables if they don't exist yet (does nothing if they do).
# Built from FILES so the table columns always match the file columns.
# Every file column is STRING (raw keeps values exactly as they arrived), plus 3 label columns.
def raw_table_ddl(table, columns):
    file_columns = ", ".join(c + " STRING" for c in columns)
    return (
        "CREATE TABLE IF NOT EXISTS "
        "`{{ var.value.joel_project_id }}.{{ var.value.joel_raw_dataset }}." + table + "` ("
        + file_columns
        + ", _source_file STRING, _export_date DATE, _loaded_at TIMESTAMP"
        + ") PARTITION BY _export_date;"
    )

CREATE_RAW_TABLES_SQL = "\n".join(raw_table_ddl(t, c) for t, c in FILES.items())

# SQL run once per file per day. Everything inside {{ }} is filled in by Airflow at run time:
#   var.value.xxx  -> an Airflow Variable
#   ds             -> the run's logical date, e.g. 2026-10-11
#   params.xxx     -> values we pass in below for each file
LOAD_SQL = """
-- Step 1: load the CSV from GCS into a scratch table, every column as text.
LOAD DATA OVERWRITE `{{ var.value.joel_project_id }}.{{ var.value.joel_raw_dataset }}._load_{{ params.table }}`
  ({{ params.schema }})
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = ['gs://{{ var.value.joel_gcs_bucket }}/capstone/joel/landing/{{ ds }}/{{ params.table }}_{{ ds }}.csv']
);

-- Step 2: replace this day's rows in the raw table, all or nothing.
BEGIN TRANSACTION;

DELETE FROM `{{ var.value.joel_project_id }}.{{ var.value.joel_raw_dataset }}.{{ params.table }}`
WHERE _export_date = DATE '{{ ds }}';

INSERT INTO `{{ var.value.joel_project_id }}.{{ var.value.joel_raw_dataset }}.{{ params.table }}`
  ({{ params.columns }}, _source_file, _export_date, _loaded_at)
SELECT {{ params.columns }},
       '{{ params.table }}_{{ ds }}.csv',
       DATE '{{ ds }}',
       CURRENT_TIMESTAMP()
FROM `{{ var.value.joel_project_id }}.{{ var.value.joel_raw_dataset }}._load_{{ params.table }}`;

COMMIT TRANSACTION;
"""

def validate_raw(table, expected_columns, ds, **context):
    """Ingestion check: did we copy this day's file into raw faithfully?

    1. The file's header must equal the expected columns (catches renamed or reordered columns).
    2. Rows in the file must equal rows in raw for this day (catches missing AND doubled rows).
    3. Our label columns must be filled on every row (catches bugs in our own load SQL).
    """
    project = Variable.get("joel_project_id")
    bucket = Variable.get("joel_gcs_bucket")
    raw_dataset = Variable.get("joel_raw_dataset")
    object_name = f"capstone/joel/landing/{ds}/{table}_{ds}.csv"

    # Read the file from the GCS landing folder.
    text = GCSHook().download(bucket_name=bucket, object_name=object_name).decode("utf-8-sig")
    rows = list(csv.reader(io.StringIO(text, newline="")))
    header = rows[0]
    data_rows = [r for r in rows[1:] if any(cell.strip() for cell in r)]  # ignore blank lines

    # Check 1: header
    if header != expected_columns:
        raise ValueError(f"{table} {ds}: columns changed. Expected {expected_columns}, got {header}")

    # Count this day's rows in raw, and rows with missing label columns.
    sql = f"""
        SELECT COUNT(*),
               COUNTIF(_source_file IS NULL OR _loaded_at IS NULL)
        FROM `{project}.{raw_dataset}.{table}`
        WHERE _export_date = DATE '{ds}'
    """
    raw_count, missing_labels = BigQueryHook(use_legacy_sql=False, location=REGION).get_first(sql)

    # Check 2: row count
    if raw_count != len(data_rows):
        raise ValueError(f"{table} {ds}: file has {len(data_rows)} rows, raw has {raw_count}")

    # Check 3: label columns
    if missing_labels:
        raise ValueError(f"{table} {ds}: {missing_labels} rows have empty _source_file/_loaded_at")

    print(f"{table} {ds}: OK, header matches and {raw_count} rows in both file and raw")


with DAG(
    dag_id="joel_quickbite",
    start_date=datetime(2026, 10, 10),   # first day of data
    schedule="@daily",                   # one run per day
    catchup=True,                        # also create runs for past days (10-10 .. 10-13)
    max_active_runs=1,                   # process days one at a time, in order
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["joel", "capstone"],
) as dag:

    # 0. Make sure the raw tables exist (first run creates them; later runs do nothing).
    create_raw_tables = BigQueryInsertJobOperator(
        task_id="create_raw_tables",
        location=REGION,
        configuration={"query": {"query": CREATE_RAW_TABLES_SQL, "useLegacySql": False}},
    )

    for table, columns in FILES.items():
        file_name = table + "_{{ ds }}.csv"   # e.g. orders_2026-10-11.csv

        # 1. Wait until the file exists in Drive. 'reschedule' frees the worker while waiting.
        wait_for_file = GoogleDriveFileExistenceSensor(
            task_id=f"wait_{table}",
            folder_id="{{ var.value.joel_drive_folder_id }}",
            file_name=file_name,
            mode="reschedule",
            poke_interval=5 * 60,        # check every 5 minutes
            timeout=6 * 60 * 60,         # give up after 6 hours
        )

        # 2. Copy the file from Drive into the GCS landing folder for that day.
        copy_to_gcs = GoogleDriveToGCSOperator(
            task_id=f"copy_{table}",
            folder_id="{{ var.value.joel_drive_folder_id }}",
            file_name=file_name,
            bucket_name="{{ var.value.joel_gcs_bucket }}",
            object_name="capstone/joel/landing/{{ ds }}/" + file_name,
        )

        # 3. Load it from GCS into the raw BigQuery table (re-run safe).
        load_to_raw = BigQueryInsertJobOperator(
            task_id=f"load_raw_{table}",
            location=REGION,
            configuration={"query": {"query": LOAD_SQL, "useLegacySql": False}},
            params={
                "table": table,
                "columns": ", ".join(columns),
                "schema": ", ".join(c + " STRING" for c in columns),
            },
        )

        # 4. Check the load was faithful. No retries: a mismatch won't fix itself by retrying.
        validate = PythonOperator(
            task_id=f"validate_raw_{table}",
            python_callable=validate_raw,
            op_kwargs={"table": table, "expected_columns": columns},
            retries=0,
        )

        create_raw_tables >> wait_for_file >> copy_to_gcs >> load_to_raw >> validate

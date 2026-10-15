"""
QuickBite daily pipeline (Joel)

One run = one day's four files, chosen by the run's logical date ({{ ds }}).

  create_raw_tables
    -> for each file: wait in Drive -> copy to GCS landing -> load into raw -> validate the load
  -> Dataform: compile -> run (staging, core, marts + assertions)
  -> data-quality check on the result
  -> archive the day's files (landing -> archive)
  -> notify: one summary line, and fail the run if anything above failed

Airflow Variables used (Admin -> Variables):
  joel_project_id, joel_gcs_bucket, joel_drive_folder_id,
  joel_raw_dataset, joel_core_dataset, joel_marts_dataset, joel_dataform_repo,
  joel_dataform_service_account
"""

import csv
import io
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.suite.sensors.drive import GoogleDriveFileExistenceSensor
from airflow.providers.google.cloud.transfers.gdrive_to_gcs import GoogleDriveToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

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


# Data-quality check after Dataform. BigQueryCheckOperator fails the task if ANY value
# in the first row is FALSE (or 0 / NULL), so each column is one yes/no check.
DQ_CHECK_SQL = """
SELECT
  -- 1. fct_orders has exactly one row per order
  (SELECT COUNT(*) = COUNT(DISTINCT order_id)
   FROM `{{ var.value.joel_project_id }}.{{ var.value.joel_core_dataset }}.fct_orders`) AS one_row_per_order,

  -- 2. every order we received is accounted for: either valid (fct_orders) or rejected
  (SELECT COUNT(DISTINCT order_id) FROM `{{ var.value.joel_project_id }}.{{ var.value.joel_core_dataset }}.stg_orders`)
  = (SELECT COUNT(*) FROM `{{ var.value.joel_project_id }}.{{ var.value.joel_core_dataset }}.fct_orders`)
  + (SELECT COUNT(*) FROM `{{ var.value.joel_project_id }}.{{ var.value.joel_core_dataset }}.rejected_orders`) AS every_order_accounted_for,

  -- 3. no negative revenue anywhere in the daily mart
  (SELECT COUNTIF(gross_revenue < 0) = 0
   FROM `{{ var.value.joel_project_id }}.{{ var.value.joel_marts_dataset }}.mart_daily_city_revenue`) AS no_negative_revenue
"""


def notify(ds, **context):
    """Last task: print one summary line for the day. If any task failed, fail this task
    too, so the whole run shows red and the summary says exactly what broke."""
    failed = sorted(
        ti.task_id
        for ti in context["dag_run"].get_task_instances()
        if ti.state in ("failed", "upstream_failed")
    )

    numbers = "numbers unavailable"
    try:
        project = Variable.get("joel_project_id")
        core = Variable.get("joel_core_dataset")
        marts = Variable.get("joel_marts_dataset")
        orders, revenue = BigQueryHook(use_legacy_sql=False, location=REGION).get_first(f"""
            SELECT COALESCE(SUM(delivered_orders), 0), COALESCE(SUM(gross_revenue), 0)
            FROM `{project}.{marts}.mart_daily_city_revenue` WHERE order_date = DATE '{ds}'""")
        rejected = BigQueryHook(use_legacy_sql=False, location=REGION).get_first(f"""
            SELECT COUNT(*) FROM `{project}.{core}.rejected_orders`
            WHERE source_export_date = DATE '{ds}'""")[0]
        numbers = f"{orders} delivered orders, revenue INR {revenue}, {rejected} rejected orders in this file"
    except Exception as err:  # the summary should never hide the real failure
        numbers = f"numbers unavailable ({err})"

    if failed:
        raise AirflowException(f"QuickBite {ds} FAILED. Failed tasks: {', '.join(failed)}. {numbers}")
    print(f"QuickBite {ds} SUCCEEDED: {numbers}")


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

    all_validations = []

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
        all_validations.append(validate)

    # 5. Dataform, step 1: compile the code on the repository's main branch.
    dataform_compile = DataformCreateCompilationResultOperator(
        task_id="dataform_compile",
        project_id="{{ var.value.joel_project_id }}",
        region=REGION,
        repository_id="{{ var.value.joel_dataform_repo }}",
        compilation_result={"git_commitish": "main"},
    )

    # 6. Dataform, step 2: run everything (staging, core, marts, assertions) and wait.
    #    The task fails if any Dataform action or assertion fails.
    dataform_run = DataformCreateWorkflowInvocationOperator(
        task_id="dataform_run",
        project_id="{{ var.value.joel_project_id }}",
        region=REGION,
        repository_id="{{ var.value.joel_dataform_repo }}",
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull(task_ids='dataform_compile')['name'] }}",
            # The repository has strict act-as checks on, so every run must name the
            # service account it runs as (the UI fills this in for you; the API doesn't).
            "invocation_config": {"service_account": "{{ var.value.joel_dataform_service_account }}"},
        },
    )

    # 7. Data-quality check on the result (see DQ_CHECK_SQL). No retries: bad data stays bad.
    dq_check = BigQueryCheckOperator(
        task_id="dq_check",
        sql=DQ_CHECK_SQL,
        use_legacy_sql=False,
        location=REGION,
        retries=0,
    )

    # 8. Move the day's files out of landing into archive (only if everything above passed).
    archive_files = GCSToGCSOperator(
        task_id="archive_files",
        source_bucket="{{ var.value.joel_gcs_bucket }}",
        source_object="capstone/joel/landing/{{ ds }}/*",
        destination_bucket="{{ var.value.joel_gcs_bucket }}",
        destination_object="capstone/joel/archive/{{ ds }}/",
        move_object=True,
    )

    # 9. Summary / alert. 'all_done' = run even if something upstream failed.
    notify_result = PythonOperator(
        task_id="notify",
        python_callable=notify,
        trigger_rule="all_done",
        retries=0,
    )

    all_validations >> dataform_compile >> dataform_run >> dq_check >> archive_files >> notify_result

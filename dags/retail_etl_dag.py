"""
dags/retail_etl_dag.py
----------------------
Cloud Composer (Airflow) DAG that orchestrates the full retail ETL pipeline:
  1. Sense new files in GCS
  2. Trigger Cloud Dataflow job (Beam pipeline)
  3. Wait for Dataflow job to complete
  4. Run dbt staging models (Silver)
  5. Run dbt mart models (Gold)
  6. Run dbt tests
  7. Notify on failure via Pub/Sub

Schedule: daily at 02:00 UTC
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# ── Config ──────────────────────────────────────────────────────────────────

PROJECT_ID   = "{{ var.value.gcp_project_id }}"
REGION       = "us-central1"
GCS_BUCKET   = "{{ var.value.gcp_bucket }}"
BQ_STAGING   = "retail_staging"
BQ_MARTS     = "retail_marts"
DBT_DIR      = "/home/airflow/gcs/dags/dbt_transforms"

DEFAULT_ARGS = {
    "owner":            "tejeswini_reddy",
    "depends_on_past":  False,
    "email_on_failure": True,
    "email":            ["tejaswiniareddy@gmail.com"],
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ── DAG ─────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="retail_etl_pipeline",
    description="GCP retail ETL: GCS → Dataflow → BigQuery → dbt → Dashboard",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["retail", "etl", "bigquery", "dataflow"],
    doc_md="""
    ## Retail ETL Pipeline

    End-to-end pipeline for e-commerce analytics on GCP.

    **Bronze (GCS)** → **Silver (BigQuery Staging via Dataflow)** → **Gold (BigQuery Marts via dbt)**

    Failure in any stage sends an alert. Dead-letter records are written to
    `gs://{bucket}/dead_letter/` for review.
    """,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # ── 1. Sense raw files ───────────────────────────────────────────────
    with TaskGroup("sense_raw_data") as sense_group:
        for entity in ["customers", "products", "orders", "order_items", "returns"]:
            GCSObjectsWithPrefixExistenceSensor(
                task_id=f"sense_{entity}",
                bucket=GCS_BUCKET,
                prefix=f"raw/{entity}",
                mode="poke",
                poke_interval=60,
                timeout=600,
            )

    # ── 2. Dataflow ETL ──────────────────────────────────────────────────
    run_dataflow = DataflowCreatePythonJobOperator(
        task_id="run_dataflow_etl",
        py_file=f"gs://{GCS_BUCKET}/scripts/etl/pipeline.py",
        job_name="retail-etl-{{ ds_nodash }}",
        options={
            "project":          PROJECT_ID,
            "region":           REGION,
            "input":            f"gs://{GCS_BUCKET}/raw/",
            "bq_dataset":       BQ_STAGING,
            "staging_location": f"gs://{GCS_BUCKET}/staging/",
            "temp_location":    f"gs://{GCS_BUCKET}/temp/",
            "dead_letter_path": f"gs://{GCS_BUCKET}/dead_letter/{{ ds_nodash }}",
            "setup_file":       f"gs://{GCS_BUCKET}/scripts/setup.py",
            "num_workers":      "4",
            "max_num_workers":  "20",
            "machine_type":     "n1-standard-4",
            "save_main_session": "true",
        },
        dataflow_default_options={
            "project": PROJECT_ID,
            "region":  REGION,
        },
        location=REGION,
        wait_until_finished=True,
    )

    # ── 3. dbt staging (Silver) ──────────────────────────────────────────
    with TaskGroup("dbt_staging") as dbt_staging_group:
        dbt_staging = BashOperator(
            task_id="dbt_run_staging",
            bash_command=f"""
                cd {DBT_DIR} && \
                dbt run \
                    --profiles-dir . \
                    --target prod \
                    --select staging \
                    --vars '{{"run_date": "{{{{ ds }}}}"}}'
            """,
        )

    # ── 4. dbt marts (Gold) ──────────────────────────────────────────────
    with TaskGroup("dbt_marts") as dbt_marts_group:
        dbt_marts = BashOperator(
            task_id="dbt_run_marts",
            bash_command=f"""
                cd {DBT_DIR} && \
                dbt run \
                    --profiles-dir . \
                    --target prod \
                    --select marts \
                    --vars '{{"run_date": "{{{{ ds }}}}"}}'
            """,
        )

    # ── 5. dbt tests ─────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
            cd {DBT_DIR} && \
            dbt test \
                --profiles-dir . \
                --target prod
        """,
    )

    # ── 6. Dead letter audit ─────────────────────────────────────────────
    audit_dead_letter = BigQueryInsertJobOperator(
        task_id="audit_dead_letter_count",
        configuration={
            "query": {
                "query": f"""
                    SELECT
                        '{{{{ ds }}}}' as run_date,
                        COUNT(*) as rejected_record_count
                    FROM `{PROJECT_ID}.{BQ_STAGING}.dead_letter_log`
                    WHERE DATE(ingested_at) = '{{{{ ds }}}}'
                """,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": BQ_STAGING,
                    "tableId":   "pipeline_audit_log",
                },
                "writeDisposition": "WRITE_APPEND",
            }
        },
    )

    # ── Dependencies ─────────────────────────────────────────────────────
    (
        start
        >> sense_group
        >> run_dataflow
        >> dbt_staging_group
        >> dbt_marts_group
        >> dbt_test
        >> audit_dead_letter
        >> end
    )

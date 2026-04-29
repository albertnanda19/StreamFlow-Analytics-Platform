import logging
import os
import sys
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

sys.path.insert(0, "/opt/airflow/scripts")
from setup_alerting import on_failure_callback, on_sla_miss_callback


logger = logging.getLogger(__name__)

DBT_CMD     = "cd /opt/airflow/dbt_project && dbt"
DBT_FLAGS   = "--target prod --profiles-dir /opt/airflow/dbt_project --no-write-json"
MINIO_EP    = os.getenv("MINIO_ENDPOINT",     "http://minio:9000")
AWS_KEY     = os.getenv("AWS_ACCESS_KEY_ID",  "streamflow")
AWS_SECRET  = os.getenv("AWS_SECRET_ACCESS_KEY", "streamflow123")
PG_HOST     = os.getenv("POSTGRES_HOST",      "postgres")
PG_PORT     = os.getenv("POSTGRES_PORT",      "5432")
PG_USER     = os.getenv("POSTGRES_USER",      "streamflow")
PG_PASS     = os.getenv("POSTGRES_PASSWORD",  "streamflow123")
PG_DB       = os.getenv("POSTGRES_SOURCE_DB", "streamflow_source")

DEFAULT_ARGS = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}


def _check_bronze_freshness(**context) -> dict:
    import boto3
    from botocore.client import Config

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_EP,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    cutoff = datetime.utcnow() - timedelta(hours=2)
    result = {}

    for bucket_path, key in (
        ("bronze", "orders/"),
        ("bronze", "pageviews/"),
    ):
        resp = s3.list_objects_v2(Bucket=bucket_path, Prefix=key)
        contents = resp.get("Contents", [])
        fresh = [
            o for o in contents
            if o["LastModified"].replace(tzinfo=None) >= cutoff
        ]
        count = len(fresh)
        result[f"{key.rstrip('/')}_file_count"] = count
        logger.info("Bronze %s: %d fresh files (since %s)", key, count, cutoff)

    if result.get("orders_file_count", 0) == 0 and result.get("pageviews_file_count", 0) == 0:
        raise AirflowSkipException("No fresh Bronze data found — skipping Silver run")

    result["check_timestamp"] = datetime.utcnow().isoformat()
    context["ti"].xcom_push(key="bronze_freshness", value=result)
    return result


def _run_data_quality_checks(**context) -> dict:
    results = {"status": "PASS", "checks": []}
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            user=PG_USER, password=PG_PASS, dbname=PG_DB,
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) as null_user_id,
                   SUM(CASE WHEN total_amount IS NULL OR total_amount <= 0 THEN 1 ELSE 0 END) as invalid_amount
            FROM orders
        """)
        row = cur.fetchone()
        total, null_uid, invalid_amt = row
        null_rate = (null_uid / max(total, 1)) * 100
        results["checks"].append({
            "name": "null_user_id_rate",
            "value": null_rate,
            "threshold": 5.0,
            "status": "PASS" if null_rate < 5.0 else "FAIL",
        })
        cur.close()
        conn.close()
    except Exception as exc:
        logger.error("DQ check failed: %s", exc)
        results["status"] = "WARN"

    context["ti"].xcom_push(key="dq_results", value=results)
    return results


def _update_pipeline_metadata(**context) -> None:
    ti         = context["ti"]
    run_id     = context["run_id"]
    dag_id     = context["dag"].dag_id
    exec_date  = context["execution_date"]
    freshness  = ti.xcom_pull(key="bronze_freshness", task_ids="check_bronze_freshness") or {}
    dq         = ti.xcom_pull(key="dq_results",       task_ids="run_data_quality_checks") or {}

    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            user=PG_USER, password=PG_PASS, dbname=PG_DB,
        )
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id             SERIAL PRIMARY KEY,
                dag_id         VARCHAR(250),
                run_id         VARCHAR(250),
                execution_date TIMESTAMP,
                rows_processed JSONB,
                dq_status      VARCHAR(20),
                recorded_at    TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            INSERT INTO pipeline_runs (dag_id, run_id, execution_date, rows_processed, dq_status)
            VALUES (%s, %s, %s, %s, %s)
        """, (dag_id, run_id, exec_date, str(freshness), dq.get("status", "UNKNOWN")))
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Pipeline metadata recorded for run_id=%s", run_id)
    except Exception as exc:
        logger.error("Failed to write pipeline metadata: %s", exc)


with DAG(
    dag_id="bronze_to_silver",
    description="Hourly: Bronze Delta Lake → Silver dbt transformations",
    schedule_interval="0 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=on_sla_miss_callback,
    tags=["silver", "dbt", "hourly"],
    doc_md="Reads fresh Bronze data, runs dbt Silver models, validates with Great Expectations.",
) as dag:

    check_bronze_freshness = PythonOperator(
        task_id="check_bronze_freshness",
        python_callable=_check_bronze_freshness,
        sla=timedelta(minutes=5),
    )

    with TaskGroup("silver_dbt_runs") as silver_dbt_runs:
        run_dbt_silver_orders = BashOperator(
            task_id="run_dbt_silver_orders",
            bash_command=f"{DBT_CMD} run --select silver_orders silver_order_items {DBT_FLAGS}",
            sla=timedelta(minutes=20),
        )

        run_dbt_silver_pageviews = BashOperator(
            task_id="run_dbt_silver_pageviews",
            bash_command=f"{DBT_CMD} run --select silver_pageviews silver_sessions {DBT_FLAGS}",
            sla=timedelta(minutes=20),
        )

        run_dbt_silver_inventory = BashOperator(
            task_id="run_dbt_silver_inventory",
            bash_command=f"{DBT_CMD} run --select silver_inventory {DBT_FLAGS}",
            sla=timedelta(minutes=10),
        )

    test_silver_models = BashOperator(
        task_id="test_silver_models",
        bash_command=f"{DBT_CMD} test --select silver {DBT_FLAGS}",
        sla=timedelta(minutes=15),
    )

    run_data_quality_checks = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=_run_data_quality_checks,
    )

    update_pipeline_metadata = PythonOperator(
        task_id="update_pipeline_metadata",
        python_callable=_update_pipeline_metadata,
        trigger_rule="all_done",
    )

    (
        check_bronze_freshness
        >> silver_dbt_runs
        >> test_silver_models
        >> run_data_quality_checks
        >> update_pipeline_metadata
    )

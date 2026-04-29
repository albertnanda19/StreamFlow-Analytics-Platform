import logging
import os
import sys
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/opt/airflow/scripts")
from setup_alerting import on_failure_callback

logger = logging.getLogger(__name__)

PG_HOST     = os.getenv("POSTGRES_HOST",      "postgres")
PG_PORT     = os.getenv("POSTGRES_PORT",      "5432")
PG_USER     = os.getenv("POSTGRES_USER",      "streamflow")
PG_PASS     = os.getenv("POSTGRES_PASSWORD",  "streamflow123")
PG_DB       = os.getenv("POSTGRES_SOURCE_DB", "streamflow_source")
MINIO_EP    = os.getenv("MINIO_ENDPOINT",     "http://minio:9000")
AWS_KEY     = os.getenv("AWS_ACCESS_KEY_ID",  "streamflow")
AWS_SECRET  = os.getenv("AWS_SECRET_ACCESS_KEY", "streamflow123")
CHUNK_SIZE  = 10_000

DEFAULT_ARGS = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "email_on_failure": False,
    "retries":          0,
    "on_failure_callback": on_failure_callback,
}


def _validate_backfill_params(**context) -> dict:
    from datetime import date

    conf       = context["dag_run"].conf or {}
    start_str  = conf.get("start_date")
    end_str    = conf.get("end_date")
    tables     = conf.get("tables", ["orders"])

    if not start_str or not end_str:
        raise ValueError("dag_run.conf must contain 'start_date' and 'end_date'")

    start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
    end_date   = datetime.strptime(end_str,   "%Y-%m-%d").date()

    if start_date >= end_date:
        raise ValueError("start_date must be before end_date")

    delta_days = (end_date - start_date).days
    if delta_days > 90:
        raise ValueError(f"Date range {delta_days} days exceeds 90-day maximum")

    params = {
        "start_date": start_str,
        "end_date":   end_str,
        "tables":     tables,
        "chunk_size": CHUNK_SIZE,
    }
    logger.info("Backfill params validated: %s", params)
    context["ti"].xcom_push(key="backfill_params", value=params)
    return params


def _export_postgres_to_bronze(**context) -> None:
    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq
    import io
    from botocore.client import Config

    ti      = context["ti"]
    params  = ti.xcom_pull(key="backfill_params", task_ids="validate_backfill_params")
    start   = params["start_date"]
    end     = params["end_date"]
    tables  = params["tables"]

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_EP,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    pg_conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASS, dbname=PG_DB,
    )

    total_written = 0
    for table in tables:
        offset = 0
        chunk_idx = 0
        while True:
            cur = pg_conn.cursor()
            cur.execute(
                f"SELECT * FROM {table} "
                f"WHERE created_at BETWEEN %s AND %s "
                f"ORDER BY created_at LIMIT %s OFFSET %s",
                (start, end, CHUNK_SIZE, offset),
            )
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            cur.close()

            if not rows:
                break

            df_data = {col: [r[i] for r in rows] for i, col in enumerate(cols)}
            arrow_table = pa.table(df_data)
            buf = io.BytesIO()
            pq.write_table(arrow_table, buf)
            buf.seek(0)

            s3_key = f"{table}/backfill/year={start[:4]}/chunk_{chunk_idx:04d}.parquet"
            s3.put_object(Bucket="bronze", Key=s3_key, Body=buf.getvalue())
            logger.info("Wrote %s/%s: %d rows", table, s3_key, len(rows))

            total_written += len(rows)
            offset        += CHUNK_SIZE
            chunk_idx     += 1

            if len(rows) < CHUNK_SIZE:
                break

    pg_conn.close()
    logger.info("Backfill complete: %d total rows written", total_written)
    context["ti"].xcom_push(key="rows_written", value=total_written)


with DAG(
    dag_id="historical_backfill",
    description="Manual: Backfill historical PostgreSQL data into Bronze Delta Lake",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["backfill", "admin"],
    params={
        "start_date": "2024-01-01",
        "end_date":   "2024-01-31",
        "tables":     ["orders"],
    },
    doc_md=(
        "Manually triggered backfill. Pass conf: "
        '{"start_date":"2024-01-01","end_date":"2024-01-31","tables":["orders"]}'
    ),
) as dag:

    validate_params = PythonOperator(
        task_id="validate_backfill_params",
        python_callable=_validate_backfill_params,
    )

    export_to_bronze = PythonOperator(
        task_id="export_postgres_to_bronze",
        python_callable=_export_postgres_to_bronze,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_refresh",
        trigger_dag_id="bronze_to_silver",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
    )

    validate_params >> export_to_bronze >> trigger_silver

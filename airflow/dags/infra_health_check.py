import logging
import os
import sys
from datetime import timedelta

import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/opt/airflow/scripts")
from setup_alerting import on_failure_callback, send_pipeline_alert

logger = logging.getLogger(__name__)

KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CH_HOST        = os.getenv("CLICKHOUSE_HOST",          "clickhouse")
CH_PORT        = int(os.getenv("CLICKHOUSE_PORT",      "8123"))
MINIO_EP       = os.getenv("MINIO_ENDPOINT",           "http://minio:9000")
AWS_KEY        = os.getenv("AWS_ACCESS_KEY_ID",        "streamflow")
AWS_SECRET     = os.getenv("AWS_SECRET_ACCESS_KEY",    "streamflow123")
SPARK_URL      = os.getenv("SPARK_MASTER_URL",         "spark://spark-master:7077")
DEBEZIUM_URL   = os.getenv("DEBEZIUM_URL",             "http://debezium:8083")
PG_HOST        = os.getenv("POSTGRES_HOST",            "postgres")
PG_PORT        = os.getenv("POSTGRES_PORT",            "5432")
PG_USER        = os.getenv("POSTGRES_USER",            "streamflow")
PG_PASS        = os.getenv("POSTGRES_PASSWORD",        "streamflow123")
PG_DB          = os.getenv("POSTGRES_SOURCE_DB",       "streamflow_source")

EXPECTED_KAFKA_TOPICS = {"orders", "pageviews", "inventory", "order-status", "dlq-orders", "dlq-pageviews"}
EXPECTED_MINIO_BUCKETS = {"bronze", "silver", "gold", "checkpoints"}

DEFAULT_ARGS = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "email_on_failure": False,
    "retries":          0,
    "on_failure_callback": on_failure_callback,
}


def _check_kafka(**context) -> dict:
    result = {"service": "kafka", "status": "UP", "message": ""}
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": KAFKA_SERVERS, "socket.timeout.ms": 5000})
        meta = admin.list_topics(timeout=10)
        existing = set(meta.topics.keys())
        missing  = EXPECTED_KAFKA_TOPICS - existing
        result["message"] = f"Topics: {len(existing)} found, missing: {missing or 'none'}"
        if missing:
            result["status"] = "WARN"
    except Exception as exc:
        result["status"] = "DOWN"
        result["message"] = str(exc)
    context["ti"].xcom_push(key="kafka_health", value=result)
    return result


def _check_minio(**context) -> dict:
    result = {"service": "minio", "status": "UP", "message": ""}
    try:
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
        existing  = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
        missing   = EXPECTED_MINIO_BUCKETS - existing
        result["message"] = f"Buckets: {existing}, missing: {missing or 'none'}"
        if missing:
            result["status"] = "WARN"
    except Exception as exc:
        result["status"] = "DOWN"
        result["message"] = str(exc)
    context["ti"].xcom_push(key="minio_health", value=result)
    return result


def _check_clickhouse(**context) -> dict:
    result = {"service": "clickhouse", "status": "UP", "message": ""}
    try:
        resp = requests.get(
            f"http://{CH_HOST}:{CH_PORT}/",
            params={"query": "SELECT 1"},
            timeout=5,
        )
        if resp.status_code == 200 and "1" in resp.text:
            result["message"] = "SELECT 1 OK"
        else:
            result["status"] = "WARN"
            result["message"] = f"Unexpected response: {resp.status_code}"
    except Exception as exc:
        result["status"] = "DOWN"
        result["message"] = str(exc)
    context["ti"].xcom_push(key="clickhouse_health", value=result)
    return result


def _check_spark(**context) -> dict:
    result = {"service": "spark", "status": "UP", "message": ""}
    spark_ui = SPARK_URL.replace("spark://", "http://").replace("7077", "8080")
    try:
        resp = requests.get(f"{spark_ui}/json/", timeout=10)
        data = resp.json()
        workers = len(data.get("workers", []))
        result["message"] = f"{workers} worker(s) registered"
        if workers == 0:
            result["status"] = "WARN"
            result["message"] = "Spark master has 0 workers"
    except Exception as exc:
        result["status"] = "DOWN"
        result["message"] = str(exc)
    context["ti"].xcom_push(key="spark_health", value=result)
    return result


def _check_debezium(**context) -> dict:
    result = {"service": "debezium", "status": "UP", "message": ""}
    try:
        resp = requests.get(
            f"{DEBEZIUM_URL}/connectors/postgres-source-connector/status",
            timeout=10,
        )
        if resp.status_code == 200:
            state = resp.json().get("connector", {}).get("state", "UNKNOWN")
            result["message"] = f"Connector state: {state}"
            if state != "RUNNING":
                result["status"] = "WARN"
        else:
            result["status"] = "WARN"
            result["message"] = f"HTTP {resp.status_code}"
    except Exception as exc:
        result["status"] = "DOWN"
        result["message"] = str(exc)
    context["ti"].xcom_push(key="debezium_health", value=result)
    return result


def _compile_health_report(**context) -> None:
    ti      = context["ti"]
    checks  = {
        "kafka":      ti.xcom_pull(key="kafka_health",      task_ids="check_kafka_health")     or {},
        "minio":      ti.xcom_pull(key="minio_health",      task_ids="check_minio_health")     or {},
        "clickhouse": ti.xcom_pull(key="clickhouse_health", task_ids="check_clickhouse_health") or {},
        "spark":      ti.xcom_pull(key="spark_health",      task_ids="check_spark_health")     or {},
        "debezium":   ti.xcom_pull(key="debezium_health",   task_ids="check_debezium_health")  or {},
    }

    down_services = [svc for svc, r in checks.items() if r.get("status") == "DOWN"]
    warn_services = [svc for svc, r in checks.items() if r.get("status") == "WARN"]

    summary_lines = [f"• {svc}: {r.get('status','?')} — {r.get('message','')}" for svc, r in checks.items()]
    summary = "\n".join(summary_lines)
    logger.info("Infrastructure health report:\n%s", summary)

    if down_services:
        send_pipeline_alert(
            "infra_health_check", "compile_health_report",
            f"Services DOWN: {', '.join(down_services)}\n{summary}",
            severity="ERROR",
        )

    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, dbname=PG_DB,
        )
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS infra_health_reports (
                id          SERIAL PRIMARY KEY,
                check_time  TIMESTAMP DEFAULT NOW(),
                kafka_status      VARCHAR(10),
                minio_status      VARCHAR(10),
                clickhouse_status VARCHAR(10),
                spark_status      VARCHAR(10),
                debezium_status   VARCHAR(10)
            )
        """)
        cur.execute("""
            INSERT INTO infra_health_reports
                (kafka_status, minio_status, clickhouse_status, spark_status, debezium_status)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            checks["kafka"].get("status"),
            checks["minio"].get("status"),
            checks["clickhouse"].get("status"),
            checks["spark"].get("status"),
            checks["debezium"].get("status"),
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as exc:
        logger.error("Failed to write health report: %s", exc)


with DAG(
    dag_id="infra_health_check",
    description="Every 15 min: health check for Kafka, MinIO, ClickHouse, Spark, Debezium",
    schedule_interval="*/15 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["infra", "monitoring"],
    doc_md="Parallel health checks for all platform components; alerts Slack on DOWN status.",
) as dag:

    check_kafka = PythonOperator(
        task_id="check_kafka_health",
        python_callable=_check_kafka,
        sla=timedelta(minutes=3),
    )

    check_minio = PythonOperator(
        task_id="check_minio_health",
        python_callable=_check_minio,
        sla=timedelta(minutes=3),
    )

    check_clickhouse = PythonOperator(
        task_id="check_clickhouse_health",
        python_callable=_check_clickhouse,
        sla=timedelta(minutes=3),
    )

    check_spark = PythonOperator(
        task_id="check_spark_health",
        python_callable=_check_spark,
        sla=timedelta(minutes=3),
    )

    check_debezium = PythonOperator(
        task_id="check_debezium_health",
        python_callable=_check_debezium,
        sla=timedelta(minutes=3),
    )

    compile_report = PythonOperator(
        task_id="compile_health_report",
        python_callable=_compile_health_report,
        trigger_rule="all_done",
    )

    [check_kafka, check_minio, check_clickhouse, check_spark, check_debezium] >> compile_report

import json
import logging
import os
import sys
import statistics
from datetime import datetime, timedelta

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
CH_USER        = os.getenv("CLICKHOUSE_USER",          "streamflow")
CH_PASS        = os.getenv("CLICKHOUSE_PASSWORD",      "streamflow123")
CH_DB          = os.getenv("CLICKHOUSE_DB",            "analytics")
PG_HOST        = os.getenv("POSTGRES_HOST",            "postgres")
PG_PORT        = os.getenv("POSTGRES_PORT",            "5432")
PG_USER        = os.getenv("POSTGRES_USER",            "streamflow")
PG_PASS        = os.getenv("POSTGRES_PASSWORD",        "streamflow123")
PG_DB          = os.getenv("POSTGRES_SOURCE_DB",       "streamflow_source")
MINIO_EP       = os.getenv("MINIO_ENDPOINT",           "http://minio:9000")
AWS_KEY        = os.getenv("AWS_ACCESS_KEY_ID",        "streamflow")
AWS_SECRET     = os.getenv("AWS_SECRET_ACCESS_KEY",    "streamflow123")

DEFAULT_ARGS = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "email_on_failure": False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "on_failure_callback": on_failure_callback,
}


def _write_quality_report(pg_conn, records: list) -> None:
    cur = pg_conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS data_quality_reports (
            id          SERIAL PRIMARY KEY,
            check_time  TIMESTAMP DEFAULT NOW(),
            check_name  VARCHAR(100),
            status      VARCHAR(10),
            metric_value DOUBLE PRECISION,
            threshold   DOUBLE PRECISION,
            message     TEXT
        )
    """)
    cur.executemany("""
        INSERT INTO data_quality_reports (check_name, status, metric_value, threshold, message)
        VALUES (%s, %s, %s, %s, %s)
    """, [(r["name"], r["status"], r.get("value"), r.get("threshold"), r.get("message", "")) for r in records])
    pg_conn.commit()
    cur.close()


def _check_kafka_lag(**context) -> dict:
    result = {"name": "kafka_consumer_lag", "status": "PASS", "value": 0}
    try:
        from confluent_kafka.admin import AdminClient
        from confluent_kafka import Consumer

        admin = AdminClient({"bootstrap.servers": KAFKA_SERVERS})
        groups = ["spark-orders-bronze", "spark-pageviews-bronze"]
        total_lag = 0

        for group in groups:
            consumer = Consumer({
                "bootstrap.servers": KAFKA_SERVERS,
                "group.id":          group,
            })
            topics    = consumer.list_topics(timeout=10)
            for topic_name, topic_meta in topics.topics.items():
                if "orders" in topic_name or "pageviews" in topic_name:
                    for partition_id in topic_meta.partitions:
                        from confluent_kafka import TopicPartition
                        tp = TopicPartition(topic_name, partition_id)
                        committed = consumer.committed([tp], timeout=10)
                        _, hi_offset = consumer.get_watermark_offsets(tp, timeout=10)
                        lag = max(0, hi_offset - (committed[0].offset or 0))
                        total_lag += lag
            consumer.close()

        result["value"] = total_lag
        if total_lag > 500_000:
            result["status"] = "FAIL"
            result["message"] = f"Critical lag: {total_lag:,} messages"
            send_pipeline_alert("data_quality_monitor", "check_kafka_lag",
                                f"Consumer lag is {total_lag:,} messages", severity="ERROR")
        elif total_lag > 100_000:
            result["status"] = "WARN"
            result["message"] = f"High lag: {total_lag:,} messages"
            logger.warning("Kafka lag WARNING: %d", total_lag)
        else:
            result["message"] = f"Lag OK: {total_lag:,} messages"

    except Exception as exc:
        result["status"] = "WARN"
        result["message"] = f"Lag check failed: {exc}"
        logger.error("Kafka lag check failed: %s", exc)

    context["ti"].xcom_push(key="kafka_lag", value=result)
    return result


def _check_bronze_row_counts(**context) -> dict:
    result = {"name": "bronze_row_counts", "status": "PASS", "value": 0}
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
        cutoff = datetime.utcnow() - timedelta(hours=1)
        fresh_count = 0
        for prefix in ("orders/", "pageviews/"):
            resp = s3.list_objects_v2(Bucket="bronze", Prefix=prefix)
            fresh_count += sum(
                1 for o in resp.get("Contents", [])
                if o["LastModified"].replace(tzinfo=None) >= cutoff
            )

        result["value"] = fresh_count
        if fresh_count == 0:
            result["status"] = "FAIL"
            result["message"] = "No new Bronze files in last hour"
            send_pipeline_alert("data_quality_monitor", "check_bronze_row_counts",
                                "Zero new Bronze files in last hour", severity="ERROR")
        else:
            result["message"] = f"{fresh_count} fresh files found"
    except Exception as exc:
        result["status"] = "WARN"
        result["message"] = str(exc)

    context["ti"].xcom_push(key="bronze_counts", value=result)
    return result


def _check_silver_freshness(**context) -> dict:
    result = {"name": "silver_freshness", "status": "PASS", "value": 0}
    current_hour = datetime.utcnow().hour + 7
    business_hours = 7 <= (current_hour % 24) <= 22

    try:
        resp = requests.get(
            f"http://{CH_HOST}:{CH_PORT}/",
            params={
                "query": "SELECT max(ingestion_timestamp) FROM analytics.orders_realtime FORMAT JSON",
                "user":  CH_USER,
                "password": CH_PASS,
            },
            timeout=15,
        )
        data = resp.json()
        rows = data.get("data", [{}])
        max_ts_str = list(rows[0].values())[0] if rows else None
        if max_ts_str:
            max_ts = datetime.strptime(max_ts_str[:19], "%Y-%m-%d %H:%M:%S")
            age_hours = (datetime.utcnow() - max_ts).total_seconds() / 3600
            result["value"] = round(age_hours, 2)
            threshold = 3.0 if business_hours else 6.0
            result["threshold"] = threshold
            if age_hours > threshold:
                result["status"] = "FAIL"
                result["message"] = f"Silver data is {age_hours:.1f}h old (threshold: {threshold}h)"
                send_pipeline_alert("data_quality_monitor", "check_silver_freshness",
                                    result["message"], severity="WARN" if not business_hours else "ERROR")
            else:
                result["message"] = f"Silver data age: {age_hours:.1f}h — OK"
        else:
            result["status"] = "WARN"
            result["message"] = "No silver data found"
    except Exception as exc:
        result["status"] = "WARN"
        result["message"] = str(exc)

    context["ti"].xcom_push(key="silver_freshness", value=result)
    return result


def _check_null_rates(**context) -> dict:
    result = {"name": "null_rates", "status": "PASS", "value": 0}
    try:
        resp = requests.get(
            f"http://{CH_HOST}:{CH_PORT}/",
            params={
                "query": (
                    "SELECT countIf(user_id IS NULL OR user_id = '') / count() * 100 as null_uid_pct, "
                    "countIf(total_amount IS NULL OR total_amount <= 0) / count() * 100 as invalid_amt_pct "
                    "FROM analytics.orders_realtime "
                    "WHERE event_timestamp > now() - INTERVAL 1 HOUR FORMAT JSON"
                ),
                "user": CH_USER, "password": CH_PASS,
            },
            timeout=15,
        )
        row = resp.json().get("data", [{}])[0]
        null_uid_pct = float(row.get("null_uid_pct", 0))
        result["value"] = null_uid_pct
        result["threshold"] = 5.0
        if null_uid_pct > 5.0:
            result["status"] = "FAIL"
            result["message"] = f"NULL user_id rate: {null_uid_pct:.1f}% > 5% threshold"
            send_pipeline_alert("data_quality_monitor", "check_null_rates",
                                result["message"], severity="ERROR")
        else:
            result["message"] = f"NULL rate OK: {null_uid_pct:.2f}%"
    except Exception as exc:
        result["status"] = "WARN"
        result["message"] = str(exc)

    context["ti"].xcom_push(key="null_rates", value=result)
    return result


def _check_revenue_anomaly(**context) -> dict:
    result = {"name": "revenue_anomaly", "status": "PASS", "value": 0}
    try:
        current_hour = datetime.utcnow().hour

        resp_now = requests.get(
            f"http://{CH_HOST}:{CH_PORT}/",
            params={
                "query": (
                    "SELECT sum(total_amount) as rev "
                    "FROM analytics.orders_realtime "
                    "WHERE event_timestamp > now() - INTERVAL 1 HOUR FORMAT JSON"
                ),
                "user": CH_USER, "password": CH_PASS,
            }, timeout=15,
        )
        current_rev = float(resp_now.json().get("data", [{"rev": 0}])[0].get("rev", 0))

        resp_hist = requests.get(
            f"http://{CH_HOST}:{CH_PORT}/",
            params={
                "query": (
                    f"SELECT sum(total_amount) as rev "
                    f"FROM analytics.orders_realtime "
                    f"WHERE toHour(event_timestamp) = {current_hour} "
                    f"AND event_timestamp < now() - INTERVAL 1 DAY "
                    f"AND event_timestamp > now() - INTERVAL 8 DAY "
                    f"GROUP BY toDate(event_timestamp) FORMAT JSON"
                ),
                "user": CH_USER, "password": CH_PASS,
            }, timeout=15,
        )
        hist_rows = resp_hist.json().get("data", [])
        hist_vals = [float(r.get("rev", 0)) for r in hist_rows if float(r.get("rev", 0)) > 0]

        if len(hist_vals) >= 3:
            mean   = statistics.mean(hist_vals)
            stdev  = statistics.stdev(hist_vals)
            z      = abs(current_rev - mean) / max(stdev, 1)
            result["value"] = round(z, 2)
            result["threshold"] = 3.0
            if z > 3.0:
                result["status"] = "WARN"
                result["message"] = f"Revenue Z-score={z:.2f} — possible anomaly (current={current_rev:.0f}, mean={mean:.0f})"
                send_pipeline_alert("data_quality_monitor", "check_revenue_anomaly",
                                    result["message"], severity="WARN")
            else:
                result["message"] = f"Revenue normal (Z={z:.2f})"
        else:
            result["message"] = "Insufficient historical data for anomaly detection"
    except Exception as exc:
        result["status"] = "WARN"
        result["message"] = str(exc)

    context["ti"].xcom_push(key="revenue_anomaly", value=result)
    return result


def _compile_quality_report(**context) -> None:
    ti = context["ti"]
    checks = [
        ti.xcom_pull(key="kafka_lag",       task_ids="check_kafka_lag")      or {},
        ti.xcom_pull(key="bronze_counts",   task_ids="check_bronze_row_counts") or {},
        ti.xcom_pull(key="silver_freshness",task_ids="check_silver_freshness") or {},
        ti.xcom_pull(key="null_rates",      task_ids="check_null_rates")     or {},
        ti.xcom_pull(key="revenue_anomaly", task_ids="check_revenue_anomaly") or {},
    ]

    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, user=PG_USER,
            password=PG_PASS, dbname=PG_DB,
        )
        _write_quality_report(conn, [c for c in checks if c])
        conn.close()
        logger.info("Quality report written for %d checks", len(checks))
    except Exception as exc:
        logger.error("Failed to write quality report: %s", exc)


with DAG(
    dag_id="data_quality_monitor",
    description="Every 30 min: Kafka lag, Bronze counts, Silver freshness, null rates, revenue anomaly",
    schedule_interval="*/30 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["quality", "monitoring"],
    doc_md="Continuous DQ monitoring — writes results to data_quality_reports PostgreSQL table.",
) as dag:

    check_kafka_lag = PythonOperator(
        task_id="check_kafka_lag",
        python_callable=_check_kafka_lag,
        sla=timedelta(minutes=5),
    )

    check_bronze_row_counts = PythonOperator(
        task_id="check_bronze_row_counts",
        python_callable=_check_bronze_row_counts,
        sla=timedelta(minutes=5),
    )

    check_silver_freshness = PythonOperator(
        task_id="check_silver_freshness",
        python_callable=_check_silver_freshness,
        sla=timedelta(minutes=5),
    )

    check_null_rates = PythonOperator(
        task_id="check_null_rates",
        python_callable=_check_null_rates,
        sla=timedelta(minutes=5),
    )

    check_revenue_anomaly = PythonOperator(
        task_id="check_revenue_anomaly",
        python_callable=_check_revenue_anomaly,
        sla=timedelta(minutes=5),
    )

    compile_quality_report = PythonOperator(
        task_id="compile_quality_report",
        python_callable=_compile_quality_report,
        trigger_rule="all_done",
    )

    [
        check_kafka_lag,
        check_bronze_row_counts,
        check_silver_freshness,
        check_null_rates,
        check_revenue_anomaly,
    ] >> compile_quality_report

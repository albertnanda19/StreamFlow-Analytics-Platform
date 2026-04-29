import logging
import os
import sys
import time

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

AIRFLOW_BASE_URL  = os.getenv("AIRFLOW_BASE_URL",  "http://localhost:8088")
AIRFLOW_USER      = os.getenv("AIRFLOW_ADMIN_USER", "admin")
AIRFLOW_PASSWORD  = os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin123")

POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "postgres")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT",     "5432")
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "streamflow")
POSTGRES_PASS     = os.getenv("POSTGRES_PASSWORD", "streamflow123")
POSTGRES_DB       = os.getenv("POSTGRES_SOURCE_DB","streamflow_source")

CLICKHOUSE_HOST   = os.getenv("CLICKHOUSE_HOST",   "clickhouse")
CLICKHOUSE_PORT   = os.getenv("CLICKHOUSE_PORT",   "8123")
CLICKHOUSE_USER   = os.getenv("CLICKHOUSE_USER",   "streamflow")
CLICKHOUSE_PASS   = os.getenv("CLICKHOUSE_PASSWORD","streamflow123")

MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT",    "http://minio:9000")
AWS_KEY           = os.getenv("AWS_ACCESS_KEY_ID", "streamflow")
AWS_SECRET        = os.getenv("AWS_SECRET_ACCESS_KEY","streamflow123")

SLACK_WEBHOOK     = os.getenv("SLACK_WEBHOOK_URL",  "")

CONNECTIONS = [
    {
        "connection_id": "postgres_streamflow",
        "conn_type":     "postgres",
        "host":          POSTGRES_HOST,
        "port":          int(POSTGRES_PORT),
        "schema":        POSTGRES_DB,
        "login":         POSTGRES_USER,
        "password":      POSTGRES_PASS,
        "description":   "StreamFlow PostgreSQL source database",
    },
    {
        "connection_id": "clickhouse_http",
        "conn_type":     "http",
        "host":          CLICKHOUSE_HOST,
        "port":          int(CLICKHOUSE_PORT),
        "login":         CLICKHOUSE_USER,
        "password":      CLICKHOUSE_PASS,
        "description":   "ClickHouse HTTP interface",
    },
    {
        "connection_id": "spark_default",
        "conn_type":     "spark",
        "host":          "spark://spark-master",
        "port":          7077,
        "description":   "Spark cluster master",
    },
    {
        "connection_id": "s3_minio",
        "conn_type":     "aws",
        "host":          MINIO_ENDPOINT,
        "login":         AWS_KEY,
        "password":      AWS_SECRET,
        "extra":         (
            '{"aws_access_key_id": "' + AWS_KEY + '", '
            '"aws_secret_access_key": "' + AWS_SECRET + '", '
            '"endpoint_url": "' + MINIO_ENDPOINT + '", '
            '"region_name": "us-east-1"}'
        ),
        "description":   "MinIO S3-compatible object storage",
    },
    {
        "connection_id": "slack_alerts",
        "conn_type":     "http",
        "host":          SLACK_WEBHOOK,
        "description":   "Slack webhook for pipeline alerts",
    },
]


def _wait_for_airflow(timeout: int = 120) -> bool:
    deadline = time.monotonic() + timeout
    url = f"{AIRFLOW_BASE_URL}/health"
    while time.monotonic() < deadline:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        logger.info("Waiting for Airflow webserver...")
        time.sleep(5)
    return False


def _upsert_connection(session: requests.Session, conn: dict) -> None:
    conn_id = conn["connection_id"]
    check_url = f"{AIRFLOW_BASE_URL}/api/v1/connections/{conn_id}"
    r = session.get(check_url)

    if r.status_code == 200:
        r2 = session.patch(check_url, json=conn)
        action = "updated"
    else:
        r2 = session.post(f"{AIRFLOW_BASE_URL}/api/v1/connections", json=conn)
        action = "created"

    if r2.status_code in (200, 201):
        logger.info("Connection %-30s %s", conn_id, action)
    else:
        logger.error("Failed to upsert %s: %s %s", conn_id, r2.status_code, r2.text)


def main() -> None:
    if not _wait_for_airflow():
        logger.error("Airflow is not available — aborting")
        sys.exit(1)

    session = requests.Session()
    session.auth = (AIRFLOW_USER, AIRFLOW_PASSWORD)
    session.headers["Content-Type"] = "application/json"

    for conn in CONNECTIONS:
        _upsert_connection(session, conn)

    logger.info("All connections configured.")


if __name__ == "__main__":
    main()

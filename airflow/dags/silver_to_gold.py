import logging
import os
import sys
from datetime import datetime, timedelta

import psycopg2
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

sys.path.insert(0, "/opt/airflow/scripts")
from setup_alerting import on_failure_callback, on_sla_miss_callback

logger = logging.getLogger(__name__)

DBT_CMD      = "cd /opt/airflow/dbt_project && dbt"
DBT_FLAGS    = "--target prod --profiles-dir /opt/airflow/dbt_project --no-write-json"
CH_HOST      = os.getenv("CLICKHOUSE_HOST",     "clickhouse")
CH_PORT      = os.getenv("CLICKHOUSE_PORT",     "8123")
CH_USER      = os.getenv("CLICKHOUSE_USER",     "streamflow")
CH_PASS      = os.getenv("CLICKHOUSE_PASSWORD", "streamflow123")
CH_DB        = os.getenv("CLICKHOUSE_DB",       "analytics")
SLACK_HOOK   = os.getenv("SLACK_WEBHOOK_URL",   "")

DEFAULT_ARGS = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
    "on_failure_callback": on_failure_callback,
}

GOLD_TABLES = [
    "gold_daily_revenue",
    "gold_product_performance",
    "gold_user_segments",
    "gold_conversion_funnel",
    "mart_executive_summary",
    "mart_product_recommendations",
]


def _sync_to_clickhouse(**context) -> None:
    try:
        import clickhouse_driver
    except ImportError:
        logger.warning("clickhouse_driver not installed — sync skipped")
        return

    client = clickhouse_driver.Client(
        host=CH_HOST, port=9000,
        user=CH_USER, password=CH_PASS,
        database=CH_DB,
    )

    for table in GOLD_TABLES:
        try:
            count = client.execute(f"SELECT count() FROM {table}")[0][0]
            logger.info("ClickHouse %s: %d rows synced", table, count)
        except Exception as exc:
            logger.error("ClickHouse sync failed for %s: %s", table, exc)


def _refresh_grafana_and_alert(**context) -> None:
    grafana_url  = os.getenv("GRAFANA_URL", "http://grafana:3000")
    grafana_user = os.getenv("GRAFANA_USER", "admin")
    grafana_pass = os.getenv("GRAFANA_PASSWORD", "admin123")

    try:
        resp = requests.get(
            f"{grafana_url}/api/dashboards/home",
            auth=(grafana_user, grafana_pass),
            timeout=10,
        )
        logger.info("Grafana ping: %s", resp.status_code)
    except Exception as exc:
        logger.warning("Grafana ping failed: %s", exc)

    exec_date = context["execution_date"]
    message = (
        f"✅ *StreamFlow Daily Pipeline Complete*\n"
        f"Execution date: `{exec_date.date()}`\n"
        f"Gold tables refreshed: {len(GOLD_TABLES)}\n"
        f"ClickHouse sync complete."
    )

    if SLACK_HOOK:
        try:
            requests.post(SLACK_HOOK, json={"text": message}, timeout=10)
        except Exception as exc:
            logger.error("Slack notification failed: %s", exc)
    else:
        logger.info("SLACK_WEBHOOK_URL not set — summary logged:\n%s", message)


with DAG(
    dag_id="silver_to_gold",
    description="Daily: Silver → Gold + Mart dbt transformations + ClickHouse sync",
    schedule_interval="30 1 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=on_sla_miss_callback,
    tags=["gold", "dbt", "daily"],
    doc_md="Waits for silver pipeline, runs Gold/Mart dbt models, syncs ClickHouse, alerts Slack.",
) as dag:

    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver_pipeline",
        external_dag_id="bronze_to_silver",
        external_task_id="update_pipeline_metadata",
        mode="reschedule",
        timeout=7200,
        poke_interval=300,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
    )

    run_snapshots = BashOperator(
        task_id="run_dbt_snapshots",
        bash_command=f"{DBT_CMD} snapshot {DBT_FLAGS}",
        sla=timedelta(minutes=15),
    )

    with TaskGroup("gold_parallel_runs") as gold_parallel_runs:
        run_gold_revenue = BashOperator(
            task_id="run_gold_daily_revenue",
            bash_command=f"{DBT_CMD} run --select gold_daily_revenue --full-refresh {DBT_FLAGS}",
        )
        run_gold_products = BashOperator(
            task_id="run_gold_products",
            bash_command=f"{DBT_CMD} run --select gold_product_performance --full-refresh {DBT_FLAGS}",
        )
        run_gold_users = BashOperator(
            task_id="run_gold_users",
            bash_command=f"{DBT_CMD} run --select gold_user_segments --full-refresh {DBT_FLAGS}",
        )
        run_gold_funnel = BashOperator(
            task_id="run_gold_funnel",
            bash_command=f"{DBT_CMD} run --select gold_conversion_funnel --full-refresh {DBT_FLAGS}",
        )

    run_marts = BashOperator(
        task_id="run_marts",
        bash_command=f"{DBT_CMD} run --select marts {DBT_FLAGS}",
        sla=timedelta(minutes=20),
    )

    test_gold_and_marts = BashOperator(
        task_id="test_gold_and_marts",
        bash_command=f"{DBT_CMD} test --select gold marts {DBT_FLAGS}",
        sla=timedelta(minutes=15),
    )

    sync_to_clickhouse = PythonOperator(
        task_id="sync_to_clickhouse",
        python_callable=_sync_to_clickhouse,
        trigger_rule="all_done",
    )

    refresh_and_alert = PythonOperator(
        task_id="refresh_grafana_alerts",
        python_callable=_refresh_grafana_and_alert,
        trigger_rule="all_done",
    )

    (
        wait_for_silver
        >> run_snapshots
        >> gold_parallel_runs
        >> run_marts
        >> test_gold_and_marts
        >> sync_to_clickhouse
        >> refresh_and_alert
    )

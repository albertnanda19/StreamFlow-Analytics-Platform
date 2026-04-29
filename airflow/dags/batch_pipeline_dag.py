from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "streamflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="streamflow_batch_pipeline",
    default_args=default_args,
    description="Daily batch transformation pipeline: Silver → Gold via dbt",
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["streamflow", "batch", "dbt"],
    max_active_runs=1,
) as dag:

    run_dbt_silver = BashOperator(
        task_id="run_dbt_silver",
        bash_command=(
            "dbt run --select silver "
            "--profiles-dir /opt/airflow/dbt_project "
            "--project-dir /opt/airflow/dbt_project"
        ),
    )

    test_dbt_silver = BashOperator(
        task_id="test_dbt_silver",
        bash_command=(
            "dbt test --select silver "
            "--profiles-dir /opt/airflow/dbt_project "
            "--project-dir /opt/airflow/dbt_project"
        ),
    )

    run_dbt_gold = BashOperator(
        task_id="run_dbt_gold",
        bash_command=(
            "dbt run --select gold "
            "--profiles-dir /opt/airflow/dbt_project "
            "--project-dir /opt/airflow/dbt_project"
        ),
    )

    test_dbt_gold = BashOperator(
        task_id="test_dbt_gold",
        bash_command=(
            "dbt test --select gold "
            "--profiles-dir /opt/airflow/dbt_project "
            "--project-dir /opt/airflow/dbt_project"
        ),
    )

    run_dbt_silver >> test_dbt_silver >> run_dbt_gold >> test_dbt_gold

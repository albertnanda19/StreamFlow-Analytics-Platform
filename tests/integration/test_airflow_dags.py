import time
import pytest


class TestDAGIntegrity:

    @pytest.mark.integration
    @pytest.mark.airflow
    @pytest.mark.parametrize("dag_id", [
        "bronze_to_silver",
        "silver_to_gold",
        "data_quality_monitor",
        "historical_backfill",
        "infra_health_check",
    ])
    def test_dag_exists_and_is_not_paused(self, airflow_api_client, dag_id):
        resp = airflow_api_client.get(
            f"http://localhost:8088/api/v1/dags/{dag_id}",
            timeout=15,
        )
        assert resp.status_code == 200, f"DAG '{dag_id}' not found: {resp.text}"
        dag_data = resp.json()
        assert not dag_data["is_paused"], f"DAG '{dag_id}' is paused — unpause it first"

    @pytest.mark.integration
    @pytest.mark.airflow
    def test_airflow_webserver_is_healthy(self, airflow_api_client):
        resp = airflow_api_client.get("http://localhost:8088/api/v1/health", timeout=10)
        assert resp.status_code == 200
        body = resp.json()
        assert body["metadatabase"]["status"] == "healthy", (
            f"Airflow metadatabase status: {body['metadatabase']['status']}"
        )

    @pytest.mark.integration
    @pytest.mark.airflow
    def test_airflow_scheduler_is_running(self, airflow_api_client):
        resp = airflow_api_client.get("http://localhost:8088/api/v1/health", timeout=10)
        assert resp.status_code == 200
        body = resp.json()
        scheduler_status = body.get("scheduler", {}).get("status", "unknown")
        assert scheduler_status == "healthy", (
            f"Airflow scheduler status: {scheduler_status}"
        )

    @pytest.mark.integration
    @pytest.mark.airflow
    def test_all_dags_have_at_least_one_task(self, airflow_api_client):
        dag_ids = [
            "bronze_to_silver", "silver_to_gold",
            "data_quality_monitor", "infra_health_check",
        ]
        for dag_id in dag_ids:
            resp = airflow_api_client.get(
                f"http://localhost:8088/api/v1/dags/{dag_id}/tasks",
                timeout=15,
            )
            if resp.status_code == 200:
                task_count = len(resp.json().get("tasks", []))
                assert task_count >= 1, f"DAG '{dag_id}' has no tasks"

    @pytest.mark.integration
    @pytest.mark.airflow
    def test_airflow_connections_are_configured(self, airflow_api_client):
        required_conn_ids = [
            "postgres_streamflow",
            "clickhouse_default",
        ]
        resp = airflow_api_client.get(
            "http://localhost:8088/api/v1/connections",
            timeout=15,
        )
        if resp.status_code != 200:
            pytest.skip(f"Cannot list connections: {resp.status_code}")
        existing = {c["connection_id"] for c in resp.json().get("connections", [])}
        for conn_id in required_conn_ids:
            assert conn_id in existing, (
                f"Airflow connection '{conn_id}' not configured"
            )


class TestDAGExecution:

    @pytest.mark.integration
    @pytest.mark.airflow
    @pytest.mark.slow
    def test_infra_health_check_dag_completes_successfully(self, airflow_api_client):
        trigger_resp = airflow_api_client.post(
            "http://localhost:8088/api/v1/dags/infra_health_check/dagRuns",
            json={"conf": {}},
            timeout=15,
        )
        if trigger_resp.status_code not in (200, 409):
            pytest.skip(f"Cannot trigger DAG: {trigger_resp.status_code} — {trigger_resp.text}")

        if trigger_resp.status_code == 409:
            pytest.skip("DAG is already running — skip to avoid conflict")

        run_id = trigger_resp.json()["dag_run_id"]
        timeout_seconds = 300
        start = time.time()
        final_state = None

        while time.time() - start < timeout_seconds:
            status_resp = airflow_api_client.get(
                f"http://localhost:8088/api/v1/dags/infra_health_check/dagRuns/{run_id}",
                timeout=10,
            )
            state = status_resp.json()["state"]
            if state in ("success", "failed"):
                final_state = state
                break
            time.sleep(15)

        assert final_state == "success", (
            f"infra_health_check DAG ended with state: {final_state}"
        )

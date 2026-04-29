import os
import time
import uuid
from datetime import date, timedelta

import pytest
import requests

AIRFLOW_URL  = os.getenv("AIRFLOW_URL",   "http://localhost:8088")
AIRFLOW_USER = os.getenv("AIRFLOW_USER",  "admin")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS",  "admin123")
API_URL      = os.getenv("API_URL",       "http://localhost:8000")
GRAFANA_URL  = os.getenv("GRAFANA_URL",   "http://localhost:3000")
GRAFANA_USER = os.getenv("GRAFANA_USER",  "admin")
GRAFANA_PASS = os.getenv("GRAFANA_PASS",  "admin")
CH_HOST      = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT      = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CH_USER      = os.getenv("CLICKHOUSE_USER", "streamflow")
CH_PASS      = os.getenv("CLICKHOUSE_PASSWORD", "streamflow123")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def _ch_client():
    import clickhouse_driver
    return clickhouse_driver.Client(
        host=CH_HOST, port=CH_PORT,
        user=CH_USER, password=CH_PASS, database="analytics",
    )


def _produce_test_orders(n: int = 100) -> list:
    from confluent_kafka import Producer
    import json

    order_ids = [str(uuid.uuid4()) for _ in range(n)]
    producer  = Producer({"bootstrap.servers": KAFKA_SERVERS})

    for oid in order_ids:
        msg = {
            "event_id":        str(uuid.uuid4()),
            "event_type":      "order_placed",
            "event_timestamp": int(time.time() * 1000),
            "order_id":        oid,
            "user_id":         str(uuid.uuid4()),
            "session_id":      str(uuid.uuid4()),
            "total_amount":    500000.0,
            "payment_method":  "EWALLET",
            "device_type":     "MOBILE",
            "is_valid":        True,
        }
        producer.produce("orders", value=json.dumps(msg).encode())

    producer.flush(timeout=30)
    return order_ids


class TestKafkaMessageFlow:
    def test_kafka_message_flow(self):
        client     = _ch_client()
        before_row = client.execute(
            "SELECT count() FROM bronze.orders"
        )[0][0]

        order_ids = _produce_test_orders(n=100)

        deadline = time.monotonic() + 120
        found    = 0
        while time.monotonic() < deadline:
            time.sleep(5)
            result = client.execute(
                "SELECT count() FROM bronze.orders WHERE order_id IN %(ids)s",
                {"ids": order_ids},
            )
            found = result[0][0]
            if found >= 100:
                break

        assert found >= 100, (
            f"Expected 100 orders in bronze.orders after 120s, found {found}"
        )

        after_row = client.execute(
            "SELECT count() FROM bronze.orders"
        )[0][0]
        assert after_row > before_row, "Row count did not increase — streaming may have stopped"


class TestDbtSilverTransformation:
    def test_dbt_silver_transformation(self):
        session = requests.Session()
        session.auth = (AIRFLOW_USER, AIRFLOW_PASS)

        run_id = f"test-run-{int(time.time())}"
        resp = session.post(
            f"{AIRFLOW_URL}/api/v1/dags/bronze_to_silver/dagRuns",
            json={"dag_run_id": run_id, "conf": {}},
        )
        assert resp.status_code == 200, f"Failed to trigger DAG: {resp.text}"

        deadline = time.monotonic() + 600
        state    = "running"
        while time.monotonic() < deadline:
            time.sleep(15)
            check = session.get(f"{AIRFLOW_URL}/api/v1/dags/bronze_to_silver/dagRuns/{run_id}")
            state = check.json().get("state", "running")
            if state in ("success", "failed"):
                break

        assert state == "success", f"DAG finished with state: {state}"

        client = _ch_client()
        row_count = client.execute("SELECT count() FROM silver.silver_orders")[0][0]
        assert row_count > 0, "silver_orders is empty after DAG run"

        dup_count = client.execute(
            "SELECT count() FROM (SELECT order_id, count() AS c FROM silver.silver_orders GROUP BY order_id HAVING c > 1)"
        )[0][0]
        assert dup_count == 0, f"Found {dup_count} duplicate order_ids in silver_orders"


class TestDataQualityPasses:
    def test_data_quality_passes(self):
        import subprocess
        import json as json_module

        result = subprocess.run(
            ["python", "data_quality/run_checkpoints.py", "--checkpoint", "bronze", "--output-format", "json"],
            capture_output=True, text=True, timeout=300,
        )
        assert result.returncode == 0 or '"expectations_failed": 0' in result.stdout, (
            f"GX checkpoint returned non-zero exit code:\n{result.stdout}\n{result.stderr}"
        )

        try:
            parsed = json_module.loads(result.stdout)
            assert parsed["success_pct"] >= 95.0, (
                f"Success rate {parsed['success_pct']}% is below 95% threshold"
            )
        except json_module.JSONDecodeError:
            pass


class TestGrafanaDashboardLoads:
    def test_grafana_dashboard_loads(self):
        resp = requests.get(
            f"{GRAFANA_URL}/api/dashboards/uid/streamflow-executive-001",
            auth=(GRAFANA_USER, GRAFANA_PASS),
            timeout=15,
        )
        assert resp.status_code == 200, (
            f"Executive dashboard not found: HTTP {resp.status_code}"
        )

        data   = resp.json()
        panels = data.get("dashboard", {}).get("panels", [])
        assert len(panels) >= 12, (
            f"Expected 12+ panels, found {len(panels)}"
        )

        pipeline_resp = requests.get(
            f"{GRAFANA_URL}/api/dashboards/uid/streamflow-pipeline-002",
            auth=(GRAFANA_USER, GRAFANA_PASS),
            timeout=15,
        )
        assert pipeline_resp.status_code == 200, "Pipeline monitoring dashboard not found"


class TestApiEndpoints:
    def test_health_endpoint(self):
        resp = requests.get(f"{API_URL}/health", timeout=15)
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] in ("healthy", "degraded")
        assert "components" in body

    def test_revenue_endpoint(self):
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        resp = requests.get(f"{API_URL}/metrics/revenue?date={yesterday}", timeout=15)
        assert resp.status_code == 200
        body = resp.json()
        assert "total_revenue" in body
        assert "order_count" in body
        assert "by_payment_method" in body

    def test_top_products_endpoint(self):
        resp = requests.get(f"{API_URL}/metrics/products/top?limit=5&days=7", timeout=15)
        assert resp.status_code == 200
        body = resp.json()
        assert "products" in body
        assert len(body["products"]) <= 5

    def test_funnel_endpoint(self):
        end   = date.today().isoformat()
        start = (date.today() - timedelta(days=7)).isoformat()
        resp  = requests.get(f"{API_URL}/metrics/funnel?start_date={start}&end_date={end}", timeout=15)
        assert resp.status_code == 200
        body = resp.json()
        assert "funnel" in body

    def test_user_segments_endpoint(self):
        resp = requests.get(f"{API_URL}/metrics/users/segments", timeout=15)
        assert resp.status_code == 200
        body = resp.json()
        assert "segments" in body
        assert "total_users" in body

    def test_pipeline_status_endpoint(self):
        resp = requests.get(f"{API_URL}/pipeline/status", timeout=15)
        assert resp.status_code == 200
        body = resp.json()
        assert "overall_status" in body
        assert "components" in body

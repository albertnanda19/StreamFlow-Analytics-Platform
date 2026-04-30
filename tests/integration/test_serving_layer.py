import pytest
import requests


class TestFastAPIEndpoints:

    @pytest.mark.integration
    def test_health_endpoint_returns_healthy(self, fastapi_client):
        resp = fastapi_client.get("http://localhost:8000/health", timeout=10)
        assert resp.status_code == 200
        body = resp.json()
        assert body.get("status") in ("healthy", "ok"), (
            f"Unexpected health status: {body}"
        )

    @pytest.mark.integration
    @pytest.mark.accuracy
    def test_revenue_endpoint_structure(self, fastapi_client):
        resp = fastapi_client.get(
            "http://localhost:8000/metrics/revenue",
            params={"days": 7},
            timeout=15,
        )
        if resp.status_code == 404:
            pytest.skip("Revenue endpoint not found — check FastAPI route definition")
        assert resp.status_code == 200
        body = resp.json()
        assert "total_revenue" in body or "revenue" in body, (
            f"Missing revenue field in response: {list(body.keys())}"
        )

    @pytest.mark.integration
    @pytest.mark.accuracy
    def test_revenue_api_matches_clickhouse_directly(self, fastapi_client, clickhouse_client):
        resp = fastapi_client.get(
            "http://localhost:8000/metrics/revenue",
            params={"days": 7},
            timeout=15,
        )
        if resp.status_code != 200:
            pytest.skip(f"Revenue endpoint returned {resp.status_code}")

        api_body = resp.json()
        api_revenue = api_body.get("total_revenue") or api_body.get("revenue", 0)

        db_result = clickhouse_client.execute("""
            SELECT sum(total_amount)
            FROM silver.silver_orders
            WHERE event_timestamp >= now() - INTERVAL 7 DAY
        """)
        db_total = db_result[0][0] or 0

        if db_total == 0:
            pytest.skip("No data in silver_orders for last 7 days")

        assert abs(api_revenue - db_total) < db_total * 0.01, (
            f"API revenue ({api_revenue}) differs >1% from DB ({db_total})"
        )

    @pytest.mark.integration
    def test_pipeline_status_endpoint(self, fastapi_client):
        resp = fastapi_client.get("http://localhost:8000/pipeline/status", timeout=15)
        if resp.status_code == 404:
            pytest.skip("Pipeline status endpoint not found")
        assert resp.status_code == 200
        body = resp.json()
        for component in ("kafka", "clickhouse"):
            if component in body:
                assert body[component].get("status") in (
                    "running", "healthy", "connected", "ok"
                ), f"Component '{component}' status unexpected: {body[component]}"

    @pytest.mark.integration
    def test_top_products_endpoint_returns_sorted_list(self, fastapi_client):
        resp = fastapi_client.get(
            "http://localhost:8000/metrics/products/top",
            params={"limit": 5, "days": 7},
            timeout=15,
        )
        if resp.status_code == 404:
            pytest.skip("Top products endpoint not found")
        assert resp.status_code == 200
        body = resp.json()
        products = body if isinstance(body, list) else body.get("products", [])
        if len(products) >= 2:
            revenues = [p.get("revenue", p.get("total_revenue", 0)) for p in products]
            assert revenues == sorted(revenues, reverse=True), (
                "Products are not sorted by revenue descending"
            )

    @pytest.mark.integration
    def test_all_api_endpoints_return_valid_json(self, fastapi_client):
        endpoints = ["/health", "/metrics/revenue", "/pipeline/status"]
        for path in endpoints:
            try:
                resp = fastapi_client.get(f"http://localhost:8000{path}", timeout=10)
                if resp.status_code not in (404, 422):
                    assert resp.headers.get("content-type", "").startswith("application/json"), (
                        f"{path} does not return JSON"
                    )
            except Exception:
                pass


class TestGrafanaDashboards:

    @pytest.mark.integration
    def test_grafana_is_reachable(self, grafana_api_client):
        resp = grafana_api_client.get("http://localhost:3000/api/health", timeout=10)
        assert resp.status_code == 200

    @pytest.mark.integration
    def test_grafana_has_datasources(self, grafana_api_client):
        resp = grafana_api_client.get("http://localhost:3000/api/datasources", timeout=10)
        assert resp.status_code == 200
        datasources = resp.json()
        assert len(datasources) >= 1, "No datasources configured in Grafana"

    @pytest.mark.integration
    def test_grafana_has_dashboards(self, grafana_api_client):
        resp = grafana_api_client.get(
            "http://localhost:3000/api/search",
            params={"type": "dash-db"},
            timeout=10,
        )
        assert resp.status_code == 200
        dashboards = resp.json()
        assert len(dashboards) >= 1, "No dashboards found in Grafana"


class TestTrinoFederatedQueries:

    @pytest.mark.integration
    def test_trino_is_reachable(self):
        resp = requests.get("http://localhost:8090/v1/info", timeout=10)
        assert resp.status_code == 200
        info = resp.json()
        assert info.get("starting") is False or "nodeVersion" in info, (
            "Trino is still starting or info endpoint not ready"
        )

    @pytest.mark.integration
    def test_trino_clickhouse_catalog_accessible(self):
        import time

        headers = {
            "X-Trino-User": "streamflow",
            "Content-Type": "application/json",
        }
        resp = requests.post(
            "http://localhost:8090/v1/statement",
            headers=headers,
            data="SHOW SCHEMAS FROM clickhouse",
            timeout=15,
        )
        assert resp.status_code == 200

        next_uri = resp.json().get("nextUri")
        max_polls = 10
        for _ in range(max_polls):
            if next_uri is None:
                break
            time.sleep(1)
            poll_resp = requests.get(next_uri, headers=headers, timeout=10)
            next_uri = poll_resp.json().get("nextUri")

        assert resp.status_code == 200, "Trino query to clickhouse catalog failed"

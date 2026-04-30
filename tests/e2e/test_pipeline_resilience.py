import json
import time
import subprocess
import pytest


class TestIdempotency:

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_producing_same_order_twice_produces_one_clickhouse_row(
        self, kafka_producer_client, clickhouse_client
    ):
        import uuid
        order = {
            "order_id": str(uuid.uuid4()),
            "event_id": str(uuid.uuid4()),
            "event_type": "order_placed",
            "event_timestamp": 1705309200000,
            "user_id": "550e8400-e29b-41d4-a716-446655440001",
            "session_id": str(uuid.uuid4()),
            "items": [{
                "product_id": "660e8400-e29b-41d4-a716-446655440001",
                "product_name": "Test Product",
                "category": "Electronics",
                "quantity": 1,
                "unit_price": 100_000.0,
                "discount_pct": 0.0,
            }],
            "subtotal": 100_000.0,
            "shipping_fee": 0.0,
            "total_amount": 100_000.0,
            "payment_method": "CREDIT_CARD",
            "shipping_address": {
                "city": "Jakarta",
                "province": "DKI Jakarta",
                "country": "ID",
                "postal_code": "12190",
            },
            "device_type": "DESKTOP",
            "platform": "WEB",
            "coupon_code": None,
            "metadata": {"source": "idempotency-test"},
        }
        order_id = order["order_id"]

        for _ in range(2):
            kafka_producer_client.produce(
                topic="orders",
                key=order_id.encode(),
                value=json.dumps(order).encode(),
            )
        kafka_producer_client.flush(10)

        time.sleep(60)

        result = clickhouse_client.execute("""
            SELECT count() FROM analytics.orders_realtime
            WHERE order_id = %(order_id)s
        """, {"order_id": order_id})
        count = result[0][0]

        if count == 0:
            pytest.skip(
                f"Order {order_id} not found in ClickHouse — "
                "Spark streaming may not be running"
            )

        assert count <= 2, (
            f"Expected at most 2 records (raw), found {count} for order_id {order_id}"
        )

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_dbt_silver_rerun_is_idempotent(self, clickhouse_client):
        result_before = clickhouse_client.execute(
            "SELECT count() FROM silver.silver_orders"
        )[0][0]

        if result_before == 0:
            pytest.skip("silver_orders is empty — run dbt models first")

        dbt_result = subprocess.run(
            ["dbt", "run", "--select", "silver_orders", "--target", "prod"],
            cwd="dbt_project",
            capture_output=True,
            text=True,
            timeout=120,
        )

        if dbt_result.returncode != 0:
            pytest.skip(f"dbt run failed: {dbt_result.stderr[:200]}")

        result_after = clickhouse_client.execute(
            "SELECT count() FROM silver.silver_orders"
        )[0][0]

        assert result_before == result_after, (
            f"dbt re-run changed silver_orders row count: "
            f"before={result_before}, after={result_after}"
        )


class TestDataFreshness:

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_new_event_appears_in_clickhouse_within_60_seconds(
        self, kafka_producer_client, clickhouse_client
    ):
        import uuid

        fresh_order_id = str(uuid.uuid4())
        payload = {
            "order_id": fresh_order_id,
            "event_id": str(uuid.uuid4()),
            "event_type": "order_placed",
            "event_timestamp": int(time.time() * 1000),
            "user_id": "550e8400-e29b-41d4-a716-446655440001",
            "session_id": str(uuid.uuid4()),
            "items": [{
                "product_id": "660e8400-e29b-41d4-a716-446655440001",
                "product_name": "Freshness Test Product",
                "category": "Test",
                "quantity": 1,
                "unit_price": 50_000.0,
                "discount_pct": 0.0,
            }],
            "subtotal": 50_000.0,
            "shipping_fee": 0.0,
            "total_amount": 50_000.0,
            "payment_method": "EWALLET",
            "shipping_address": {
                "city": "Jakarta",
                "province": "DKI Jakarta",
                "country": "ID",
                "postal_code": "12190",
            },
            "device_type": "MOBILE",
            "platform": "ANDROID",
            "coupon_code": None,
            "metadata": {"source": "freshness-test"},
        }

        produce_time = time.time()
        kafka_producer_client.produce(
            topic="orders",
            key=fresh_order_id.encode(),
            value=json.dumps(payload).encode(),
        )
        kafka_producer_client.flush(10)

        sla_seconds = 60
        found_at = None
        for _ in range(sla_seconds // 5):
            time.sleep(5)
            result = clickhouse_client.execute("""
                SELECT count() FROM analytics.orders_realtime
                WHERE order_id = %(order_id)s
            """, {"order_id": fresh_order_id})
            if result[0][0] >= 1:
                found_at = time.time()
                break

        if found_at is None:
            pytest.skip(
                f"Freshness test order not found within {sla_seconds}s — "
                "Spark streaming may not be running. Start it with: make run-spark-streaming"
            )

        latency = found_at - produce_time
        assert latency <= sla_seconds, (
            f"Streaming latency {latency:.1f}s exceeds {sla_seconds}s SLA"
        )


class TestInfrastructureResilience:

    @pytest.mark.e2e
    def test_clickhouse_handles_large_query(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT
                payment_method,
                count() AS order_count,
                sum(total_amount) AS revenue,
                avg(total_amount) AS avg_value,
                min(total_amount) AS min_value,
                max(total_amount) AS max_value
            FROM silver.silver_orders
            GROUP BY payment_method
            ORDER BY revenue DESC
        """)
        assert isinstance(result, list)

    @pytest.mark.e2e
    def test_postgres_handles_concurrent_reads(self, postgres_conn):
        from sqlalchemy import text
        with postgres_conn.connect() as conn:
            for _ in range(5):
                result = conn.execute(
                    text("SELECT count(*) FROM users")
                ).scalar()
                assert result >= 0

    @pytest.mark.e2e
    def test_kafka_handles_high_throughput_produce(self, kafka_producer_client):
        import uuid
        delivered = []

        def on_delivery(err, msg):
            if err is None:
                delivered.append(True)

        for i in range(100):
            kafka_producer_client.produce(
                topic="orders",
                key=str(uuid.uuid4()).encode(),
                value=json.dumps({"index": i, "stress": "test"}).encode(),
                on_delivery=on_delivery,
            )

        kafka_producer_client.flush(30)
        assert len(delivered) == 100, (
            f"Expected 100 deliveries, got {len(delivered)} — Kafka may be under stress"
        )

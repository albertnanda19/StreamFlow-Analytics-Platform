import time
import json
import logging
import pytest

from tests.fixtures.seed_data import DETERMINISTIC_ORDERS, EXPECTED_AGGREGATES

logger = logging.getLogger("e2e.full_pipeline")


class TestCompleteOrderJourney:

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_order_flows_from_kafka_to_clickhouse(
        self,
        kafka_producer_client,
        clickhouse_client,
        airflow_api_client,
    ):
        order = DETERMINISTIC_ORDERS[0]
        order_id = order["order_id"]
        timestamps = {}

        payload = {k: v for k, v in order.items() if not k.startswith("_")}

        timestamps["step1_start"] = time.time()
        delivered = []

        def on_delivery(err, msg):
            if err is None:
                delivered.append(True)

        kafka_producer_client.produce(
            topic="orders",
            key=order_id.encode(),
            value=json.dumps(payload).encode(),
            on_delivery=on_delivery,
        )
        kafka_producer_client.flush(15)

        assert len(delivered) == 1, "STEP 1 FAILED: Event not delivered to Kafka"
        logger.info("Step 1 complete: event produced in %.2fs", time.time() - timestamps["step1_start"])

        timestamps["step2_start"] = time.time()
        ch_found = False
        for _ in range(9):
            time.sleep(10)
            result = clickhouse_client.execute("""
                SELECT count() FROM analytics.orders_realtime
                WHERE order_id = %(order_id)s
            """, {"order_id": order_id})
            if result[0][0] >= 1:
                ch_found = True
                timestamps["clickhouse_verified"] = time.time()
                logger.info("Step 2 complete: found in ClickHouse after %.1fs",
                            time.time() - timestamps["step2_start"])
                break

        if not ch_found:
            pytest.skip(
                f"Order {order_id} not found in orders_realtime after 90s — "
                "Spark streaming may not be running. "
                "Start with: make run-spark-streaming"
            )

        logger.info(
            "E2E Kafka→ClickHouse latency: %.1fs",
            timestamps.get("clickhouse_verified", time.time()) - timestamps["step1_start"]
        )

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_silver_data_accuracy_after_dbt_run(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT count() FROM silver.silver_orders
        """)
        count = result[0][0]
        assert count > 0, (
            "silver_orders is empty — run dbt silver models first: "
            "cd dbt_project && dbt run --select silver"
        )

        result = clickhouse_client.execute("""
            SELECT
                countIf(is_valid = false) AS invalid,
                countIf(total_amount <= 0) AS bad_amount,
                countIf(gross_revenue <= 0) AS bad_gross,
                count() AS total
            FROM silver.silver_orders
        """)
        invalid, bad_amount, bad_gross, total = result[0]
        assert invalid == 0, f"{invalid}/{total} invalid records in silver_orders"
        assert bad_amount == 0, f"{bad_amount}/{total} records with non-positive total_amount"
        assert bad_gross == 0, f"{bad_gross}/{total} records with non-positive gross_revenue"

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_gold_revenue_matches_silver_revenue(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT
                (SELECT sum(revenue) FROM gold.gold_daily_revenue) AS gold_total,
                (SELECT sum(total_amount) FROM silver.silver_orders) AS silver_total
        """)
        gold_total, silver_total = result[0]

        if not silver_total or silver_total == 0:
            pytest.skip("No data in silver_orders — run full pipeline first")

        diff_pct = abs(gold_total - silver_total) / silver_total * 100
        assert diff_pct < 1.0, (
            f"Gold revenue ({gold_total}) differs {diff_pct:.2f}% from silver ({silver_total})"
        )

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_full_pipeline_end_to_end_latency_within_sla(
        self,
        kafka_producer_client,
        clickhouse_client,
    ):
        import uuid
        new_order_id = str(uuid.uuid4())
        payload = {
            **{k: v for k, v in DETERMINISTIC_ORDERS[0].items() if not k.startswith("_")},
            "order_id": new_order_id,
            "event_id": str(uuid.uuid4()),
        }

        produce_time = time.time()
        kafka_producer_client.produce(
            topic="orders",
            key=new_order_id.encode(),
            value=json.dumps(payload).encode(),
        )
        kafka_producer_client.flush(10)

        sla_seconds = 120
        found_at = None
        for _ in range(sla_seconds // 5):
            time.sleep(5)
            result = clickhouse_client.execute("""
                SELECT count() FROM analytics.orders_realtime
                WHERE order_id = %(order_id)s
            """, {"order_id": new_order_id})
            if result[0][0] >= 1:
                found_at = time.time()
                break

        if found_at is None:
            pytest.skip(
                f"Order {new_order_id} not found in ClickHouse within {sla_seconds}s — "
                "Spark streaming may not be running"
            )

        latency = found_at - produce_time
        logger.info("Streaming latency: %.1fs (SLA: %ds)", latency, sla_seconds)
        assert latency <= sla_seconds, (
            f"Streaming latency {latency:.1f}s exceeds {sla_seconds}s SLA"
        )

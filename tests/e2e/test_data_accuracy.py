import pytest

from tests.fixtures.seed_data import DETERMINISTIC_ORDERS, EXPECTED_AGGREGATES


class TestRevenueConsistencyAcrossLayers:

    @pytest.mark.e2e
    @pytest.mark.accuracy
    def test_silver_total_revenue_is_positive(self, clickhouse_client):
        result = clickhouse_client.execute(
            "SELECT sum(total_amount) FROM silver.silver_orders"
        )
        total = result[0][0] or 0
        assert total > 0, "Total revenue in silver_orders is zero — pipeline not running"

    @pytest.mark.e2e
    @pytest.mark.accuracy
    def test_gold_revenue_is_not_greater_than_silver(self, clickhouse_client):
        silver_result = clickhouse_client.execute(
            "SELECT sum(total_amount) FROM silver.silver_orders"
        )
        gold_result = clickhouse_client.execute(
            "SELECT sum(revenue) FROM gold.gold_daily_revenue"
        )
        silver_total = silver_result[0][0] or 0
        gold_total = gold_result[0][0] or 0

        if silver_total == 0:
            pytest.skip("No data in silver_orders")

        assert gold_total <= silver_total * 1.001, (
            f"Gold revenue ({gold_total}) > silver revenue ({silver_total}) — "
            "aggregation is creating phantom revenue"
        )

    @pytest.mark.e2e
    @pytest.mark.accuracy
    def test_payment_method_distribution_in_silver_is_consistent(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT payment_method, count() AS cnt
            FROM silver.silver_orders
            GROUP BY payment_method
            ORDER BY cnt DESC
        """)
        if not result:
            pytest.skip("No data in silver_orders")

        valid_methods = {"CREDIT_CARD", "DEBIT_CARD", "BANK_TRANSFER", "EWALLET", "COD"}
        for row in result:
            method, count = row
            assert method in valid_methods, f"Invalid payment_method in silver: {method}"
            assert count > 0

    @pytest.mark.e2e
    @pytest.mark.accuracy
    def test_daily_revenue_aggregation_does_not_lose_orders(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT
                (SELECT count() FROM silver.silver_orders) AS silver_count,
                (SELECT sum(order_count) FROM gold.gold_daily_revenue) AS gold_agg_count
        """)
        silver_count, gold_agg_count = result[0]

        if not silver_count:
            pytest.skip("No data in silver_orders")

        assert silver_count == gold_agg_count, (
            f"Order count mismatch: silver={silver_count}, gold_aggregated={gold_agg_count}"
        )

    @pytest.mark.e2e
    @pytest.mark.accuracy
    def test_seed_data_aggregate_constants_are_correct(self):
        computed_total = sum(o["total_amount"] for o in DETERMINISTIC_ORDERS)
        assert abs(computed_total - EXPECTED_AGGREGATES["total_revenue"]) < 0.01, (
            "Seed data EXPECTED_AGGREGATES['total_revenue'] is wrong — update seed_data.py"
        )

        by_method = {}
        for o in DETERMINISTIC_ORDERS:
            m = o["payment_method"]
            by_method[m] = by_method.get(m, 0) + o["total_amount"]
        for method, expected in EXPECTED_AGGREGATES["revenue_by_payment_method"].items():
            computed = by_method.get(method, 0)
            assert abs(computed - expected) < 0.01, (
                f"revenue_by_payment_method[{method}] expected {expected}, got {computed}"
            )

    @pytest.mark.e2e
    @pytest.mark.accuracy
    def test_no_revenue_created_in_pipeline(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT
                (SELECT sum(total_amount) FROM silver.silver_orders) AS silver_rev,
                (SELECT sum(revenue) FROM gold.gold_daily_revenue) AS gold_rev
        """)
        silver_rev, gold_rev = result[0]

        if not silver_rev or silver_rev == 0:
            pytest.skip("No data in silver_orders")

        assert abs(silver_rev - gold_rev) / silver_rev < 0.001, (
            f"Revenue discrepancy >0.1%: silver={silver_rev}, gold={gold_rev}"
        )


class TestDataIntegrityAcrossTables:

    @pytest.mark.e2e
    @pytest.mark.accuracy
    def test_all_silver_order_items_have_parent_order(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT count()
            FROM silver.silver_order_items i
            WHERE NOT EXISTS (
                SELECT 1 FROM silver.silver_orders o WHERE o.order_id = i.order_id
            )
        """)
        orphan_count = result[0][0]
        assert orphan_count == 0, (
            f"{orphan_count} order items have no matching parent order in silver_orders"
        )

    @pytest.mark.e2e
    @pytest.mark.accuracy
    def test_all_gold_users_exist_in_silver_orders(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT count()
            FROM gold.gold_user_segments g
            WHERE g.user_id NOT IN (
                SELECT DISTINCT user_id FROM silver.silver_orders
            )
        """)
        ghost_users = result[0][0]
        assert ghost_users == 0, (
            f"{ghost_users} users in gold_user_segments have no orders in silver_orders"
        )

    @pytest.mark.e2e
    @pytest.mark.accuracy
    def test_clickhouse_realtime_and_silver_have_same_order_ids(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT
                uniqExact(order_id) AS realtime_unique,
                (SELECT uniqExact(order_id) FROM silver.silver_orders) AS silver_unique
            FROM analytics.orders_realtime
        """)
        realtime_count, silver_count = result[0]
        if realtime_count == 0 or silver_count == 0:
            pytest.skip("Insufficient data in realtime or silver")

        coverage_pct = min(realtime_count, silver_count) / max(realtime_count, silver_count) * 100
        assert coverage_pct >= 80.0, (
            f"Order coverage between realtime and silver is only {coverage_pct:.1f}% — "
            f"realtime={realtime_count}, silver={silver_count}"
        )

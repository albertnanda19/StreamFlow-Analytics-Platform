import pytest

from tests.fixtures.seed_data import VALID_SEGMENT_LABELS


class TestGoldTableExistence:

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.parametrize("schema_table", [
        ("gold", "gold_daily_revenue"),
        ("gold", "gold_product_performance"),
        ("gold", "gold_user_segments"),
        ("gold", "gold_conversion_funnel"),
    ])
    def test_gold_table_exists_and_has_rows(self, clickhouse_client, schema_table):
        schema, table = schema_table
        result = clickhouse_client.execute(f"SELECT count() FROM {schema}.{table}")
        count = result[0][0]
        assert count > 0, f"Table {schema}.{table} is empty — run dbt gold models first"


class TestGoldDailyRevenue:

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_gold_daily_revenue_totals_match_silver(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT
                gold_total,
                silver_total,
                abs(gold_total - silver_total) AS diff
            FROM (
                SELECT sum(revenue) AS gold_total
                FROM gold.gold_daily_revenue
                WHERE order_date >= today() - 7
            ) g,
            (
                SELECT sum(total_amount) AS silver_total
                FROM silver.silver_orders
                WHERE toDate(event_timestamp) >= today() - 7
            ) s
        """)
        gold_total, silver_total, diff = result[0]
        assert diff < 1.0, (
            f"Revenue mismatch: gold={gold_total}, silver={silver_total}, diff={diff}"
        )

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_gold_daily_revenue_no_future_dates(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT countIf(order_date > today()) FROM gold.gold_daily_revenue
        """)
        assert result[0][0] == 0, "Future-dated records found in gold_daily_revenue"

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_gold_daily_revenue_all_positive(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT countIf(revenue <= 0 OR order_count <= 0)
            FROM gold.gold_daily_revenue
        """)
        assert result[0][0] == 0, "Non-positive revenue or order_count in gold_daily_revenue"

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_gold_daily_revenue_avg_order_value_formula(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT count()
            FROM gold.gold_daily_revenue
            WHERE abs(avg_order_value - (revenue / order_count)) > 1.0
        """)
        assert result[0][0] == 0, "avg_order_value formula incorrect in gold_daily_revenue"

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_gold_daily_revenue_order_count_matches_silver(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT count()
            FROM (
                SELECT
                    g.order_date,
                    g.order_count AS gold_count,
                    s.silver_count,
                    abs(g.order_count - s.silver_count) AS diff
                FROM gold.gold_daily_revenue g
                JOIN (
                    SELECT
                        toDate(event_timestamp) AS order_date,
                        count() AS silver_count
                    FROM silver.silver_orders
                    GROUP BY order_date
                ) s ON g.order_date = s.order_date
            )
            WHERE diff > 0
        """)
        mismatch_count = result[0][0]
        assert mismatch_count == 0, (
            f"{mismatch_count} dates have mismatched order_count between gold and silver"
        )


class TestGoldUserSegments:

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_all_active_users_have_rfm_segment(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT count()
            FROM (
                SELECT DISTINCT user_id FROM silver.silver_orders
            ) orders_users
            WHERE user_id NOT IN (
                SELECT user_id FROM gold.gold_user_segments
            )
        """)
        missing_count = result[0][0]
        assert missing_count == 0, (
            f"{missing_count} users with orders are missing from gold_user_segments"
        )

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_rfm_segment_labels_are_valid(self, clickhouse_client):
        result = clickhouse_client.execute(
            "SELECT DISTINCT segment FROM gold.gold_user_segments"
        )
        actual = {row[0] for row in result}
        invalid = actual - VALID_SEGMENT_LABELS
        assert invalid == set(), f"Invalid segment labels: {invalid}"

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_gold_user_segments_no_duplicate_users(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT count(user_id) AS total, uniq(user_id) AS unique_count
            FROM gold.gold_user_segments
        """)
        total, unique = result[0]
        assert total == unique, (
            f"Duplicate users in gold_user_segments: {total} total vs {unique} unique"
        )

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_gold_user_segments_total_revenue_positive(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT countIf(total_revenue <= 0) FROM gold.gold_user_segments
        """)
        assert result[0][0] == 0, "Users with non-positive total_revenue in gold_user_segments"


class TestGoldConversionFunnel:

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_funnel_stages_monotonically_decreasing(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT count()
            FROM gold.gold_conversion_funnel
            WHERE NOT (
                sessions_total >= sessions_with_product_view
                AND sessions_with_product_view >= sessions_with_cart
                AND sessions_with_cart >= sessions_with_checkout
                AND sessions_with_checkout >= sessions_converted
            )
        """)
        violations = result[0][0]
        assert violations == 0, (
            f"{violations} funnel records violate monotonically decreasing stages"
        )

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_conversion_rates_between_zero_and_one(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT count()
            FROM gold.gold_conversion_funnel
            WHERE overall_conversion_rate < 0
               OR overall_conversion_rate > 1
        """)
        assert result[0][0] == 0, "Conversion rates out of [0, 1] found in gold_conversion_funnel"

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_funnel_no_future_dates(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT countIf(session_date > today()) FROM gold.gold_conversion_funnel
        """)
        assert result[0][0] == 0, "Future session_date records in gold_conversion_funnel"


class TestGoldProductPerformance:

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_product_performance_all_positive_revenue(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT countIf(total_revenue <= 0) FROM gold.gold_product_performance
        """)
        assert result[0][0] == 0, "Non-positive total_revenue in gold_product_performance"

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_product_performance_no_duplicate_products(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT count(product_id) AS total, uniq(product_id) AS unique_count
            FROM gold.gold_product_performance
        """)
        total, unique = result[0]
        assert total == unique, (
            f"Duplicate products in gold_product_performance: {total} total vs {unique} unique"
        )

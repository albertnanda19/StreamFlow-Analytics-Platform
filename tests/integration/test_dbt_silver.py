import pytest

from tests.fixtures.expected_results import SILVER_ORDER_EXPECTED


class TestSilverTableExistence:

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.parametrize("schema_table", [
        ("silver", "silver_orders"),
        ("silver", "silver_order_items"),
        ("silver", "silver_pageviews"),
        ("silver", "silver_sessions"),
        ("silver", "silver_inventory"),
    ])
    def test_silver_table_exists_and_has_rows(self, clickhouse_client, schema_table):
        schema, table = schema_table
        result = clickhouse_client.execute(
            f"SELECT count() FROM {schema}.{table}"
        )
        count = result[0][0]
        assert count > 0, f"Table {schema}.{table} is empty — run dbt silver models first"


class TestSilverOrdersAccuracy:

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_silver_orders_no_duplicates(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT
                count(order_id) AS total,
                uniq(order_id) AS unique_count
            FROM silver.silver_orders
        """)
        total, unique = result[0]
        assert total == unique, (
            f"Duplicates in silver_orders: {total} total vs {unique} unique order_ids"
        )

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_silver_orders_contains_only_valid_records(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT countIf(is_valid = false) FROM silver.silver_orders
        """)
        invalid_count = result[0][0]
        assert invalid_count == 0, (
            f"Found {invalid_count} invalid records in silver_orders — "
            "Silver must contain only is_valid=true records"
        )

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_silver_orders_no_null_order_id(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT countIf(order_id = '' OR order_id IS NULL)
            FROM silver.silver_orders
        """)
        assert result[0][0] == 0, "Found null/empty order_ids in silver_orders"

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_silver_orders_gross_revenue_formula_correct(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT count()
            FROM silver.silver_orders
            WHERE abs(gross_revenue - (total_amount - shipping_fee)) > 0.01
        """)
        mismatch_count = result[0][0]
        assert mismatch_count == 0, (
            f"{mismatch_count} records have incorrect gross_revenue formula"
        )

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_silver_orders_is_high_value_threshold_correct(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT
                countIf(total_amount > 500000 AND is_high_value = false) AS wrong_high,
                countIf(total_amount <= 500000 AND is_high_value = true) AS wrong_low
            FROM silver.silver_orders
        """)
        wrong_high, wrong_low = result[0]
        assert wrong_high == 0, (
            f"{wrong_high} high-value orders incorrectly marked is_high_value=false"
        )
        assert wrong_low == 0, (
            f"{wrong_low} normal orders incorrectly marked is_high_value=true"
        )

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_silver_orders_no_future_timestamps(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT countIf(event_timestamp > now() + INTERVAL 1 HOUR)
            FROM silver.silver_orders
        """)
        assert result[0][0] == 0, "Future timestamps found in silver_orders"

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_silver_orders_total_amount_all_positive(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT countIf(total_amount <= 0) FROM silver.silver_orders
        """)
        assert result[0][0] == 0, "Non-positive total_amount found in silver_orders"

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_silver_orders_payment_method_all_valid(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT countIf(payment_method NOT IN (
                'CREDIT_CARD','DEBIT_CARD','BANK_TRANSFER','EWALLET','COD'
            ))
            FROM silver.silver_orders
        """)
        assert result[0][0] == 0, "Invalid payment_method values found in silver_orders"

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_silver_order_items_revenue_formula_correct(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT count()
            FROM silver.silver_order_items
            WHERE abs(item_revenue - (quantity * unit_price * (1 - discount_pct))) > 0.01
        """)
        assert result[0][0] == 0, "Item revenue formula incorrect for some records"

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_silver_order_items_sum_matches_gross_revenue(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT count()
            FROM (
                SELECT
                    o.order_id,
                    o.gross_revenue,
                    sum(i.item_revenue) AS items_total,
                    abs(o.gross_revenue - sum(i.item_revenue)) AS diff
                FROM silver.silver_orders o
                JOIN silver.silver_order_items i ON o.order_id = i.order_id
                GROUP BY o.order_id, o.gross_revenue
            )
            WHERE diff > 1.0
        """)
        mismatch_count = result[0][0]
        assert mismatch_count == 0, (
            f"{mismatch_count} orders have item sum != gross_revenue (cross-table inconsistency)"
        )


class TestSilverPageviewsAccuracy:

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_silver_pageviews_duration_positive(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT countIf(duration_seconds <= 0) FROM silver.silver_pageviews
        """)
        assert result[0][0] == 0, "Non-positive duration_seconds in silver_pageviews"

    @pytest.mark.integration
    @pytest.mark.dbt
    @pytest.mark.accuracy
    def test_silver_pageviews_page_type_valid(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT countIf(page_type NOT IN (
                'HOME','CATEGORY','PRODUCT_DETAIL','SEARCH',
                'CART','CHECKOUT','ORDER_CONFIRMATION','PROFILE'
            ))
            FROM silver.silver_pageviews
        """)
        assert result[0][0] == 0, "Invalid page_type values in silver_pageviews"

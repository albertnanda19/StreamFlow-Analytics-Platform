import re
import pytest

from tests.fixtures.expected_results import BRONZE_REQUIRED_COLUMNS


class TestBronzeBucketsAndFiles:

    @pytest.mark.integration
    @pytest.mark.slow
    def test_bronze_bucket_exists_in_minio(self, minio_client):
        buckets = [b["Name"] for b in minio_client.list_buckets()["Buckets"]]
        assert "bronze" in buckets, "MinIO bucket 'bronze' not found"

    @pytest.mark.integration
    @pytest.mark.slow
    @pytest.mark.parametrize("prefix", ["orders/", "pageviews/", "inventory-state/"])
    def test_bronze_table_has_parquet_files(self, minio_client, prefix):
        response = minio_client.list_objects_v2(Bucket="bronze", Prefix=prefix)
        objects = response.get("Contents", [])
        parquet_files = [
            o for o in objects
            if o["Key"].endswith(".parquet") or "_delta_log" not in o["Key"]
        ]
        assert len(parquet_files) > 0, f"No files found in bronze/{prefix}"

    @pytest.mark.integration
    @pytest.mark.slow
    @pytest.mark.parametrize("prefix", ["orders/_delta_log/", "pageviews/_delta_log/"])
    def test_bronze_table_has_delta_log(self, minio_client, prefix):
        response = minio_client.list_objects_v2(Bucket="bronze", Prefix=prefix)
        contents = response.get("Contents", [])
        assert len(contents) > 0, (
            f"Delta log not found at bronze/{prefix} — "
            "table may not be a proper Delta table"
        )

    @pytest.mark.integration
    @pytest.mark.slow
    def test_bronze_orders_is_partitioned_by_date(self, minio_client):
        response = minio_client.list_objects_v2(Bucket="bronze", Prefix="orders/")
        keys = [o["Key"] for o in response.get("Contents", [])]
        date_partitions = [
            k for k in keys
            if re.search(r"processing_date=\d{4}-\d{2}-\d{2}", k)
        ]
        assert len(date_partitions) > 0, (
            "No date-partitioned files found in bronze/orders/ — "
            "check partitionBy('processing_date') in Spark job"
        )

    @pytest.mark.integration
    @pytest.mark.slow
    def test_checkpoints_bucket_has_orders_checkpoint(self, minio_client):
        response = minio_client.list_objects_v2(
            Bucket="checkpoints", Prefix="orders-bronze/"
        )
        contents = response.get("Contents", [])
        assert len(contents) > 0, (
            "Checkpoint directory 'orders-bronze/' not found in 'checkpoints' bucket — "
            "streaming job may not have checkpointing enabled"
        )


class TestBronzeDataAccuracy:

    @pytest.mark.integration
    @pytest.mark.accuracy
    @pytest.mark.slow
    def test_bronze_orders_has_records(self, clickhouse_client):
        result = clickhouse_client.execute(
            "SELECT count() FROM analytics.orders_realtime"
        )
        count = result[0][0]
        assert count > 0, "orders_realtime in ClickHouse has 0 records — streaming may not be running"

    @pytest.mark.integration
    @pytest.mark.accuracy
    @pytest.mark.slow
    def test_bronze_realtime_no_negative_amounts(self, clickhouse_client):
        result = clickhouse_client.execute(
            "SELECT countIf(total_amount <= 0) FROM analytics.orders_realtime"
        )
        invalid_count = result[0][0]
        assert invalid_count == 0, (
            f"Found {invalid_count} records with total_amount <= 0 in orders_realtime"
        )

    @pytest.mark.integration
    @pytest.mark.accuracy
    @pytest.mark.slow
    def test_bronze_realtime_all_have_order_id(self, clickhouse_client):
        result = clickhouse_client.execute(
            "SELECT countIf(order_id = '' OR order_id IS NULL) FROM analytics.orders_realtime"
        )
        null_count = result[0][0]
        assert null_count == 0, (
            f"Found {null_count} records with null/empty order_id in orders_realtime"
        )

    @pytest.mark.integration
    @pytest.mark.accuracy
    @pytest.mark.slow
    def test_bronze_realtime_timestamps_are_not_future(self, clickhouse_client):
        result = clickhouse_client.execute("""
            SELECT countIf(event_timestamp > now() + INTERVAL 1 HOUR)
            FROM analytics.orders_realtime
        """)
        future_count = result[0][0]
        assert future_count == 0, (
            f"Found {future_count} records with future event_timestamp"
        )

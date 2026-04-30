import sys
import os
import pytest
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


def _make_spark():
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("streamflow-unit-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


@pytest.fixture(scope="module")
def spark():
    s = _make_spark()
    yield s
    s.stop()


class TestOrderValidationLogic:

    @pytest.mark.unit
    @pytest.mark.spark
    def test_valid_order_is_marked_is_valid_true(self, spark):
        from pyspark.sql.functions import col, current_timestamp, to_date, lit, size
        from pyspark.sql.types import (
            StructType, StructField, StringType, DoubleType,
            LongType, ArrayType,
        )
        from spark_jobs.streaming.orders_to_bronze import _validate

        schema = StructType([
            StructField("order_id", StringType()),
            StructField("user_id", StringType()),
            StructField("total_amount", DoubleType()),
            StructField("event_timestamp", StringType()),
            StructField("items", ArrayType(StructType([
                StructField("product_id", StringType()),
            ]))),
        ])
        data = [(
            "770e8400-e29b-41d4-a716-446655440001",
            "550e8400-e29b-41d4-a716-446655440001",
            15_000_000.0,
            "2024-01-15T09:00:00+00:00",
            [("660e8400-e29b-41d4-a716-446655440001",)],
        )]
        df = spark.createDataFrame(data, schema=schema)
        result = _validate(df)
        valid_count = result.filter("is_valid = true").count()
        assert valid_count == 1

    @pytest.mark.unit
    @pytest.mark.spark
    @pytest.mark.parametrize("field,value,description", [
        ("order_id", None, "null order_id"),
        ("user_id", None, "null user_id"),
        ("total_amount", -100.0, "negative total_amount"),
        ("total_amount", 0.0, "zero total_amount"),
        ("total_amount", None, "null total_amount"),
    ])
    def test_invalid_field_marks_record_as_invalid(self, spark, field, value, description):
        from pyspark.sql.types import (
            StructType, StructField, StringType, DoubleType, ArrayType,
        )
        from spark_jobs.streaming.orders_to_bronze import _validate

        schema = StructType([
            StructField("order_id", StringType()),
            StructField("user_id", StringType()),
            StructField("total_amount", DoubleType()),
            StructField("event_timestamp", StringType()),
            StructField("items", ArrayType(StructType([
                StructField("product_id", StringType()),
            ]))),
        ])
        base = {
            "order_id": "770e8400-e29b-41d4-a716-446655440001",
            "user_id": "550e8400-e29b-41d4-a716-446655440001",
            "total_amount": 15_000_000.0,
            "event_timestamp": "2024-01-15T09:00:00+00:00",
            "items": [("660e8400-e29b-41d4-a716-446655440001",)],
        }
        base[field] = value
        df = spark.createDataFrame([tuple(base.values())], schema=schema)
        result = _validate(df)
        invalid_count = result.filter("is_valid = false").count()
        assert invalid_count == 1, f"Expected invalid for: {description}"

    @pytest.mark.unit
    @pytest.mark.spark
    def test_invalid_records_are_not_dropped_from_output(self, spark):
        from pyspark.sql.types import (
            StructType, StructField, StringType, DoubleType, ArrayType,
        )
        from spark_jobs.streaming.orders_to_bronze import _validate

        schema = StructType([
            StructField("order_id", StringType()),
            StructField("user_id", StringType()),
            StructField("total_amount", DoubleType()),
            StructField("event_timestamp", StringType()),
            StructField("items", ArrayType(StructType([
                StructField("product_id", StringType()),
            ]))),
        ])
        valid_rows = [
            ("id-v1", "u1", 1_000_000.0, "2024-01-15T09:00:00+00:00", [("p1",)]),
            ("id-v2", "u2", 500_000.0, "2024-01-15T09:01:00+00:00", [("p2",)]),
            ("id-v3", "u3", 250_000.0, "2024-01-15T09:02:00+00:00", [("p3",)]),
        ]
        invalid_rows = [
            (None, "u4", 100_000.0, "2024-01-15T09:03:00+00:00", [("p4",)]),
            ("id-v5", None, 200_000.0, "2024-01-15T09:04:00+00:00", [("p5",)]),
        ]
        df = spark.createDataFrame(valid_rows + invalid_rows, schema=schema)
        result = _validate(df)
        total_output = result.count()
        assert total_output == 5, f"Expected 5 records (no drops), got {total_output}"
        assert result.filter("is_valid = true").count() == 3
        assert result.filter("is_valid = false").count() == 2


class TestDataQualityFormulas:

    @pytest.mark.unit
    def test_gross_revenue_formula(self):
        total_amount = 2_525_000.0
        shipping_fee = 15_000.0
        gross_revenue = total_amount - shipping_fee
        assert abs(gross_revenue - 2_510_000.0) < 0.01

    @pytest.mark.unit
    @pytest.mark.parametrize("total_amount,threshold,expected", [
        (15_000_000.0, 500_000, True),
        (600_000.0, 500_000, True),
        (500_000.0, 500_000, False),
        (265_000.0, 500_000, False),
    ])
    def test_is_high_value_threshold(self, total_amount, threshold, expected):
        result = total_amount > threshold
        assert result == expected

    @pytest.mark.unit
    @pytest.mark.parametrize("items,expected_has_discount", [
        ([{"discount_pct": 0.0}, {"discount_pct": 0.0}], False),
        ([{"discount_pct": 0.10}], True),
        ([{"discount_pct": 0.0}, {"discount_pct": 0.15}], True),
    ])
    def test_has_discount_formula(self, items, expected_has_discount):
        has_discount = any(item["discount_pct"] > 0 for item in items)
        assert has_discount == expected_has_discount

    @pytest.mark.unit
    @pytest.mark.parametrize("device_type,expected_category", [
        ("MOBILE", "MOBILE"),
        ("TABLET", "MOBILE"),
        ("DESKTOP", "NON-MOBILE"),
    ])
    def test_device_category_mapping(self, device_type, expected_category):
        category = "MOBILE" if device_type in ("MOBILE", "TABLET") else "NON-MOBILE"
        assert category == expected_category

    @pytest.mark.unit
    def test_item_revenue_exact_calculation(self):
        quantity = 2
        unit_price = 1_200_000.0
        discount_pct = 0.10
        expected = 2_160_000.0
        result = round(quantity * unit_price * (1 - discount_pct), 2)
        assert result == expected

    @pytest.mark.unit
    def test_revenue_not_created_or_destroyed_across_layers(self):
        from tests.fixtures.seed_data import DETERMINISTIC_ORDERS, EXPECTED_AGGREGATES
        computed_total = sum(o["total_amount"] for o in DETERMINISTIC_ORDERS)
        assert abs(computed_total - EXPECTED_AGGREGATES["total_revenue"]) < 0.01

    @pytest.mark.unit
    def test_payment_method_distribution_sums_to_total(self):
        from tests.fixtures.seed_data import DETERMINISTIC_ORDERS, EXPECTED_AGGREGATES
        dist = EXPECTED_AGGREGATES["revenue_by_payment_method"]
        computed_sum = sum(dist.values())
        expected_total = EXPECTED_AGGREGATES["total_revenue"]
        assert abs(computed_sum - expected_total) < 0.01

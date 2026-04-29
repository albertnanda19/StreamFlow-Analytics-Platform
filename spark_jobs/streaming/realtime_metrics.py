import logging
import os
import sys
import time

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    expr,
    explode,
    from_json,
    lit,
    sum as spark_sum,
    to_timestamp,
    window,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.schemas import ORDER_PLACED_SCHEMA, PAGEVIEW_SCHEMA
from config.spark_config import SparkSessionBuilder
from utils.avro_deserializer import make_avro_deserializer_udf
from utils.metrics_reporter import StreamingMetricsReporter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("realtime-metrics")

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCHEMA_REG_URL   = os.getenv("SCHEMA_REGISTRY_URL",     "http://schema-registry:8081")
CLICKHOUSE_HOST  = os.getenv("CLICKHOUSE_HOST",          "clickhouse")
CLICKHOUSE_PORT  = os.getenv("CLICKHOUSE_PORT",          "8123")
CLICKHOUSE_USER  = os.getenv("CLICKHOUSE_USER",          "streamflow")
CLICKHOUSE_PASS  = os.getenv("CLICKHOUSE_PASSWORD",      "streamflow123")
CLICKHOUSE_DB    = os.getenv("CLICKHOUSE_DB",            "analytics")
CHECKPOINT_BASE  = SparkSessionBuilder.checkpoint_path("realtime-metrics")


def _ch_jdbc_options(table: str) -> dict:
    return {
        "url":      f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}",
        "driver":   "com.clickhouse.jdbc.ClickHouseDriver",
        "dbtable":  table,
        "user":     CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASS,
    }


def _write_metric(df: DataFrame, batch_id: int, table: str) -> None:
    try:
        df.write.format("jdbc").options(**_ch_jdbc_options(table)).mode("append").save()
        logger.info("Wrote batch %d to ClickHouse table %s", batch_id, table)
    except Exception as exc:
        logger.error("ClickHouse write to %s failed (batch %d): %s", table, batch_id, exc)


def _read_orders_stream(spark, deserialize_udf):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "orders")
        .option("startingOffsets", "latest")
        .option("kafka.group.id", "spark-realtime-metrics-orders")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 5000)
        .load()
    )
    return (
        raw
        .withColumn("json_str", deserialize_udf(col("value")))
        .filter(col("json_str").isNotNull())
        .withColumn("data", from_json(col("json_str"), ORDER_PLACED_SCHEMA))
        .select(
            col("data.order_id"),
            col("data.user_id"),
            col("data.session_id"),
            col("data.total_amount"),
            col("data.payment_method"),
            col("data.device_type"),
            col("data.items"),
            to_timestamp(expr("CAST(data.event_timestamp / 1000 AS BIGINT)")).alias("event_timestamp"),
        )
    )


def _read_pageviews_stream(spark, deserialize_udf):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "pageviews")
        .option("startingOffsets", "latest")
        .option("kafka.group.id", "spark-realtime-metrics-pv")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 10000)
        .load()
    )
    return (
        raw
        .withColumn("json_str", deserialize_udf(col("value")))
        .filter(col("json_str").isNotNull())
        .withColumn("data", from_json(col("json_str"), PAGEVIEW_SCHEMA))
        .select(
            col("data.session_id"),
            col("data.user_id"),
            col("data.page_type"),
            to_timestamp(expr("CAST(data.event_timestamp / 1000 AS BIGINT)")).alias("event_timestamp"),
        )
    )


def run() -> None:
    spark = SparkSessionBuilder.build("streamflow-realtime-metrics")
    spark.sparkContext.setLogLevel("WARN")

    deserialize_udf = make_avro_deserializer_udf(SCHEMA_REG_URL)

    orders_stream    = _read_orders_stream(spark, deserialize_udf)
    pageviews_stream = _read_pageviews_stream(spark, deserialize_udf)

    revenue_window = (
        orders_stream
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            window("event_timestamp", "5 minutes", "1 minute"),
            col("payment_method"),
        )
        .agg(
            spark_sum("total_amount").alias("revenue"),
            count("order_id").alias("order_count"),
            avg("total_amount").alias("avg_order_value"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("payment_method"),
            col("revenue"),
            col("order_count"),
            col("avg_order_value"),
            current_timestamp().alias("_updated_at"),
        )
    )

    items_stream = (
        orders_stream
        .withWatermark("event_timestamp", "15 minutes")
        .select(
            col("event_timestamp"),
            explode(col("items")).alias("item"),
        )
        .select(
            col("event_timestamp"),
            col("item.product_id"),
            col("item.product_name"),
            col("item.category"),
            col("item.quantity"),
            (col("item.unit_price") * col("item.quantity") * (lit(1) - col("item.discount_pct"))).alias("item_revenue"),
        )
    )

    product_window = (
        items_stream
        .groupBy(
            window("event_timestamp", "1 hour", "5 minutes"),
            col("product_id"),
            col("product_name"),
            col("category"),
        )
        .agg(
            spark_sum("quantity").alias("units_sold"),
            spark_sum("item_revenue").alias("revenue"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("product_id"),
            col("product_name"),
            col("category"),
            col("units_sold"),
            col("revenue"),
            current_timestamp().alias("_updated_at"),
        )
    )

    sessions_with_order = (
        orders_stream
        .withWatermark("event_timestamp", "30 minutes")
        .select(col("session_id"), lit(True).alias("has_order"))
        .distinct()
    )

    sessions_with_pv = (
        pageviews_stream
        .withWatermark("event_timestamp", "30 minutes")
        .select(col("session_id"), col("page_type"))
    )

    funnel = (
        sessions_with_pv
        .join(
            sessions_with_order,
            on="session_id",
            how="left",
        )
        .withColumn("converted", col("has_order").isNotNull())
        .groupBy("page_type")
        .agg(
            count("session_id").alias("total_sessions"),
            spark_sum(expr("CAST(converted AS INT)")).alias("converted_sessions"),
        )
        .withColumn(
            "conversion_rate",
            (col("converted_sessions") / col("total_sessions") * lit(100)),
        )
        .withColumn("_updated_at", current_timestamp())
    )

    def write_revenue(batch_df: DataFrame, batch_id: int):
        _write_metric(batch_df, batch_id, "daily_revenue")

    def write_products(batch_df: DataFrame, batch_id: int):
        _write_metric(batch_df, batch_id, "product_performance")

    def write_funnel(batch_df: DataFrame, batch_id: int):
        _write_metric(batch_df, batch_id, "user_behavior")

    q_revenue = (
        revenue_window.writeStream
        .outputMode("update")
        .trigger(processingTime="60 seconds")
        .option("checkpointLocation", CHECKPOINT_BASE + "revenue")
        .foreachBatch(write_revenue)
        .start()
    )

    q_products = (
        product_window.writeStream
        .outputMode("update")
        .trigger(processingTime="60 seconds")
        .option("checkpointLocation", CHECKPOINT_BASE + "products")
        .foreachBatch(write_products)
        .start()
    )

    q_funnel = (
        funnel.writeStream
        .outputMode("complete")
        .trigger(processingTime="60 seconds")
        .option("checkpointLocation", CHECKPOINT_BASE + "funnel")
        .foreachBatch(write_funnel)
        .start()
    )

    reporter = StreamingMetricsReporter(q_revenue, "realtime-metrics-revenue")
    reporter.start()

    logger.info("realtime_metrics streaming started — 3 queries active")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Interrupted — stopping streams")
    finally:
        reporter.stop()
        for q in (q_revenue, q_products, q_funnel):
            q.stop()
        spark.stop()


if __name__ == "__main__":
    run()

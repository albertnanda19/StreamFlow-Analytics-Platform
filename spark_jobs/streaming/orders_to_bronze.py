import argparse
import logging
import os
import shutil
import sys
import time

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    from_json,
    lit,
    size,
    to_date,
    to_timestamp,
    when,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.schemas import ORDER_PLACED_SCHEMA
from config.spark_config import SparkSessionBuilder
from utils.avro_deserializer import make_avro_deserializer_udf
from utils.metrics_reporter import StreamingMetricsReporter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("orders-to-bronze")

KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCHEMA_REG_URL    = os.getenv("SCHEMA_REGISTRY_URL",     "http://schema-registry:8081")
CLICKHOUSE_HOST   = os.getenv("CLICKHOUSE_HOST",          "clickhouse")
CLICKHOUSE_PORT   = os.getenv("CLICKHOUSE_PORT",          "8123")
CLICKHOUSE_USER   = os.getenv("CLICKHOUSE_USER",          "streamflow")
CLICKHOUSE_PASS   = os.getenv("CLICKHOUSE_PASSWORD",      "streamflow123")
CLICKHOUSE_DB     = os.getenv("CLICKHOUSE_DB",            "analytics")
BRONZE_PATH       = "s3a://bronze/orders"
CHECKPOINT_PATH   = SparkSessionBuilder.checkpoint_path("orders-bronze")
TRIGGER_INTERVAL  = "30 seconds"


def _validate(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_valid",
        (
            col("order_id").isNotNull()
            & (col("order_id") != "")
            & col("user_id").isNotNull()
            & (col("user_id") != "")
            & col("total_amount").isNotNull()
            & (col("total_amount") > 0)
        ),
    )


def _write_to_clickhouse(batch_df: DataFrame, batch_id: int) -> None:
    t0 = time.time()
    try:
        agg = batch_df.filter(col("is_valid")).selectExpr(
            "order_id",
            "user_id",
            "'' as product_id",
            "event_type",
            "'' as status",
            "total_amount",
            "CAST(0 AS INT) as quantity",
            "CAST(0.0 AS DOUBLE) as unit_price",
            "COALESCE(shipping_address.country, 'UNKNOWN') as country",
            "'' as user_segment",
            "'' as category",
            "event_timestamp",
        )
        agg.write.format("jdbc").options(
            url=f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}",
            driver="com.clickhouse.jdbc.ClickHouseDriver",
            dbtable="orders_realtime",
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
        ).mode("append").save()

        num_valid   = batch_df.filter(col("is_valid")).count()
        num_invalid = batch_df.filter(~col("is_valid")).count()
        elapsed_ms  = int((time.time() - t0) * 1000)

        logger.info(
            "batch_id=%d total=%d valid=%d invalid=%d ch_write_ms=%d",
            batch_id, num_valid + num_invalid, num_valid, num_invalid, elapsed_ms,
        )
    except Exception as exc:
        logger.error("ClickHouse write failed for batch %d: %s — skipping", batch_id, exc)


def build_stream(spark, deserialize_udf):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "orders")
        .option("startingOffsets", "earliest")
        .option("kafka.group.id", "spark-orders-bronze")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 10000)
        .load()
    )

    parsed = (
        raw
        .withColumn("json_str", deserialize_udf(col("value")))
        .filter(col("json_str").isNotNull())
        .withColumn("data", from_json(col("json_str"), ORDER_PLACED_SCHEMA))
        .select(
            col("data.event_id"),
            col("data.event_type"),
            col("data.order_id"),
            col("data.user_id"),
            col("data.session_id"),
            col("data.total_amount"),
            col("data.subtotal"),
            col("data.shipping_fee"),
            col("data.payment_method"),
            col("data.platform"),
            col("data.device_type"),
            col("data.shipping_address"),
            col("data.items"),
            col("data.coupon_code"),
            to_timestamp(expr("CAST(data.event_timestamp / 1000 AS BIGINT)")).alias("event_timestamp"),
            current_timestamp().alias("ingestion_timestamp"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            to_date(current_timestamp()).alias("processing_date"),
            lit("kafka_orders").alias("data_source"),
        )
    )

    return _validate(parsed)


def run(reset_checkpoint: bool = False) -> None:
    if reset_checkpoint:
        logger.warning("Resetting checkpoint at %s", CHECKPOINT_PATH)
        try:
            shutil.rmtree(CHECKPOINT_PATH.replace("s3a://", "/tmp/s3/"), ignore_errors=True)
        except Exception:
            pass

    spark = SparkSessionBuilder.build("streamflow-orders-bronze")
    spark.sparkContext.setLogLevel("WARN")

    deserialize_udf = make_avro_deserializer_udf(SCHEMA_REG_URL)

    stream_df = build_stream(spark, deserialize_udf)

    delta_query = (
        stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .partitionBy("processing_date")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(BRONZE_PATH)
    )

    ch_query = (
        stream_df.writeStream
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", CHECKPOINT_PATH + "-ch")
        .foreachBatch(_write_to_clickhouse)
        .start()
    )

    reporter = StreamingMetricsReporter(delta_query, "orders-to-bronze")
    reporter.start()

    logger.info("orders_to_bronze streaming started → %s", BRONZE_PATH)

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Interrupted — stopping streams")
    finally:
        reporter.stop()
        delta_query.stop()
        ch_query.stop()
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset-checkpoint", action="store_true")
    args = parser.parse_args()
    run(reset_checkpoint=args.reset_checkpoint)

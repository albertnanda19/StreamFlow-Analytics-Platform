import logging
import os
import sys
from datetime import datetime
from typing import Iterator, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    from_json,
    lit,
    to_date,
    to_timestamp,
    window,
)
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.schemas import INVENTORY_UPDATE_SCHEMA
from config.spark_config import SparkSessionBuilder
from utils.avro_deserializer import make_avro_deserializer_udf
from utils.metrics_reporter import StreamingMetricsReporter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("inventory-tracker")

KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCHEMA_REG_URL      = os.getenv("SCHEMA_REGISTRY_URL",     "http://schema-registry:8081")
INVENTORY_PATH      = "s3a://bronze/inventory-state"
ALERTS_PATH         = "s3a://bronze/alerts/low-stock"
CHECKPOINT_PATH     = SparkSessionBuilder.checkpoint_path("inventory-tracker")
ALERT_CHECKPOINT    = SparkSessionBuilder.checkpoint_path("inventory-alerts")
TRIGGER_INTERVAL    = "30 seconds"
LOW_STOCK_THRESHOLD = int(os.getenv("LOW_STOCK_THRESHOLD", "10"))

INVENTORY_STATE_SCHEMA = StructType([
    StructField("product_id",           StringType(),   False),
    StructField("current_quantity",     IntegerType(),  True),
    StructField("last_updated",         TimestampType(), True),
    StructField("total_sold_today",     IntegerType(),  True),
    StructField("total_restocked_today", IntegerType(), True),
    StructField("alert_triggered",      BooleanType(),  True),
])


def _update_inventory_state(
    product_id: str,
    events: Iterator,
    state: GroupState,
) -> Iterator[Tuple]:
    if state.hasTimedOut:
        state.remove()
        return

    if state.exists:
        current_qty, sold_today, restocked_today = (
            state.get[1], state.get[3], state.get[4]
        )
    else:
        current_qty    = 0
        sold_today     = 0
        restocked_today = 0

    last_updated = None

    for event in events:
        reason   = event["change_reason"]
        prev_qty = event["previous_quantity"]
        new_qty  = event["new_quantity"]
        delta    = new_qty - prev_qty

        current_qty = new_qty

        if reason == "SALE":
            sold_today += abs(delta)
        elif reason == "RESTOCK":
            restocked_today += abs(delta)

        last_updated = event["event_timestamp"]

    alert = current_qty < LOW_STOCK_THRESHOLD

    state.update((
        product_id,
        current_qty,
        last_updated,
        sold_today,
        restocked_today,
        alert,
    ))
    state.setTimeoutDuration(86_400_000)

    yield (
        product_id,
        current_qty,
        last_updated,
        sold_today,
        restocked_today,
        alert,
    )


def build_stream(spark, deserialize_udf):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "inventory")
        .option("startingOffsets", "latest")
        .option("kafka.group.id", "spark-inventory-tracker")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 5000)
        .load()
    )

    return (
        raw
        .withColumn("json_str", deserialize_udf(col("value")))
        .filter(col("json_str").isNotNull())
        .withColumn("data", from_json(col("json_str"), INVENTORY_UPDATE_SCHEMA))
        .select(
            col("data.product_id"),
            col("data.warehouse_id"),
            col("data.previous_quantity"),
            col("data.new_quantity"),
            col("data.change_reason"),
            col("data.reference_id"),
            to_timestamp(expr("CAST(data.event_timestamp / 1000 AS BIGINT)")).alias("event_timestamp"),
        )
        .filter(col("product_id").isNotNull())
    )


def _write_alerts(batch_df: DataFrame, batch_id: int) -> None:
    alerts = batch_df.filter(col("alert_triggered") == True)  # noqa: E712
    if alerts.count() > 0:
        logger.warning("Low-stock alerts triggered in batch %d", batch_id)
        (
            alerts
            .withColumn("alert_timestamp", current_timestamp())
            .withColumn("processing_date", to_date(current_timestamp()))
            .write
            .format("delta")
            .mode("append")
            .partitionBy("processing_date")
            .save(ALERTS_PATH)
        )


def run() -> None:
    spark = SparkSessionBuilder.build("streamflow-inventory-tracker")
    spark.sparkContext.setLogLevel("WARN")

    deserialize_udf = make_avro_deserializer_udf(SCHEMA_REG_URL)
    inventory_stream = build_stream(spark, deserialize_udf)

    windowed_state = (
        inventory_stream
        .withWatermark("event_timestamp", "5 minutes")
        .groupBy(
            window("event_timestamp", "1 day", "30 minutes"),
            col("product_id"),
        )
        .agg(
            expr("last(new_quantity)").alias("current_quantity"),
            expr("sum(CASE WHEN change_reason = 'SALE' THEN ABS(new_quantity - previous_quantity) ELSE 0 END)").alias("total_sold_today"),
            expr("sum(CASE WHEN change_reason = 'RESTOCK' THEN ABS(new_quantity - previous_quantity) ELSE 0 END)").alias("total_restocked_today"),
            expr("max(event_timestamp)").alias("last_updated"),
        )
        .withColumn("alert_triggered", col("current_quantity") < lit(LOW_STOCK_THRESHOLD))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("processing_date", to_date(current_timestamp()))
        .select(
            col("product_id"),
            col("current_quantity"),
            col("last_updated"),
            col("total_sold_today"),
            col("total_restocked_today"),
            col("alert_triggered"),
            col("_ingested_at"),
            col("processing_date"),
        )
    )

    state_query = (
        windowed_state.writeStream
        .format("delta")
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .partitionBy("processing_date")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(INVENTORY_PATH)
    )

    alert_query = (
        windowed_state.writeStream
        .outputMode("update")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", ALERT_CHECKPOINT)
        .foreachBatch(_write_alerts)
        .start()
    )

    reporter = StreamingMetricsReporter(state_query, "inventory-tracker")
    reporter.start()

    logger.info("inventory_tracker streaming started → %s", INVENTORY_PATH)

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Interrupted — stopping streams")
    finally:
        reporter.stop()
        state_query.stop()
        alert_query.stop()
        spark.stop()


if __name__ == "__main__":
    run()

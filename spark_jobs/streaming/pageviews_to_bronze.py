import argparse
import logging
import os
import sys

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    from_json,
    hour,
    lit,
    regexp_extract,
    to_date,
    to_timestamp,
    when,
    dayofweek,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.schemas import PAGEVIEW_SCHEMA, VALID_PAGE_TYPES
from config.spark_config import SparkSessionBuilder
from utils.avro_deserializer import make_avro_deserializer_udf
from utils.metrics_reporter import StreamingMetricsReporter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("pageviews-to-bronze")

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCHEMA_REG_URL   = os.getenv("SCHEMA_REGISTRY_URL",     "http://schema-registry:8081")
BRONZE_PATH      = "s3a://bronze/pageviews"
CHECKPOINT_PATH  = SparkSessionBuilder.checkpoint_path("pageviews-bronze")
TRIGGER_INTERVAL = "15 seconds"

_VALID_PAGE_TYPES_LIST = list(VALID_PAGE_TYPES)


def _extract_utm(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("utm_source",   regexp_extract(col("page_url"), r"utm_source=([^&]+)",   1))
        .withColumn("utm_medium",   regexp_extract(col("page_url"), r"utm_medium=([^&]+)",   1))
        .withColumn("utm_campaign", regexp_extract(col("page_url"), r"utm_campaign=([^&]+)", 1))
    )


def _classify_referrer(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "referrer_type",
        when(col("referrer_url").isNull(), lit("direct"))
        .when(col("referrer_url").contains("streamflow.io"), lit("internal"))
        .otherwise(lit("external")),
    )


def _mask_ip(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "ip_masked",
        regexp_extract(col("ip_address"), r"^(\d+\.\d+)\.", 1),
    )


def _validate(df: DataFrame) -> DataFrame:
    valid_types_expr = " OR ".join(
        [f"page_type = '{pt}'" for pt in _VALID_PAGE_TYPES_LIST]
    )
    return df.withColumn(
        "is_valid",
        (
            col("session_id").isNotNull()
            & col("page_url").isNotNull()
            & (col("page_url") != "")
            & col("event_timestamp").isNotNull()
            & expr(valid_types_expr)
        ),
    )


def build_stream(spark, deserialize_udf):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "pageviews")
        .option("startingOffsets", "latest")
        .option("kafka.group.id", "spark-pageviews-bronze")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 20000)
        .load()
    )

    parsed = (
        raw
        .withColumn("json_str", deserialize_udf(col("value")))
        .filter(col("json_str").isNotNull())
        .withColumn("data", from_json(col("json_str"), PAGEVIEW_SCHEMA))
        .select(
            col("data.*"),
            to_timestamp(expr("CAST(data.event_timestamp / 1000 AS BIGINT)")).alias("event_timestamp"),
            current_timestamp().alias("ingestion_timestamp"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            to_date(current_timestamp()).alias("processing_date"),
            lit("kafka_pageviews").alias("data_source"),
        )
        .withColumn("hour_of_day",  hour(col("event_timestamp")))
        .withColumn("day_of_week",  dayofweek(col("event_timestamp")))
    )

    enriched = _extract_utm(parsed)
    enriched = _classify_referrer(enriched)
    enriched = _mask_ip(enriched)
    enriched = enriched.drop("ip_address")

    return _validate(enriched)


def run(reset_checkpoint: bool = False) -> None:
    spark = SparkSessionBuilder.build("streamflow-pageviews-bronze")
    spark.sparkContext.setLogLevel("WARN")

    deserialize_udf = make_avro_deserializer_udf(SCHEMA_REG_URL)
    stream_df = build_stream(spark, deserialize_udf)

    query = (
        stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .partitionBy("processing_date", "hour_of_day")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(BRONZE_PATH)
    )

    reporter = StreamingMetricsReporter(query, "pageviews-to-bronze")
    reporter.start()

    logger.info("pageviews_to_bronze streaming started → %s", BRONZE_PATH)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Interrupted — stopping stream")
    finally:
        reporter.stop()
        query.stop()
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset-checkpoint", action="store_true")
    args = parser.parse_args()
    run(reset_checkpoint=args.reset_checkpoint)

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
AWS_KEY         = os.getenv("AWS_ACCESS_KEY_ID", "streamflow")
AWS_SECRET      = os.getenv("AWS_SECRET_ACCESS_KEY", "streamflow123")
CHECKPOINT_PATH = "s3a://checkpoints/orders-streaming"
BRONZE_PATH     = "s3a://bronze/orders"

ORDER_SCHEMA = StructType([
    StructField("event_id",         StringType()),
    StructField("order_id",         StringType()),
    StructField("user_id",          StringType()),
    StructField("product_id",       StringType()),
    StructField("event_type",       StringType()),
    StructField("status",           StringType()),
    StructField("quantity",         IntegerType()),
    StructField("unit_price",       DoubleType()),
    StructField("total_amount",     DoubleType()),
    StructField("country",          StringType()),
    StructField("user_segment",     StringType()),
    StructField("category",         StringType()),
    StructField("product_name",     StringType()),
    StructField("event_timestamp",  StringType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("streamflow-orders-streaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", AWS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def run():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "streamflow.orders.events")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream
        .select(from_json(col("value").cast("string"), ORDER_SCHEMA).alias("data"), col("timestamp"))
        .select("data.*", col("timestamp").alias("kafka_timestamp"))
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
        .withColumn("_processing_time", current_timestamp())
        .withColumn("_source", lit("kafka"))
    )

    bronze_writer = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .partitionBy("country")
        .start(BRONZE_PATH)
    )

    bronze_writer.awaitTermination()


if __name__ == "__main__":
    run()

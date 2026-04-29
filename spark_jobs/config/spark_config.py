import os

from pyspark.sql import SparkSession


class SparkSessionBuilder:
    @classmethod
    def build(
        cls,
        app_name: str,
        enable_delta: bool = True,
        enable_kafka: bool = True,
    ) -> SparkSession:
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        aws_key        = os.getenv("AWS_ACCESS_KEY_ID", "streamflow")
        aws_secret     = os.getenv("AWS_SECRET_ACCESS_KEY", "streamflow123")

        builder = SparkSession.builder.appName(app_name)

        if enable_delta:
            builder = builder.config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            ).config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )

        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint",                    minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key",                  aws_key)
            .config("spark.hadoop.fs.s3a.secret.key",                  aws_secret)
            .config("spark.hadoop.fs.s3a.path.style.access",           "true")
            .config("spark.hadoop.fs.s3a.impl",                        "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled",      "false")
            .config("spark.hadoop.fs.s3a.fast.upload",                 "true")
        )

        if enable_kafka:
            builder = builder.config(
                "spark.streaming.kafka.maxRatePerPartition", "1000"
            ).config(
                "spark.streaming.backpressure.enabled", "true"
            )

        builder = (
            builder
            .config("spark.sql.shuffle.partitions",  "8")
            .config("spark.default.parallelism",      "8")
            .config("spark.serializer",               "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.streaming.schemaInference", "true")
            .config("spark.sql.adaptive.enabled",     "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.streaming.metricsEnabled", "true")
        )

        return builder.getOrCreate()

    @classmethod
    def checkpoint_path(cls, job_name: str) -> str:
        base = os.getenv("CHECKPOINT_BASE", "s3a://checkpoints")
        return f"{base}/{job_name}/"

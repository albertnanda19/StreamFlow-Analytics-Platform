#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/submit_all.sh" --source-only 2>/dev/null || true

SPARK_MASTER="${SPARK_MASTER_URL:-spark://spark-master:7077}"
SPARK_HOME="${SPARK_HOME:-/opt/bitnami/spark}"
SUBMIT="${SPARK_HOME}/bin/spark-submit"
JOBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PACKAGES="io.delta:delta-spark_2.12:3.0.0,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.540,\
com.clickhouse:clickhouse-jdbc:0.6.0,\
org.apache.spark:spark-avro_2.12:3.5.0"

JOB="${1:-orders_to_bronze}"
LOG="/tmp/${JOB}.log"

echo "Submitting ${JOB} → master=${SPARK_MASTER}"

exec "${SUBMIT}" \
  --master "${SPARK_MASTER}" \
  --packages "${PACKAGES}" \
  --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT:-http://minio:9000}" \
  --conf "spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID:-streamflow}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY:-streamflow123}" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
  --py-files "${JOBS_DIR}/utils/avro_deserializer.py,${JOBS_DIR}/config/schemas.py,${JOBS_DIR}/utils/metrics_reporter.py" \
  "${JOBS_DIR}/streaming/${JOB}.py" \
  "$@"

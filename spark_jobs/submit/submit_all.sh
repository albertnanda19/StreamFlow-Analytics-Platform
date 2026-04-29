#!/usr/bin/env bash
set -euo pipefail

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

COMMON_CONF=(
  "--master"   "${SPARK_MASTER}"
  "--packages" "${PACKAGES}"
  "--conf"     "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp"
  "--conf"     "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
  "--conf"     "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  "--conf"     "spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT:-http://minio:9000}"
  "--conf"     "spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID:-streamflow}"
  "--conf"     "spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY:-streamflow123}"
  "--conf"     "spark.hadoop.fs.s3a.path.style.access=true"
  "--conf"     "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
  "--conf"     "spark.hadoop.fs.s3a.connection.ssl.enabled=false"
  "--py-files" "${JOBS_DIR}/utils/avro_deserializer.py,${JOBS_DIR}/config/schemas.py,${JOBS_DIR}/utils/metrics_reporter.py"
)

echo "=============================================="
echo " StreamFlow Spark Streaming Jobs Launcher"
echo "=============================================="
echo " Master : ${SPARK_MASTER}"
echo " Jobs   : ${JOBS_DIR}"
echo ""

echo "[1/4] Starting orders_to_bronze..."
"${SUBMIT}" "${COMMON_CONF[@]}" \
  "${JOBS_DIR}/streaming/orders_to_bronze.py" \
  > /tmp/orders_to_bronze.log 2>&1 &
ORDERS_PID=$!
echo "      PID=${ORDERS_PID}  log=/tmp/orders_to_bronze.log"

sleep 5

echo "[2/4] Starting pageviews_to_bronze..."
"${SUBMIT}" "${COMMON_CONF[@]}" \
  "${JOBS_DIR}/streaming/pageviews_to_bronze.py" \
  > /tmp/pageviews_to_bronze.log 2>&1 &
PAGEVIEWS_PID=$!
echo "      PID=${PAGEVIEWS_PID}  log=/tmp/pageviews_to_bronze.log"

sleep 5

echo "[3/4] Starting realtime_metrics..."
"${SUBMIT}" "${COMMON_CONF[@]}" \
  "${JOBS_DIR}/streaming/realtime_metrics.py" \
  > /tmp/realtime_metrics.log 2>&1 &
METRICS_PID=$!
echo "      PID=${METRICS_PID}  log=/tmp/realtime_metrics.log"

sleep 5

echo "[4/4] Starting inventory_tracker..."
"${SUBMIT}" "${COMMON_CONF[@]}" \
  "${JOBS_DIR}/streaming/inventory_tracker.py" \
  > /tmp/inventory_tracker.log 2>&1 &
INVENTORY_PID=$!
echo "      PID=${INVENTORY_PID}  log=/tmp/inventory_tracker.log"

echo ""
echo "All 4 streaming jobs submitted."
echo ""
echo "Monitor logs:"
echo "  tail -f /tmp/orders_to_bronze.log"
echo "  tail -f /tmp/pageviews_to_bronze.log"
echo "  tail -f /tmp/realtime_metrics.log"
echo "  tail -f /tmp/inventory_tracker.log"
echo ""
echo "Spark UI: http://localhost:8082"
echo ""
echo "Waiting for all jobs (CTRL+C to detach)..."
wait "${ORDERS_PID}" "${PAGEVIEWS_PID}" "${METRICS_PID}" "${INVENTORY_PID}"

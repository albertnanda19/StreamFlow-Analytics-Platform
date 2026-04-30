#!/usr/bin/env bash
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0
WARN=0

check() {
  local name="$1"
  local result="$2"
  local detail="${3:-}"
  if [[ "$result" == "ok" ]]; then
    printf "  ${GREEN}✔${NC}  %-45s %s\n" "$name" "${detail}"
    ((PASS++)) || true
  elif [[ "$result" == "warn" ]]; then
    printf "  ${YELLOW}⚠${NC}  %-45s %s\n" "$name" "${detail}"
    ((WARN++)) || true
  else
    printf "  ${RED}✗${NC}  %-45s %s\n" "$name" "${detail}"
    ((FAIL++)) || true
  fi
}

ping_http() {
  local url="$1"
  curl -sf --max-time 5 "$url" > /dev/null 2>&1 && echo "ok" || echo "fail"
}

ping_tcp() {
  local host="$1"
  local port="$2"
  (echo >/dev/tcp/"$host"/"$port") 2>/dev/null && echo "ok" || echo "fail"
}

echo ""
echo "================================================="
echo " STREAMFLOW PRE-TEST PREREQUISITE CHECK"
echo "================================================="
echo ""
echo "--- Core Infrastructure ---"

result=$(ping_tcp "localhost" "29092")
check "Kafka broker (localhost:29092)" "$result"

result=$(ping_http "http://localhost:8081/subjects")
check "Schema Registry (localhost:8081)" "$result"

result=$(ping_tcp "localhost" "5433")
check "PostgreSQL (localhost:5433)" "$result"

result=$(ping_tcp "localhost" "8123")
check "ClickHouse HTTP (localhost:8123)" "$result"

result=$(ping_http "http://localhost:9001/login")
check "MinIO Console (localhost:9001)" "$result"

result=$(ping_http "http://localhost:8088/health")
check "Airflow Webserver (localhost:8088)" "$result"

result=$(ping_http "http://localhost:3000/api/health")
check "Grafana (localhost:3000)" "$result"

result=$(ping_http "http://localhost:8090/v1/info")
check "Trino (localhost:8090)" "$result"

result=$(ping_http "http://localhost:8082")
check "Spark Master UI (localhost:8082)" "$result"

result=$(ping_http "http://localhost:8000/health" 2>/dev/null)
if [[ "$result" == "ok" ]]; then
  check "FastAPI (localhost:8000)" "ok"
else
  check "FastAPI (localhost:8000)" "warn" "(not running — serving layer tests will skip)"
fi

echo ""
echo "--- Kafka Topics ---"
TOPICS=("orders" "order-status" "pageviews" "inventory")
for topic in "${TOPICS[@]}"; do
  count=$(docker exec streamflow-kafka kafka-topics.sh \
    --bootstrap-server kafka:9092 --describe --topic "$topic" 2>/dev/null \
    | grep -c "PartitionCount" || echo "0")
  if [[ "$count" -ge 1 ]]; then
    check "Topic: $topic" "ok"
  else
    check "Topic: $topic" "fail" "(run: make kafka-topics)"
  fi
done

echo ""
echo "--- MinIO Buckets ---"
BUCKETS=("bronze" "silver" "gold" "checkpoints")
for bucket in "${BUCKETS[@]}"; do
  result=$(curl -sf --max-time 5 \
    "http://streamflow:streamflow123@localhost:9000/$bucket" > /dev/null 2>&1 \
    && echo "ok" || echo "warn")
  check "Bucket: $bucket" "$result"
done

echo ""
echo "--- ClickHouse Tables ---"
TABLES=("silver.silver_orders" "silver.silver_order_items" "gold.gold_daily_revenue" "gold.gold_user_segments")
for table in "${TABLES[@]}"; do
  count=$(curl -sf "http://localhost:8123/?query=SELECT+count()+FROM+${table}" \
    -u streamflow:streamflow123 2>/dev/null | tr -d ' \n' || echo "0")
  if [[ "$count" =~ ^[0-9]+$ ]] && [[ "$count" -gt 0 ]]; then
    check "Table: $table ($count rows)" "ok"
  elif [[ "$count" == "0" ]]; then
    check "Table: $table" "warn" "(empty — run: cd dbt_project && dbt run)"
  else
    check "Table: $table" "fail" "(table missing or ClickHouse unreachable)"
  fi
done

echo ""
echo "--- Event Producer Activity ---"
PAGEVIEWS_COUNT=$(curl -sf \
  "http://localhost:8123/?query=SELECT+count()+FROM+analytics.orders_realtime+WHERE+event_timestamp+%3E+now()-300" \
  -u streamflow:streamflow123 2>/dev/null | tr -d ' \n' || echo "0")
if [[ "$PAGEVIEWS_COUNT" =~ ^[0-9]+$ ]] && [[ "$PAGEVIEWS_COUNT" -gt 0 ]]; then
  check "Producer active (events in last 5 min: $PAGEVIEWS_COUNT)" "ok"
else
  check "Producer active" "warn" "(0 events in last 5min — run: make run-producer)"
fi

echo ""
echo "================================================="
TOTAL=$((PASS + FAIL + WARN))
echo " Checks: $TOTAL total | ${GREEN}$PASS passed${NC} | ${YELLOW}$WARN warnings${NC} | ${RED}$FAIL failed${NC}"
echo ""

if [[ "$FAIL" -gt 0 ]]; then
  echo -e " Status: ${RED}NO-GO${NC} — Fix failed checks before running tests"
  echo ""
  exit 1
elif [[ "$WARN" -gt 0 ]]; then
  echo -e " Status: ${YELLOW}GO (with warnings)${NC} — Some tests may be skipped"
  echo ""
  exit 0
else
  echo -e " Status: ${GREEN}GO${NC} — All prerequisites met, safe to run full test suite"
  echo ""
  exit 0
fi

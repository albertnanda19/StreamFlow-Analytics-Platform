# StreamFlow Operations Runbook

## Procedure 1: Restart a Failed Spark Streaming Job Without Data Loss

Spark's checkpoint mechanism ensures no data is lost or duplicated when a job restarts.

```bash
# 1. Identify which job failed
docker logs streamflow-spark-master 2>&1 | grep -i "error\|failed"

# 2. Check the streaming job container
docker ps -a | grep spark-job

# 3. Do NOT delete the checkpoint directory — this preserves exactly-once state
# Checkpoint location: s3a://checkpoints/<job-name>/

# 4. Resubmit the failed job (Spark reads checkpoint and resumes from last committed offset)
docker exec streamflow-spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/stream_orders.py

# 5. Verify the job resumed from the correct Kafka offset
docker logs streamflow-spark-master | grep "Starting micro-batch"
```

> **Important**: If you delete the checkpoint directory, the job will reprocess all Kafka messages from the beginning, causing duplicates in Bronze Delta Lake. Use `VACUUM` or a custom dedup job to clean up.

---

## Procedure 2: Re-run a Failed Airflow DAG for a Specific Date

```bash
# Option A: Via Airflow UI
# 1. Go to http://localhost:8088
# 2. Click on the DAG → Graph View → select failed run
# 3. Click "Clear" on the failed task → "Downstream" + "Future" checked = No
# 4. Confirm — Airflow will re-execute from the failed task

# Option B: Via Airflow CLI
docker exec streamflow-airflow-scheduler \
  airflow dags backfill bronze_to_silver \
  --start-date 2024-03-15 \
  --end-date 2024-03-15 \
  --reset-dagruns

# Option C: Via Airflow REST API
curl -X POST http://localhost:8088/api/v1/dags/bronze_to_silver/dagRuns \
  -u admin:admin123 \
  -H "Content-Type: application/json" \
  -d '{"execution_date": "2024-03-15T00:00:00+07:00", "conf": {}}'

# Verify the run
curl http://localhost:8088/api/v1/dags/bronze_to_silver/dagRuns \
  -u admin:admin123 | jq '.dag_runs[-1]'
```

---

## Procedure 3: Reset Kafka Consumer Group Offsets

```bash
# 1. Stop the Spark streaming job first (do NOT reset while consumer is active)
# Identify the consumer group
docker exec streamflow-kafka \
  kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 \
  --list | grep spark

# 2. Check current lag before reset
docker exec streamflow-kafka \
  kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 \
  --group spark-orders-bronze \
  --describe

# 3a. Reset to earliest (reprocess all historical messages)
docker exec streamflow-kafka \
  kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 \
  --group spark-orders-bronze \
  --topic orders \
  --reset-offsets --to-earliest \
  --execute

# 3b. Reset to specific timestamp (reprocess from a given point)
docker exec streamflow-kafka \
  kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 \
  --group spark-orders-bronze \
  --topic orders \
  --reset-offsets \
  --to-datetime 2024-03-15T00:00:00.000 \
  --execute

# 4. Also delete the Spark checkpoint to match the Kafka offset
docker exec streamflow-minio \
  mc rm --recursive --force minio/checkpoints/orders-bronze/

# 5. Restart the Spark job
```

---

## Procedure 4: Perform a Data Backfill from PostgreSQL

Use the `historical_backfill` Airflow DAG for date-range backfills (max 90 days).

```bash
# 1. Trigger via Airflow REST API with date range and target tables
curl -X POST http://localhost:8088/api/v1/dags/historical_backfill/dagRuns \
  -u admin:admin123 \
  -H "Content-Type: application/json" \
  -d '{
    "conf": {
      "start_date": "2024-01-01",
      "end_date": "2024-01-31",
      "tables": ["orders", "order_items"]
    }
  }'

# 2. Monitor progress
curl http://localhost:8088/api/v1/dags/historical_backfill/dagRuns \
  -u admin:admin123 | jq '.dag_runs[-1].state'

# 3. After backfill completes, run dbt to propagate to Silver/Gold
docker exec streamflow-airflow-worker \
  bash -c "cd /opt/airflow/dbt_project && dbt run --select silver --target prod"

# 4. Validate the backfilled data
python data_quality/run_checkpoints.py --checkpoint silver --fail-on-error
```

---

## Procedure 5: Add a New Kafka Topic and Schema

```bash
# 1. Create the Avro schema file
cat > producer/schemas/new_event.avsc << 'EOF'
{
  "type": "record",
  "name": "NewEvent",
  "namespace": "com.streamflow.events",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "event_timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "new_field", "type": ["null", "string"], "default": null}
  ]
}
EOF

# 2. Register schema in Schema Registry
curl -X POST http://localhost:8081/subjects/new-event-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "{\"schema\": $(cat producer/schemas/new_event.avsc | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')}"

# 3. Create the Kafka topic
docker exec streamflow-kafka \
  kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --create \
  --topic new-event \
  --partitions 6 \
  --replication-factor 1

# 4. Create a new PySpark streaming job: spark/jobs/stream_new_event.py
# (Copy stream_orders.py as template, modify schema reading and ClickHouse table)

# 5. Add Bronze table in ClickHouse
docker exec streamflow-clickhouse \
  clickhouse-client --query "
  CREATE TABLE IF NOT EXISTS bronze.new_event (
    event_id String,
    event_timestamp DateTime,
    new_field Nullable(String),
    ingestion_timestamp DateTime DEFAULT now()
  ) ENGINE = MergeTree()
  ORDER BY (event_timestamp, event_id)"

# 6. Update data_quality/expectations/bronze_new_event_suite.json
# 7. Update Airflow bronze_to_silver.py to include the new streaming job monitoring
```

---

## Procedure 6: Add a New dbt Model to the Pipeline

```bash
# 1. Create the model file
cat > dbt_project/models/silver/silver_new_entity.sql << 'EOF'
{{ config(materialized='incremental', unique_key='entity_id', tags=['silver']) }}

with source as (
    select * from {{ source('bronze', 'new_event') }}
    {% if is_incremental() %}
    where ingestion_timestamp > {{ incremental_predicate('ingestion_timestamp') }}
    {% endif %}
)
select
    event_id as entity_id,
    event_timestamp,
    new_field,
    current_timestamp() as dbt_updated_at
from source
EOF

# 2. Add to models/silver/schema.yml under models:
#   - name: silver_new_entity
#     columns:
#       - name: entity_id
#         tests: [not_null, unique]

# 3. Test the model locally first (uses DuckDB)
cd dbt_project && dbt run --select silver_new_entity --target dev
dbt test --select silver_new_entity --target dev

# 4. Run against production ClickHouse
dbt run --select silver_new_entity --target prod
dbt test --select silver_new_entity --target prod

# 5. Update bronze_to_silver Airflow DAG to include the new model:
# Add to silver_dbt_runs TaskGroup:
# BashOperator(task_id="run_dbt_silver_new_entity",
#              bash_command=f"{DBT_CMD} run --select silver_new_entity {DBT_FLAGS}")

# 6. Generate and serve updated docs
dbt docs generate && dbt docs serve --port 8090
```

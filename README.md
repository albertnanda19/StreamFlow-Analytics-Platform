# StreamFlow Analytics Platform

A production-grade, end-to-end real-time e-commerce data pipeline and analytics platform built with a **Lakehouse + Medallion Architecture** (Bronze → Silver → Gold).

## Architecture

```
Sources          Ingestion         Processing         Storage              Serving
─────────────────────────────────────────────────────────────────────────────────
Python Simulator ──► Kafka         ──► PySpark       ──► MinIO/Delta Lake ──► Trino
REST API         ──► Schema Reg    ──► Structured     ──► ClickHouse       ──► Grafana
PostgreSQL CDC   ──► Debezium      ──► Streaming      ──► (Medallion)      ──► Superset
CSV Batch        ──► Kafka Connect ──► dbt batch                           ──► BI Tools
                                         │
                                   Airflow (orchestration)
                                   Great Expectations (quality)
```

## Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Apache Kafka 7.5 + Schema Registry + Debezium 2.4 |
| Processing | PySpark 3.5 Structured Streaming + dbt Core |
| Storage | MinIO (S3) + Delta Lake |
| OLAP | ClickHouse 23.8 |
| Serving | Trino 426 + Grafana 10.2 |
| Orchestration | Apache Airflow 2.8 + Celery + Redis |
| Quality | Great Expectations 0.18 + dbt tests |
| Infrastructure | Docker Compose (local) + Terraform (GCP) |

## Quick Start

```bash
# 1. Copy environment variables
cp .env.example .env

# 2. Start all services
make up

# 3. Wait for services to be healthy (~3-5 minutes)
make status

# 4. Create Kafka topics
make kafka-topics

# 5. Register Debezium CDC connector
make register-debezium

# 6. Start event producer
make run-producer
```

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8080 | — |
| Schema Registry | http://localhost:8081 | — |
| Airflow | http://localhost:8088 | admin / admin123 |
| MinIO Console | http://localhost:9001 | streamflow / streamflow123 |
| Grafana | http://localhost:3000 | admin / admin123 |
| Spark Master UI | http://localhost:8082 | — |
| Trino | http://localhost:8090 | — |
| Prometheus | http://localhost:9090 | — |
| Debezium Connect | http://localhost:8083 | — |
| ClickHouse HTTP | http://localhost:8123 | streamflow / streamflow123 |

## Validation Checklist

### Kafka
```bash
docker exec streamflow-kafka kafka-broker-api-versions --bootstrap-server localhost:9092
docker exec streamflow-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### MinIO Buckets
```bash
# Via MinIO Console → http://localhost:9001
# Or via CLI:
docker exec streamflow-minio-init mc ls myminio
```

### PostgreSQL Tables
```bash
docker exec streamflow-postgres psql -U streamflow -d streamflow_source \
  -c "\dt" -c "SELECT COUNT(*) FROM users;" -c "SELECT COUNT(*) FROM products;"
```

### Schema Registry
```bash
curl http://localhost:8081/subjects
curl http://localhost:8081/config
```

### Airflow Webserver
```bash
curl http://localhost:8088/health
```

### Spark Master
```bash
curl http://localhost:8082/
# Verify workers: should show 2 workers registered
```

## Project Structure

```
streamFlow-analytics-platform/
├── docker-compose.yml          # All 20+ services
├── docker-compose.override.yml # Local dev overrides
├── .env.example                # Environment template
├── Makefile                    # Platform management commands
├── infrastructure/
│   ├── terraform/              # GCP cloud provisioning
│   ├── docker/                 # Custom Dockerfiles
│   ├── postgres/               # DB init + seed data
│   ├── clickhouse/             # ClickHouse config + init
│   └── debezium/               # CDC connector config
├── producer/                   # Event simulator (Python)
├── spark_jobs/
│   ├── streaming/              # PySpark Structured Streaming
│   └── batch/                  # Batch transformation jobs
├── dbt_project/                # dbt transformations (Bronze→Silver→Gold)
├── airflow/dags/               # Orchestration DAGs
├── data_quality/               # Great Expectations
├── serving/
│   ├── trino/                  # Federated query engine
│   ├── grafana/                # Dashboards + provisioning
│   └── prometheus/             # Metrics collection
└── docs/                       # Architecture documentation
```

## Make Commands

```bash
make up                   # Start all services
make down                 # Stop all services
make restart              # Restart all services
make status               # Health status of all services
make logs service=kafka   # Tail service logs

make kafka-topics         # Create all Kafka topics
make register-schemas     # Register Avro schemas
make register-debezium    # Register CDC connector

make run-producer         # Start event simulator
make run-spark-streaming  # Submit streaming job
make run-dbt              # Run dbt transformations
make test-dbt             # Run dbt tests
make quality-check        # Run GE checkpoints
make grafana-setup        # Import dashboards
```

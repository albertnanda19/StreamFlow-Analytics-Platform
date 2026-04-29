# StreamFlow Analytics Platform

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5-red?logo=apachekafka)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange?logo=apachespark)
![dbt](https://img.shields.io/badge/dbt-1.7-green?logo=dbt)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-blue?logo=apacheairflow)
![ClickHouse](https://img.shields.io/badge/ClickHouse-23.x-yellow?logo=clickhouse)
![Great Expectations](https://img.shields.io/badge/Great%20Expectations-0.18-purple)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![License](https://img.shields.io/badge/License-MIT-lightgrey)

---

## Overview

**StreamFlow Analytics Platform** is a production-grade, end-to-end data engineering portfolio project that simulates the complete data infrastructure of a mid-sized Indonesian e-commerce company. It demonstrates the full modern data engineering lifecycle: real-time event ingestion, stream processing, lakehouse batch transformation, OLAP serving, orchestration, data quality validation, and analytics dashboards.

The platform processes simulated order placements, pageviews, and inventory changes continuously — ingesting thousands of events per minute through Apache Kafka, processing them in real time with PySpark Structured Streaming, transforming them through a Medallion Architecture (Bronze → Silver → Gold) using dbt Core, and serving rich analytics through Grafana dashboards and a FastAPI REST layer.

Every design decision in StreamFlow reflects real-world trade-offs: Avro with Schema Registry for backward-compatible schema evolution, Delta Lake for ACID compliance in the data lake, ClickHouse for sub-second OLAP queries, Great Expectations for automated data observability, and Airflow for reliable orchestration with built-in SLA monitoring and alerting.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                     │
│  Python Event Simulator  │  PostgreSQL (CDC via Debezium)  │  CSV Batch  │
└────────────┬─────────────┴────────────────┬────────────────┴─────────────┘
             │                              │
             ▼                              ▼
┌─────────────────────────┐    ┌──────────────────────────┐
│   Apache Kafka (7.5)    │    │   Debezium CDC Connector  │
│   + Schema Registry     │    │   (PostgreSQL → Kafka)    │
│   Topics: orders,       │    └──────────┬───────────────┘
│   pageviews, inventory, │               │
│   order-status          │               │
└────────────┬────────────┘               │
             │◄──────────────────────────┘
             ▼
┌─────────────────────────────────────────────────┐
│     PySpark Structured Streaming (3.5)          │
│     - 4 streaming jobs with watermarking        │
│     - Exactly-once via checkpointing            │
│     - Writes to Bronze + ClickHouse realtime    │
└────────┬────────────────────────────────────────┘
         │
         ▼
┌────────────────────────────────────┐
│   BRONZE — MinIO (S3-compatible)   │
│   Delta Lake format (ACID)         │
│   s3a://bronze/orders/             │
│   s3a://bronze/pageviews/          │
│   s3a://bronze/inventory-state/    │
└────────┬───────────────────────────┘
         │  Orchestrated by Airflow 2.8
         ▼
┌────────────────────────────────────┐
│   dbt Core — Batch Transformation  │
│   SILVER: dedup, enrich, validate  │
│   GOLD: aggregations, RFM, funnel  │
│   MARTS: executive KPIs, recs      │
└────────┬───────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────┐
│           ClickHouse OLAP (23.x)                         │
│   bronze.*  │  silver.*  │  gold.*  │  analytics.*       │
└──────┬───────────────────────────────────────────────────┘
       │
       ├──► Trino Federated Queries (cross-source SQL)
       ├──► Grafana Dashboards (port 3000)
       └──► FastAPI REST Layer (port 8000)

┌──────────────────────────────────────────────────────────┐
│           Cross-cutting Concerns                         │
│   Great Expectations DQ  │  Data Docs (port 8085)        │
│   Airflow SLA Monitoring │  Slack Alerting               │
│   Prometheus + Grafana   │  PostgreSQL audit logs         │
└──────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Component | Technology | Version | Purpose |
|---|---|---|---|
| Event Streaming | Apache Kafka | 7.5.0 | Real-time message backbone |
| Schema Management | Confluent Schema Registry | 7.5.0 | Avro schema versioning |
| Stream Processing | PySpark Structured Streaming | 3.5.0 | Real-time data processing |
| Data Lake Storage | MinIO + Delta Lake | Latest | S3-compatible lakehouse |
| Batch Transformation | dbt Core | 1.7.x | SQL-based Medallion transforms |
| OLAP Engine | ClickHouse | 23.x | Sub-second analytical queries |
| Orchestration | Apache Airflow | 2.8.x | DAG scheduling and monitoring |
| Data Quality | Great Expectations | 0.18.x | Automated validation |
| Federated Queries | Trino | 435 | Cross-source SQL |
| Dashboards | Grafana | 10.x | Business intelligence |
| API Layer | FastAPI | 0.109 | REST serving layer |
| CDC | Debezium | 2.4 | PostgreSQL change capture |
| Message Format | Apache Avro | 1.11 | Schema-enforced serialization |
| Containerization | Docker Compose | 2.x | Local orchestration |
| Monitoring | Prometheus | 2.47 | Metrics collection |
| OLTP | PostgreSQL | 15 | Transactional source DB |

---

## Data Engineering Principles Applied

### Medallion Architecture
Bronze stores raw, unmodified Avro events directly from Kafka — no filtering, no transformation. Silver applies deduplication (using `row_number()` window functions on unique keys), enrichment (joining with PostgreSQL user/product tables), and schema standardization. Gold provides pre-aggregated business metrics (daily revenue, RFM segments, conversion funnels) materialized as full-refresh tables for sub-millisecond dashboard queries.

### Exactly-Once Semantics
PySpark checkpointing writes offset progress to MinIO before committing data to Delta Lake. If a Spark job crashes, it resumes from the last committed checkpoint — no records are lost or duplicated. Delta Lake's transaction log provides ACID guarantees at the storage layer.

### Schema Evolution
All Kafka topics use Avro with backward-compatible schema evolution enforced by Confluent Schema Registry. Producers can add optional fields without breaking existing consumers — the Schema Registry rejects incompatible changes before they reach production.

### Idempotency
dbt Silver models use `unique_key` with `delete+insert` incremental strategy — re-running the same DAG twice produces identical results. Airflow sets `max_active_runs=1` and uses `AirflowSkipException` when no fresh Bronze data exists, preventing redundant downstream runs.

### Data Observability
Great Expectations validates 50+ expectations across all layers. The `DeltaFreshnessSensor` detects stale Bronze data before triggering dbt runs. Grafana's Pipeline Health panel shows the real-time DQ check status. Every validation result is persisted to PostgreSQL `gx_validation_results` for trend analysis.

### Change Data Capture (CDC)
Debezium connects to PostgreSQL's binary replication log (WAL) and streams row-level changes to Kafka — without polling and without impacting the source database. This enables the pipeline to react to OLTP changes in near-real-time without full table scans.

---

## Project Structure

```
streamflow-analytics-platform/
├── producer/               # Kafka event producer + Avro schemas
│   └── schemas/            # .avsc schema definitions
├── spark/                  # PySpark Structured Streaming jobs
│   └── jobs/               # 4 streaming job scripts
├── dbt_project/            # dbt Core transformation project
│   ├── models/silver/      # 5 Silver layer models
│   ├── models/gold/        # 4 Gold layer models
│   ├── models/marts/       # 2 Mart models
│   ├── macros/             # safe_divide, incremental_predicate, generate_surrogate_key
│   ├── snapshots/          # SCD Type 2 product price snapshot
│   └── seeds/              # Reference CSV files
├── airflow/
│   ├── dags/               # 5 production DAGs
│   ├── plugins/            # Custom Hook, Operator, Sensor
│   └── scripts/            # Connection + alerting setup
├── data_quality/           # Great Expectations project
│   ├── expectations/       # 5 expectation suites
│   ├── checkpoints/        # 3 GX checkpoints
│   └── plugins/            # 2 custom expectations
├── serving/
│   ├── trino/              # Trino coordinator config + 3 catalogs
│   ├── grafana/            # Dashboards + provisioning
│   └── api/                # FastAPI analytics REST layer
├── infrastructure/
│   ├── docker/             # Dockerfiles per service
│   ├── postgres/           # DB initialization SQL
│   └── terraform/          # Cloud infrastructure as code
├── tests/
│   └── e2e/                # End-to-end pytest suite
├── docs/                   # ADRs, data dictionary, runbook
├── docker-compose.yml      # Full 22-service stack
├── Makefile                # Developer convenience commands
└── .env.example            # Environment variable template
```

---

## Quick Start

### Prerequisites
- Docker Desktop (16 GB RAM recommended)
- Git
- GNU Make

```bash
# Step 1: Clone
git clone https://github.com/albertnanda19/StreamFlow-Analytics-Platform.git
cd StreamFlow-Analytics-Platform

# Step 2: Configure environment
cp .env.example .env
# Edit .env if needed (defaults work for local dev)

# Step 3: Start core infrastructure
make up
# Wait ~3 minutes for all services to initialize

# Step 4: Initialize infrastructure
make kafka-topics          # Create Kafka topics
make register-schemas      # Register Avro schemas in Schema Registry
make init-postgres         # Seed PostgreSQL with reference data
make register-debezium     # Start CDC connector

# Step 5: Start data generation
make run-producer          # Event producer (continuous background stream)

# Step 6: Start stream processing
make run-spark-streaming   # Submit all 4 Spark jobs to the cluster

# Step 7: Run batch pipeline
# Trigger manually in Airflow UI:
open http://localhost:8088  # Trigger bronze_to_silver DAG

# Step 8: View analytics
open http://localhost:3000  # Grafana dashboards (admin/admin)
open http://localhost:8000/docs  # FastAPI Swagger UI
open http://localhost:8085  # Great Expectations Data Docs
```

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin |
| Airflow | http://localhost:8088 | admin / admin123 |
| Kafka UI | http://localhost:8080 | No auth |
| Spark Master UI | http://localhost:8082 | No auth |
| MinIO Console | http://localhost:9001 | streamflow / streamflow123 |
| Schema Registry | http://localhost:8081 | No auth |
| ClickHouse HTTP | http://localhost:8123 | streamflow / streamflow123 |
| Trino | http://localhost:8090 | No auth |
| Data Docs (GX) | http://localhost:8085 | No auth |
| FastAPI Docs | http://localhost:8000/docs | No auth |
| Prometheus | http://localhost:9090 | No auth |
| Debezium | http://localhost:8083 | No auth |

---

## Data Flow: One Order Event's Journey

```
T+0.0s   Python producer creates order event → serializes to Avro
T+0.1s   Schema Registry validates schema compatibility
T+0.1s   Event published to Kafka topic 'orders' (partition by user_id)
T+2-5s   PySpark reads micro-batch from Kafka (trigger: 30s interval)
T+5-10s  Spark validates schema → writes to Bronze Delta Lake in MinIO
T+5-10s  Spark simultaneously inserts to ClickHouse orders_realtime
T+10s    Grafana auto-refresh shows updated real-time order count
T+60min  Airflow bronze_to_silver DAG triggers (hourly at minute 0)
T+65min  GX bronze checkpoint validates batch — DeltaFreshnessSensor confirms fresh data
T+70min  dbt silver_orders runs: dedup + user enrichment + feature engineering
T+75min  dbt test --select silver validates 20+ expectations
T+80min  Pipeline metadata written to PostgreSQL pipeline_runs
T+01:30  silver_to_gold DAG triggers (daily at 01:30 AM WIB)
T+01:45  Gold aggregations (revenue, RFM, funnel) refreshed in ClickHouse
T+01:50  Grafana Gold layer dashboards reflect new daily aggregations
```

---

## Data Model

### Silver Layer

**silver_orders** — Deduplicated, enriched order events

| Column | Type | Description |
|---|---|---|
| order_id | UUID | Primary key — unique order identifier |
| user_id | UUID | Purchasing user |
| user_email | String | Enriched from PostgreSQL (PII) |
| user_segment | Enum | NEW / REGULAR / VIP / UNKNOWN |
| total_amount | Float | Full order value in IDR |
| gross_revenue | Float | Revenue excluding shipping |
| payment_method | Enum | CREDIT_CARD / DEBIT_CARD / BANK_TRANSFER / EWALLET / COD |
| device_type | Enum | MOBILE / DESKTOP / TABLET |
| is_high_value | Boolean | total_amount > IDR 500,000 |
| order_item_count | Int | Number of line items |
| event_timestamp | Timestamp | When order was placed |
| dbt_updated_at | Timestamp | Last dbt processing time |

**silver_sessions** — Aggregated session records

| Column | Type | Description |
|---|---|---|
| session_id | UUID | Primary key |
| session_duration_seconds | Int | End minus start |
| page_count | Int | Total pageviews in session |
| is_bounce | Boolean | Session had only 1 pageview |
| has_converted | Boolean | Session resulted in an order |
| viewed_products | Array | Product IDs browsed |
| utm_source | String | Acquisition channel |

### Gold Layer

**gold_daily_revenue** — Daily revenue aggregations

| Column | Type | Description |
|---|---|---|
| revenue_date | Date | Primary key |
| total_revenue | Float | Sum of all order amounts |
| order_count | Int | Number of orders |
| avg_order_value | Float | Average order size |
| dod_growth_pct | Float | Day-over-day growth (%) |
| revenue_7d_avg | Float | 7-day rolling average |

**gold_user_segments** — RFM segmentation

| Column | Type | Description |
|---|---|---|
| user_id | UUID | Primary key |
| recency_days | Int | Days since last order |
| frequency | Int | Total order count |
| monetary | Float | Total lifetime spend |
| r_score / f_score / m_score | Int | RFM scores (1-5) |
| segment | Enum | Champions / Loyal / At Risk / Lost / New / Potential / Others |

---

## Makefile Commands

| Command | Description |
|---|---|
| `make up` | Start all Docker Compose services |
| `make down` | Stop and remove all containers |
| `make logs` | Tail all service logs |
| `make kafka-topics` | Create all Kafka topics |
| `make register-schemas` | Register Avro schemas in Schema Registry |
| `make init-postgres` | Initialize PostgreSQL with seed data |
| `make register-debezium` | Deploy Debezium CDC connector |
| `make run-producer` | Start event producer |
| `make run-spark-streaming` | Submit all PySpark streaming jobs |
| `make dbt-run` | Run all dbt models (dev target) |
| `make dbt-test` | Run all dbt tests |
| `make dbt-docs` | Generate and serve dbt docs |
| `make gx-bronze` | Run GX bronze checkpoint |
| `make gx-silver` | Run GX silver checkpoint |
| `make gx-all` | Run all GX checkpoints |
| `make e2e-test` | Run end-to-end test suite |
| `make airflow-setup` | Initialize Airflow connections |
| `make clean` | Remove all volumes and reset state |

---

## Troubleshooting

| Issue | Cause | Fix |
|---|---|---|
| Kafka topics not created | Kafka not ready yet | `make kafka-topics` after `docker ps` shows kafka healthy |
| Schema Registry 500 errors | Zookeeper connection lost | `docker restart streamflow-schema-registry` |
| Spark job exits immediately | Delta Lake jars not loaded | Check `spark/jobs/` Dockerfile for correct jar versions |
| dbt connection refused | ClickHouse not running | `docker ps \| grep clickhouse` — check health status |
| Airflow DAG not visible | DAG parse error | `docker logs streamflow-airflow-scheduler \| grep ERROR` |
| Bronze tables empty | Producer not running | `make run-producer` to restart event generation |
| GX checkpoint fails | ClickHouse schema mismatch | Run `dbt run` first to create tables, then retry GX |
| Grafana shows "No Data" | Wrong datasource UID | Re-provision: `docker restart streamflow-grafana` |
| MinIO bucket not found | MinIO startup delay | Wait 60s then `make init-minio` |
| ClickHouse OOM error | Query too large | Add `LIMIT` clause or use `gold.*` tables instead of `bronze.*` |

---

## Portfolio Talking Points

See [`docs/portfolio_guide.md`](docs/portfolio_guide.md) for detailed interview preparation including design decisions, trade-offs, and production scaling considerations.

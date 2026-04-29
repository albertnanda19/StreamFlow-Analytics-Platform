# Architecture Decision Records

## ADR-001: Delta Lake over Plain Parquet for Bronze Layer

**Date**: 2024-01-15
**Status**: Accepted

### Context
The Bronze layer receives high-throughput streaming writes from PySpark. We needed a storage format that supports concurrent writes from multiple Spark workers, handles late-arriving data gracefully, and allows re-processing without duplicates.

### Decision
Use Delta Lake (via `delta-spark` library) on top of MinIO instead of plain Parquet files.

### Consequences
**Positive**: ACID transactions prevent partial writes from corrupting the table; the Delta transaction log enables time-travel queries for debugging; `MERGE INTO` supports upserts for deduplication at the Bronze-to-Silver boundary; Z-ordering on `ingestion_timestamp` improves scan performance by 3-5x for time-range queries.
**Negative**: Delta Lake adds a `_delta_log/` directory overhead; requires the `delta-spark` JAR on every Spark worker; slightly higher write latency (~10-15%) vs raw Parquet due to transaction log commits.
**Trade-off considered**: Apache Iceberg was evaluated but Delta Lake has better Spark native integration and simpler local development setup without a separate catalog service.

---

## ADR-002: ClickHouse over Druid/Pinot for OLAP

**Date**: 2024-01-20
**Status**: Accepted

### Context
The platform needs sub-second analytical queries on 100M+ row datasets for Grafana dashboards. We evaluated Apache Druid, Apache Pinot, and ClickHouse.

### Decision
Use ClickHouse as the primary OLAP engine for both real-time ingestion (from Spark) and batch analytics (from dbt).

### Consequences
**Positive**: ClickHouse's MergeTree engine achieves 10-100x faster aggregation queries than PostgreSQL on the same hardware; single binary deployment (no Zookeeper dependency like Druid); native support for `ReplacingMergeTree` handles upserts elegantly for Silver layer; excellent dbt adapter (`dbt-clickhouse`); supports both streaming inserts and batch loads in the same table.
**Negative**: ClickHouse is not ACID-compliant for concurrent updates (by design); JOINs are intentionally limited — wide denormalization is preferred; SQL dialect differences from standard SQL require adapter-specific dbt macros.
**Trade-off considered**: Pinot was rejected due to its operational complexity (requires Zookeeper + Helix controller). Druid was rejected due to its requirement for a separate metadata store and its weaker dbt integration.

---

## ADR-003: dbt Core for Batch Transforms over Pure PySpark

**Date**: 2024-02-01
**Status**: Accepted

### Context
The Silver and Gold transformation logic involves complex SQL operations: deduplication, enrichment joins, window functions, and business aggregations. We could implement these in PySpark DataFrames or in dbt SQL models.

### Decision
Use dbt Core for all batch transformations (Bronze → Silver → Gold → Marts).

### Consequences
**Positive**: SQL is more readable and reviewable than equivalent PySpark DataFrame code; dbt's `ref()` function provides automatic dependency resolution and incremental build; built-in testing (`not_null`, `unique`, `accepted_values`, `relationships`) adds a data quality layer without extra tooling; `dbt docs generate` creates a living data dictionary automatically; the `--select` flag enables granular re-runs without full pipeline execution.
**Negative**: dbt cannot process binary formats (Avro, Parquet) directly — it operates on SQL-accessible tables, so the Bronze layer still requires PySpark for format conversion; dbt's Jinja templating adds cognitive overhead for complex macros; no native streaming support (batch-only).
**Trade-off considered**: Pure PySpark would be more flexible for complex ML feature engineering but loses the SQL readability, built-in testing, and documentation benefits of dbt. The hybrid approach (PySpark for streaming ingestion, dbt for batch transforms) gives us the best of both.

---

## ADR-004: Great Expectations over Custom Validation Scripts

**Date**: 2024-02-10
**Status**: Accepted

### Context
Data quality validation is required at every layer. Options include custom Python scripts, dbt tests only, or a dedicated DQ framework.

### Decision
Use Great Expectations (GX) v0.18 OSS as the primary data quality framework, complementing (not replacing) dbt generic tests.

### Consequences
**Positive**: GX provides a rich expectation library (100+ built-in expectations) covering statistical checks (mean, median, percentiles) that dbt tests cannot express; the `Data Docs` portal auto-generates a human-readable DQ report with historical pass/fail trends; custom expectations (`ColumnMapExpectation`, `QueryExpectation`) allow cross-table validation impossible with dbt tests; GX results are structured JSON — easy to store in PostgreSQL and visualize in Grafana; Airflow integration via `PythonOperator` allows DQ gates in the pipeline.
**Negative**: GX has a steeper learning curve than dbt tests; version 0.18 is a major API refactor from 0.15 — documentation gaps exist; `Data Docs` requires a separate web server to be accessible team-wide.
**Trade-off considered**: dbt tests alone cover schema-level validation well but lack statistical thresholds (e.g., "bounce rate must be between 20-80%") and cross-table consistency checks. The combination of dbt tests (schema/referential integrity) + GX (statistical bounds, custom business rules) gives comprehensive coverage.

---

## ADR-005: Avro + Schema Registry over JSON for Kafka Messages

**Date**: 2024-01-10
**Status**: Accepted

### Context
Kafka topics carry high-throughput event data. Message format options include JSON, Protocol Buffers, Avro, and plain text.

### Decision
Use Apache Avro with Confluent Schema Registry for all Kafka topic message serialization.

### Consequences
**Positive**: Avro binary encoding is 3-5x smaller than JSON for the same data — reduces Kafka broker storage and network I/O significantly at scale; Schema Registry enforces backward compatibility — producers cannot publish a schema that would break existing consumers; schema evolution rules (adding optional fields with defaults) are explicit and auditable; PySpark's `from_avro` function integrates natively with Schema Registry for streaming deserialization.
**Negative**: Avro is not human-readable — Kafka UI consumers need schema-aware deserializers; Schema Registry is an additional service to maintain; schema ID lookup adds a small network round-trip per producer batch.
**Trade-off considered**: JSON was rejected for production because schema enforcement is optional — a producer bug adding a wrong field type would silently corrupt downstream consumers. Protocol Buffers were evaluated but have weaker Python tooling and less native Kafka ecosystem support than Avro.

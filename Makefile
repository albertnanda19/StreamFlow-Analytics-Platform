SHELL := /bin/bash
.DEFAULT_GOAL := help

include .env
export

DOCKER_COMPOSE := docker compose
SERVICE ?= ""
EPS     ?= 10
DURATION ?= ""

.PHONY: help up down restart logs status \
        kafka-topics init-postgres register-schemas register-debezium \
        create-topics \
        run-producer run-producer-burst run-producer-slow \
        run-spark-streaming run-dbt test-dbt \
        quality-check grafana-setup \
        producer-local schemas-local topics-local debezium-local

help:
	@echo "StreamFlow Analytics Platform - Available Commands"
	@echo "=================================================="
	@echo ""
	@echo "Infrastructure:"
	@echo "  make up               Start all services"
	@echo "  make down             Stop all services"
	@echo "  make restart          Restart all services"
	@echo "  make status           Show health status of all services"
	@echo "  make logs service=X   Tail logs for a specific service"
	@echo ""
	@echo "Setup:"
	@echo "  make kafka-topics        Create all Kafka topics (via docker exec)"
	@echo "  make create-topics       Create topics via Python script (local)"
	@echo "  make init-postgres       Run PostgreSQL initialization"
	@echo "  make register-schemas    Register Avro schemas to Schema Registry"
	@echo "  make register-debezium   Register Debezium CDC connector"
	@echo ""
	@echo "Producer (Docker):"
	@echo "  make run-producer          Start producer at EPS=10 (normal mode)"
	@echo "  make run-producer-burst    Start producer in burst mode (5x)"
	@echo "  make run-producer-slow     Start producer in slow mode (0.5x)"
	@echo ""
	@echo "Producer (Local - requires local Python env):"
	@echo "  make schemas-local         Register schemas from local machine"
	@echo "  make topics-local          Create topics from local machine"
	@echo "  make debezium-local        Register Debezium from local machine"
	@echo "  make producer-local        Run producer from local machine"
	@echo ""
	@echo "Pipelines:"
	@echo "  make run-spark-streaming  Submit Spark Structured Streaming job"
	@echo "  make run-dbt              Run dbt transformations"
	@echo "  make test-dbt             Run dbt tests"
	@echo "  make quality-check        Run Great Expectations checkpoints"
	@echo ""
	@echo "Observability:"
	@echo "  make grafana-setup        Import Grafana dashboards"

up:
	@echo "Starting StreamFlow Analytics Platform..."
	@cp -n .env.example .env 2>/dev/null || true
	$(DOCKER_COMPOSE) up -d --build
	@echo "All services started. Use 'make status' to check health."

down:
	@echo "Stopping StreamFlow Analytics Platform..."
	$(DOCKER_COMPOSE) down --remove-orphans

restart: down up

logs:
	$(DOCKER_COMPOSE) logs -f $(service)

status:
	@echo "Service Health Status:"
	@echo "======================"
	$(DOCKER_COMPOSE) ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

kafka-topics:
	@echo "Creating Kafka topics via kafka container..."
	$(DOCKER_COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:9092 \
		--create --if-not-exists --topic orders \
		--partitions 6 --replication-factor 1 \
		--config retention.ms=604800000 --config compression.type=snappy
	$(DOCKER_COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:9092 \
		--create --if-not-exists --topic order-status \
		--partitions 6 --replication-factor 1 \
		--config retention.ms=604800000 --config compression.type=snappy
	$(DOCKER_COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:9092 \
		--create --if-not-exists --topic pageviews \
		--partitions 12 --replication-factor 1 \
		--config retention.ms=259200000 --config compression.type=snappy
	$(DOCKER_COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:9092 \
		--create --if-not-exists --topic inventory \
		--partitions 3 --replication-factor 1 \
		--config retention.ms=604800000
	$(DOCKER_COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:9092 \
		--create --if-not-exists --topic dlq-orders \
		--partitions 3 --replication-factor 1 \
		--config retention.ms=2592000000
	$(DOCKER_COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:9092 \
		--create --if-not-exists --topic dlq-pageviews \
		--partitions 3 --replication-factor 1 \
		--config retention.ms=2592000000
	$(DOCKER_COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:9092 \
		--create --if-not-exists --topic debezium_connect_configs \
		--partitions 1 --replication-factor 1 \
		--config cleanup.policy=compact
	$(DOCKER_COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:9092 \
		--create --if-not-exists --topic debezium_connect_offsets \
		--partitions 25 --replication-factor 1 \
		--config cleanup.policy=compact
	$(DOCKER_COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:9092 \
		--create --if-not-exists --topic debezium_connect_statuses \
		--partitions 5 --replication-factor 1 \
		--config cleanup.policy=compact
	@echo "Kafka topics created successfully."

create-topics:
	@echo "Creating Kafka topics via Python script..."
	cd producer && KAFKA_BOOTSTRAP_SERVERS=localhost:29092 python create_topics.py

init-postgres:
	@echo "Initializing PostgreSQL..."
	$(DOCKER_COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_SOURCE_DB) \
		-f /docker-entrypoint-initdb.d/init.sql
	@echo "PostgreSQL initialized."

register-schemas:
	@echo "Registering Avro schemas via Python script (Docker)..."
	$(DOCKER_COMPOSE) run --rm --no-deps \
		-e KAFKA_BOOTSTRAP_SERVERS=$(KAFKA_BOOTSTRAP_SERVERS) \
		-e SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) \
		-e POSTGRES_HOST=$(POSTGRES_HOST) \
		-e POSTGRES_PORT=$(POSTGRES_PORT) \
		-e POSTGRES_USER=$(POSTGRES_USER) \
		-e POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) \
		-e POSTGRES_SOURCE_DB=$(POSTGRES_SOURCE_DB) \
		producer python register_schemas.py

register-debezium:
	@echo "Registering Debezium PostgreSQL connector..."
	curl -s -X POST \
		-H "Content-Type: application/json" \
		-d @infrastructure/debezium/postgres-connector.json \
		http://localhost:8083/connectors | python3 -m json.tool
	@echo ""
	@echo "Checking connector status..."
	@sleep 5
	curl -s http://localhost:8083/connectors/postgres-source-connector/status | python3 -m json.tool

run-producer:
	@echo "Starting event producer (Docker, normal mode, EPS=$(EPS))..."
	$(DOCKER_COMPOSE) run --rm \
		-e KAFKA_BOOTSTRAP_SERVERS=$(KAFKA_BOOTSTRAP_SERVERS) \
		-e SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) \
		-e POSTGRES_HOST=$(POSTGRES_HOST) \
		-e POSTGRES_PORT=$(POSTGRES_PORT) \
		-e POSTGRES_USER=$(POSTGRES_USER) \
		-e POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) \
		-e POSTGRES_SOURCE_DB=$(POSTGRES_SOURCE_DB) \
		producer python main.py --eps $(EPS) --mode normal \
		$(if $(DURATION),--duration $(DURATION),)

run-producer-burst:
	@echo "Starting event producer (burst mode)..."
	$(DOCKER_COMPOSE) run --rm \
		-e KAFKA_BOOTSTRAP_SERVERS=$(KAFKA_BOOTSTRAP_SERVERS) \
		-e SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) \
		-e POSTGRES_HOST=$(POSTGRES_HOST) \
		-e POSTGRES_PORT=$(POSTGRES_PORT) \
		-e POSTGRES_USER=$(POSTGRES_USER) \
		-e POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) \
		-e POSTGRES_SOURCE_DB=$(POSTGRES_SOURCE_DB) \
		producer python main.py --eps $(EPS) --mode burst

run-producer-slow:
	@echo "Starting event producer (slow/test mode)..."
	$(DOCKER_COMPOSE) run --rm \
		-e KAFKA_BOOTSTRAP_SERVERS=$(KAFKA_BOOTSTRAP_SERVERS) \
		-e SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) \
		-e POSTGRES_HOST=$(POSTGRES_HOST) \
		-e POSTGRES_PORT=$(POSTGRES_PORT) \
		-e POSTGRES_USER=$(POSTGRES_USER) \
		-e POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) \
		-e POSTGRES_SOURCE_DB=$(POSTGRES_SOURCE_DB) \
		producer python main.py --eps 2 --mode slow $(if $(DURATION),--duration $(DURATION),)

schemas-local:
	@echo "Registering schemas from local environment..."
	cd producer && \
		KAFKA_BOOTSTRAP_SERVERS=localhost:29092 \
		SCHEMA_REGISTRY_URL=http://localhost:8081 \
		POSTGRES_HOST=localhost POSTGRES_PORT=5432 \
		POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) \
		POSTGRES_SOURCE_DB=$(POSTGRES_SOURCE_DB) \
		python register_schemas.py

topics-local:
	@echo "Creating topics from local environment..."
	cd producer && \
		KAFKA_BOOTSTRAP_SERVERS=localhost:29092 \
		SCHEMA_REGISTRY_URL=http://localhost:8081 \
		POSTGRES_HOST=localhost POSTGRES_PORT=5432 \
		POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) \
		POSTGRES_SOURCE_DB=$(POSTGRES_SOURCE_DB) \
		python create_topics.py

debezium-local:
	@echo "Registering Debezium connector from local environment..."
	cd producer && \
		DEBEZIUM_URL=http://localhost:8083 \
		KAFKA_BOOTSTRAP_SERVERS=localhost:29092 \
		SCHEMA_REGISTRY_URL=http://localhost:8081 \
		POSTGRES_HOST=localhost POSTGRES_PORT=5432 \
		POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) \
		POSTGRES_SOURCE_DB=$(POSTGRES_SOURCE_DB) \
		python register_debezium.py

producer-local:
	@echo "Running producer from local environment (EPS=$(EPS))..."
	cd producer && \
		KAFKA_BOOTSTRAP_SERVERS=localhost:29092 \
		SCHEMA_REGISTRY_URL=http://localhost:8081 \
		POSTGRES_HOST=localhost POSTGRES_PORT=5432 \
		POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) \
		POSTGRES_SOURCE_DB=$(POSTGRES_SOURCE_DB) \
		python main.py --eps $(EPS) --mode $(if $(MODE),$(MODE),normal) \
		$(if $(DURATION),--duration $(DURATION),)

run-spark-streaming:
	@echo "Submitting Spark Structured Streaming job..."
	$(DOCKER_COMPOSE) exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--packages io.delta:delta-spark_2.12:3.0.0 \
		--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
		--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
		/opt/spark_jobs/streaming/orders_stream.py

run-dbt:
	@echo "Running dbt transformations..."
	$(DOCKER_COMPOSE) run --rm -w /dbt dbt-runner dbt run --profiles-dir /dbt --project-dir /dbt

test-dbt:
	@echo "Running dbt tests..."
	$(DOCKER_COMPOSE) run --rm -w /dbt dbt-runner dbt test --profiles-dir /dbt --project-dir /dbt

quality-check:
	@echo "Running Great Expectations checkpoints..."
	$(DOCKER_COMPOSE) run --rm -w /data_quality data-quality \
		python -m great_expectations checkpoint run orders_checkpoint

grafana-setup:
	@echo "Importing Grafana dashboards..."
	@for dashboard in serving/grafana/dashboards/*.json; do \
		echo "Importing: $$dashboard"; \
		curl -s -X POST \
			-H "Content-Type: application/json" \
			-u admin:admin123 \
			-d @$$dashboard \
			http://localhost:3000/api/dashboards/db; \
		echo ""; \
	done
	@echo "Dashboards imported."

SHELL := /bin/bash
.DEFAULT_GOAL := help

include .env
export

DOCKER_COMPOSE := docker compose
SERVICE ?= ""

.PHONY: help up down restart logs status \
        kafka-topics init-postgres register-schemas register-debezium \
        run-producer run-spark-streaming run-dbt test-dbt \
        quality-check grafana-setup

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
	@echo "  make kafka-topics        Create all Kafka topics"
	@echo "  make init-postgres       Run PostgreSQL initialization"
	@echo "  make register-schemas    Register Avro schemas to Schema Registry"
	@echo "  make register-debezium   Register Debezium CDC connector"
	@echo ""
	@echo "Pipelines:"
	@echo "  make run-producer         Start event producer"
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
	@echo "Creating Kafka topics..."
	$(DOCKER_COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:9092 \
		--create --if-not-exists --topic streamflow.orders.events \
		--partitions 6 --replication-factor 1 \
		--config retention.ms=604800000
	$(DOCKER_COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:9092 \
		--create --if-not-exists --topic streamflow.users.events \
		--partitions 3 --replication-factor 1 \
		--config retention.ms=604800000
	$(DOCKER_COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:9092 \
		--create --if-not-exists --topic streamflow.products.events \
		--partitions 3 --replication-factor 1 \
		--config retention.ms=604800000
	$(DOCKER_COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:9092 \
		--create --if-not-exists --topic streamflow.clickstream.events \
		--partitions 6 --replication-factor 1 \
		--config retention.ms=86400000
	$(DOCKER_COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:9092 \
		--create --if-not-exists --topic streamflow.cdc.orders \
		--partitions 3 --replication-factor 1
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

init-postgres:
	@echo "Initializing PostgreSQL..."
	$(DOCKER_COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_SOURCE_DB) \
		-f /docker-entrypoint-initdb.d/init.sql
	@echo "PostgreSQL initialized."

register-schemas:
	@echo "Registering Avro schemas..."
	@for schema_file in producer/schemas/*.avsc; do \
		subject=$$(basename $$schema_file .avsc); \
		echo "Registering schema: $$subject"; \
		schema=$$(cat $$schema_file | tr -d '\n' | sed 's/"/\\"/g'); \
		curl -s -X POST \
			-H "Content-Type: application/vnd.schemaregistry.v1+json" \
			-d "{\"schema\": \"$$schema\"}" \
			http://localhost:8081/subjects/$${subject}-value/versions; \
		echo ""; \
	done
	@echo "Schemas registered."

register-debezium:
	@echo "Registering Debezium PostgreSQL connector..."
	curl -s -X POST \
		-H "Content-Type: application/json" \
		-d @infrastructure/debezium/postgres-connector.json \
		http://localhost:8083/connectors
	@echo ""
	@echo "Debezium connector registered."

run-producer:
	@echo "Starting event producer..."
	$(DOCKER_COMPOSE) run --rm producer python main.py

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

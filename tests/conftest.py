import time
import pytest
import requests
from requests.auth import HTTPBasicAuth


@pytest.fixture(scope="session")
def kafka_admin_client():
    from confluent_kafka.admin import AdminClient
    client = AdminClient({"bootstrap.servers": "localhost:29092"})
    metadata = client.list_topics(timeout=10)
    assert metadata is not None, "Cannot connect to Kafka broker at localhost:29092"
    yield client


@pytest.fixture(scope="session")
def kafka_producer_client():
    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": "localhost:29092", "acks": "all"})
    yield producer
    producer.flush(10)


@pytest.fixture(scope="session")
def kafka_consumer_factory():
    from confluent_kafka import Consumer
    consumers = []

    def _make(group_id: str, topics: list[str]):
        c = Consumer({
            "bootstrap.servers": "localhost:29092",
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        c.subscribe(topics)
        consumers.append(c)
        return c

    yield _make

    for c in consumers:
        c.close()


@pytest.fixture(scope="session")
def schema_registry_client():
    from confluent_kafka.schema_registry import SchemaRegistryClient
    client = SchemaRegistryClient({"url": "http://localhost:8081"})
    yield client


@pytest.fixture(scope="session")
def minio_client():
    import boto3
    client = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="streamflow",
        aws_secret_access_key="streamflow123",
        region_name="us-east-1",
    )
    buckets = [b["Name"] for b in client.list_buckets()["Buckets"]]
    for required in ["bronze", "silver", "gold", "checkpoints"]:
        assert required in buckets, f"MinIO bucket '{required}' not found"
    yield client


@pytest.fixture(scope="session")
def clickhouse_client():
    from clickhouse_driver import Client
    client = Client(
        host="localhost",
        port=9000,
        user="streamflow",
        password="streamflow123",
        connect_timeout=10,
    )
    result = client.execute("SELECT 1")
    assert result == [(1,)], "ClickHouse connection failed"
    yield client


@pytest.fixture(scope="session")
def postgres_conn():
    from sqlalchemy import create_engine, text
    engine = create_engine(
        "postgresql+psycopg2://streamflow:streamflow123@localhost:5433/streamflow_source",
        pool_pre_ping=True,
    )
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).scalar()
        assert result == 1, "PostgreSQL connection failed"
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def airflow_api_client():
    session = requests.Session()
    session.auth = HTTPBasicAuth("admin", "admin")
    response = session.get("http://localhost:8088/api/v1/health", timeout=10)
    assert response.status_code == 200, f"Airflow API unreachable: {response.status_code}"
    yield session


@pytest.fixture(scope="session")
def grafana_api_client():
    session = requests.Session()
    session.auth = HTTPBasicAuth("admin", "admin123")
    response = session.get("http://localhost:3000/api/health", timeout=10)
    assert response.status_code == 200, f"Grafana unreachable: {response.status_code}"
    yield session


@pytest.fixture(scope="session")
def fastapi_client():
    session = requests.Session()
    response = session.get("http://localhost:8000/health", timeout=10)
    assert response.status_code == 200, f"FastAPI unreachable: {response.status_code}"
    yield session


@pytest.fixture(scope="function")
def clean_kafka_consumer(kafka_consumer_factory):
    import uuid
    consumer = kafka_consumer_factory(
        group_id=f"test-consumer-{uuid.uuid4().hex[:8]}",
        topics=["orders", "order-status", "pageviews", "inventory"],
    )
    time.sleep(1)
    consumer.poll(timeout=0.1)
    yield consumer
    consumer.close()


@pytest.fixture(scope="function")
def temp_delta_path(tmp_path):
    return str(tmp_path / "delta_test")


@pytest.fixture(scope="function")
def seeded_postgres_data(postgres_conn):
    from sqlalchemy import text
    from tests.fixtures.seed_data import (
        DETERMINISTIC_USERS,
        DETERMINISTIC_PRODUCTS,
        DETERMINISTIC_ORDERS,
    )

    inserted_order_ids = [o["order_id"] for o in DETERMINISTIC_ORDERS]
    inserted_user_ids = [u["user_id"] for u in DETERMINISTIC_USERS]
    inserted_product_ids = [p["product_id"] for p in DETERMINISTIC_PRODUCTS]

    with postgres_conn.connect() as conn:
        for u in DETERMINISTIC_USERS:
            conn.execute(text("""
                INSERT INTO users (user_id, email, name, country, segment, created_at)
                VALUES (:user_id, :email, :name, :country, :segment, :created_at)
                ON CONFLICT (email) DO NOTHING
            """), u)

        for p in DETERMINISTIC_PRODUCTS:
            conn.execute(text("""
                INSERT INTO products (product_id, name, category, price, stock_quantity)
                VALUES (:product_id, :name, :category, :price, :stock_quantity)
                ON CONFLICT DO NOTHING
            """), p)

        for o in DETERMINISTIC_ORDERS:
            conn.execute(text("""
                INSERT INTO orders (order_id, user_id, status, total_amount, created_at)
                VALUES (:order_id, :user_id, :status, :total_amount, :created_at)
                ON CONFLICT DO NOTHING
            """), {
                "order_id": o["order_id"],
                "user_id": o["user_id"],
                "status": "pending",
                "total_amount": o["total_amount"],
                "created_at": o["event_timestamp"],
            })

        conn.commit()

    yield {
        "order_ids": inserted_order_ids,
        "user_ids": inserted_user_ids,
        "product_ids": inserted_product_ids,
        "expected_total_revenue": sum(o["total_amount"] for o in DETERMINISTIC_ORDERS),
    }

    with postgres_conn.connect() as conn:
        ids_placeholder = ", ".join(f"'{oid}'" for oid in inserted_order_ids)
        conn.execute(text(f"DELETE FROM orders WHERE order_id IN ({ids_placeholder})"))
        conn.commit()

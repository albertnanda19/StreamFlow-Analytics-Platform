import os
import time
import uuid
import json
import random
import logging
from datetime import datetime, timezone

from faker import Faker
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

fake = Faker()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

PRODUCTS = [
    {"id": str(uuid.uuid4()), "name": "Headphones", "category": "Electronics", "price": 299.99},
    {"id": str(uuid.uuid4()), "name": "Smart TV", "category": "Electronics", "price": 799.99},
    {"id": str(uuid.uuid4()), "name": "Office Chair", "category": "Furniture", "price": 449.99},
    {"id": str(uuid.uuid4()), "name": "Running Shoes", "category": "Sports", "price": 149.99},
    {"id": str(uuid.uuid4()), "name": "Coffee Maker", "category": "Kitchen", "price": 89.99},
]

COUNTRIES = ["US", "UK", "DE", "CA", "FR", "JP", "AU", "SG", "BR", "IN"]
SEGMENTS  = ["bronze", "silver", "gold", "platinum"]
STATUSES  = ["pending", "confirmed", "shipped", "delivered", "cancelled"]


def delivery_report(err, msg):
    if err:
        logger.error("Delivery failed for %s: %s", msg.key(), err)
    else:
        logger.debug("Delivered %s to %s [%d]", msg.key(), msg.topic(), msg.partition())


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 10,
        "batch.size": 32768,
        "compression.type": "snappy",
    })


def generate_order_event() -> dict:
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    return {
        "event_id":         str(uuid.uuid4()),
        "order_id":         str(uuid.uuid4()),
        "user_id":          str(uuid.uuid4()),
        "product_id":       product["id"],
        "event_type":       "order_created",
        "status":           random.choice(STATUSES),
        "quantity":         quantity,
        "unit_price":       product["price"],
        "total_amount":     round(product["price"] * quantity, 2),
        "country":          random.choice(COUNTRIES),
        "user_segment":     random.choice(SEGMENTS),
        "category":         product["category"],
        "product_name":     product["name"],
        "event_timestamp":  datetime.now(timezone.utc).isoformat(),
    }


def run():
    producer = build_producer()
    topic = "streamflow.orders.events"
    interval_seconds = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "0.5"))

    logger.info("Starting event producer → topic: %s", topic)

    try:
        while True:
            event = generate_order_event()
            producer.produce(
                topic=topic,
                key=event["order_id"],
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        producer.flush(30)
        logger.info("Producer closed.")


if __name__ == "__main__":
    run()

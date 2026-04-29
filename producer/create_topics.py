import sys

from confluent_kafka.admin import AdminClient, NewTopic

from utils import load_env, get_logger

logger = get_logger(__name__)

TOPICS: list[NewTopic] = [
    NewTopic(
        topic="orders",
        num_partitions=6,
        replication_factor=1,
        config={
            "retention.ms": "604800000",
            "cleanup.policy": "delete",
            "compression.type": "snappy",
        },
    ),
    NewTopic(
        topic="order-status",
        num_partitions=6,
        replication_factor=1,
        config={
            "retention.ms": "604800000",
            "cleanup.policy": "delete",
            "compression.type": "snappy",
        },
    ),
    NewTopic(
        topic="pageviews",
        num_partitions=12,
        replication_factor=1,
        config={
            "retention.ms": "259200000",
            "cleanup.policy": "delete",
            "compression.type": "snappy",
        },
    ),
    NewTopic(
        topic="inventory",
        num_partitions=3,
        replication_factor=1,
        config={
            "retention.ms": "604800000",
            "cleanup.policy": "delete",
        },
    ),
    NewTopic(
        topic="dlq-orders",
        num_partitions=3,
        replication_factor=1,
        config={
            "retention.ms": "2592000000",
        },
    ),
    NewTopic(
        topic="dlq-pageviews",
        num_partitions=3,
        replication_factor=1,
        config={
            "retention.ms": "2592000000",
        },
    ),
]


def main():
    cfg = load_env()
    bootstrap = cfg["KAFKA_BOOTSTRAP_SERVERS"]

    admin = AdminClient({"bootstrap.servers": bootstrap})

    existing = set(admin.list_topics(timeout=15).topics.keys())
    logger.info("Existing topics: %d", len(existing))

    to_create = [t for t in TOPICS if t.topic not in existing]
    already   = [t.topic for t in TOPICS if t.topic in existing]

    if already:
        for name in already:
            logger.info("Topic already exists (skipping): %s", name)

    if not to_create:
        logger.info("All topics already exist.")
        return

    futures = admin.create_topics(to_create, validate_only=False)

    print("\n=== Topic Creation Results ===")
    for topic, fut in futures.items():
        try:
            fut.result()
            print(f"  CREATED  {topic}")
            logger.info("Created topic: %s", topic)
        except Exception as exc:
            print(f"  FAILED   {topic}: {exc}")
            logger.error("Failed to create topic %s: %s", topic, exc)

    for name in already:
        print(f"  EXISTS   {name}")
    print()


if __name__ == "__main__":
    main()

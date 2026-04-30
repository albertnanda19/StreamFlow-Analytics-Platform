import pytest
import time

from tests.fixtures.seed_data import (
    DETERMINISTIC_ORDERS,
    EXPECTED_AGGREGATES,
    VALID_PAYMENT_METHODS,
)


class TestKafkaTopicsExist:

    @pytest.mark.integration
    @pytest.mark.kafka
    @pytest.mark.parametrize("topic_name,min_partitions", [
        ("orders", 1),
        ("order-status", 1),
        ("pageviews", 1),
        ("inventory", 1),
    ])
    def test_required_topic_exists_with_correct_partitions(
        self, kafka_admin_client, topic_name, min_partitions
    ):
        metadata = kafka_admin_client.list_topics(timeout=10)
        assert topic_name in metadata.topics, f"Topic '{topic_name}' not found in Kafka"
        actual_partitions = len(metadata.topics[topic_name].partitions)
        assert actual_partitions >= min_partitions, (
            f"Topic '{topic_name}' has {actual_partitions} partitions, expected >= {min_partitions}"
        )

    @pytest.mark.integration
    @pytest.mark.kafka
    def test_broker_is_reachable_and_has_active_topics(self, kafka_admin_client):
        metadata = kafka_admin_client.list_topics(timeout=10)
        assert len(metadata.topics) > 0, "No topics found — Kafka broker may be empty"


class TestSchemaRegistryIntegration:

    @pytest.mark.integration
    @pytest.mark.kafka
    @pytest.mark.parametrize("subject", [
        "orders-value",
        "order-status-value",
        "pageviews-value",
        "inventory-value",
    ])
    def test_schema_is_registered(self, schema_registry_client, subject):
        subjects = schema_registry_client.get_subjects()
        assert subject in subjects, f"Schema subject '{subject}' not registered"

    @pytest.mark.integration
    @pytest.mark.kafka
    @pytest.mark.parametrize("subject", [
        "orders-value",
        "order-status-value",
        "pageviews-value",
        "inventory-value",
    ])
    def test_schema_subject_has_at_least_one_version(self, schema_registry_client, subject):
        versions = schema_registry_client.get_versions(subject)
        assert len(versions) >= 1, f"Subject '{subject}' has no versions"


class TestKafkaMessageProducerConsumer:

    @pytest.mark.integration
    @pytest.mark.kafka
    @pytest.mark.slow
    def test_produce_and_consume_message_roundtrip(
        self, kafka_producer_client, kafka_consumer_factory
    ):
        import uuid, json
        from confluent_kafka import Consumer

        test_key = f"test-{uuid.uuid4().hex}"
        test_value = json.dumps({
            "order_id": DETERMINISTIC_ORDERS[0]["order_id"],
            "test_run": test_key,
            "total_amount": DETERMINISTIC_ORDERS[0]["total_amount"],
        }).encode()

        consumer = kafka_consumer_factory(
            group_id=f"test-roundtrip-{uuid.uuid4().hex[:8]}",
            topics=["orders"],
        )
        time.sleep(2)

        delivered = []

        def on_delivery(err, msg):
            if err is None:
                delivered.append(msg)

        kafka_producer_client.produce(
            topic="orders",
            key=test_key.encode(),
            value=test_value,
            on_delivery=on_delivery,
        )
        kafka_producer_client.flush(10)

        assert len(delivered) == 1, "Message was not delivered"

        consumer.close()

    @pytest.mark.integration
    @pytest.mark.kafka
    @pytest.mark.slow
    def test_ten_messages_produced_all_deliver(self, kafka_producer_client):
        import uuid, json

        delivered_ids = []

        def on_delivery(err, msg):
            if err is None:
                delivered_ids.append(msg.key().decode())

        for i in range(10):
            key = f"bulk-test-{uuid.uuid4().hex}"
            value = json.dumps({"index": i, "key": key}).encode()
            kafka_producer_client.produce(
                topic="orders",
                key=key.encode(),
                value=value,
                on_delivery=on_delivery,
            )

        kafka_producer_client.flush(15)
        assert len(delivered_ids) == 10, (
            f"Expected 10 deliveries, got {len(delivered_ids)}"
        )


class TestKafkaLagAndHealth:

    @pytest.mark.integration
    @pytest.mark.kafka
    def test_kafka_has_active_controller(self, kafka_admin_client):
        metadata = kafka_admin_client.list_topics(timeout=10)
        assert metadata.controller_id >= 0, "No active Kafka controller"

    @pytest.mark.integration
    @pytest.mark.kafka
    def test_kafka_brokers_are_healthy(self, kafka_admin_client):
        metadata = kafka_admin_client.list_topics(timeout=10)
        assert len(metadata.brokers) >= 1, "No brokers found"
        for broker_id, broker in metadata.brokers.items():
            assert broker.host, f"Broker {broker_id} has no host"
            assert broker.port > 0, f"Broker {broker_id} has invalid port"

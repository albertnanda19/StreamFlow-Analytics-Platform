import sys
import time

import requests

from utils import get_logger, load_env

logger = get_logger(__name__)

DEBEZIUM_URL = "http://debezium:8083"
CONNECTOR_NAME = "postgres-source-connector"

CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class":               "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname":             "postgres",
        "database.port":                 "5432",
        "database.user":                 "streamflow",
        "database.password":             "streamflow123",
        "database.dbname":               "streamflow_source",
        "database.server.name":          "streamflow",
        "table.include.list":            "public.orders,public.order_items,public.products,public.users",
        "plugin.name":                   "pgoutput",
        "slot.name":                     "debezium_slot",
        "publication.name":              "debezium_publication",
        "publication.autocreate.mode":   "filtered",
        "topic.prefix":                  "cdc",
        "snapshot.mode":                 "initial",
        "heartbeat.interval.ms":         "30000",
        "tombstones.on.delete":          "false",
        "decimal.handling.mode":         "double",
        "time.precision.mode":           "connect",
        "transforms":                    "unwrap",
        "transforms.unwrap.type":        "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        "transforms.unwrap.delete.handling.mode": "rewrite",
        "transforms.unwrap.add.fields":  "op,table,source.ts_ms",
        "key.converter":                 "org.apache.kafka.connect.json.JsonConverter",
        "value.converter":               "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable":  "false",
        "value.converter.schemas.enable": "false",
    },
}


def wait_for_debezium(timeout: int = 120) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = requests.get(f"{DEBEZIUM_URL}/connectors", timeout=5)
            if r.status_code == 200:
                logger.info("Debezium Connect is ready")
                return True
        except Exception:
            pass
        logger.info("Waiting for Debezium Connect...")
        time.sleep(5)
    return False


def connector_exists() -> bool:
    r = requests.get(f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}", timeout=10)
    return r.status_code == 200


def register_connector() -> dict:
    if connector_exists():
        logger.info("Connector already registered: %s", CONNECTOR_NAME)
        return requests.get(f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}/status", timeout=10).json()

    r = requests.post(
        f"{DEBEZIUM_URL}/connectors",
        json=CONNECTOR_CONFIG,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    if r.status_code not in (200, 201):
        logger.error("Failed to register connector: %s %s", r.status_code, r.text)
        raise RuntimeError(f"Connector registration failed: {r.text}")

    logger.info("Connector registered: %s", CONNECTOR_NAME)
    return r.json()


def wait_for_running(timeout: int = 60) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = requests.get(
                f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}/status", timeout=10
            )
            if r.status_code == 200:
                state = r.json().get("connector", {}).get("state", "UNKNOWN")
                if state == "RUNNING":
                    return state
                logger.info("Connector state: %s — waiting...", state)
        except Exception as exc:
            logger.warning("Status check failed: %s", exc)
        time.sleep(5)
    return "UNKNOWN"


def main():
    debezium_base = __import__("os").getenv("DEBEZIUM_URL", DEBEZIUM_URL)

    if not wait_for_debezium():
        logger.error("Debezium Connect did not become ready in time")
        sys.exit(1)

    register_connector()

    state = wait_for_running()
    if state == "RUNNING":
        logger.info("✅ Connector %s is RUNNING", CONNECTOR_NAME)
        print(f"\n✅  {CONNECTOR_NAME} → RUNNING")
        print(f"    Topics: cdc.public.orders, cdc.public.order_items, cdc.public.products, cdc.public.users\n")
    else:
        logger.error("❌ Connector %s ended in state: %s", CONNECTOR_NAME, state)
        r = requests.get(f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}/status", timeout=10)
        print(r.json())
        sys.exit(1)


if __name__ == "__main__":
    main()

import json
import logging
import sys
from pathlib import Path

import requests

from utils import load_env, get_logger

logger = get_logger(__name__)

SCHEMA_DIR = Path(__file__).parent / "schemas"

SUBJECT_SCHEMA_MAP = {
    "orders-value":        "order_placed.avsc",
    "order-status-value":  "order_status_updated.avsc",
    "pageviews-value":     "pageview.avsc",
    "inventory-value":     "inventory_update.avsc",
}


def load_schema(filename: str) -> str:
    path = SCHEMA_DIR / filename
    raw = json.loads(path.read_text())
    return json.dumps(raw)


def set_compatibility(base_url: str, subject: str, level: str = "BACKWARD") -> None:
    url = f"{base_url}/config/{subject}"
    resp = requests.put(url, json={"compatibility": level}, timeout=10)
    if resp.status_code not in (200, 201):
        logger.warning("Could not set compatibility for %s: %s", subject, resp.text)


def register_schema(base_url: str, subject: str, schema_str: str) -> int:
    url = f"{base_url}/subjects/{subject}/versions"
    payload = {"schema": schema_str, "schemaType": "AVRO"}
    resp = requests.post(url, json=payload, timeout=10)

    if resp.status_code in (200, 201):
        schema_id = resp.json().get("id")
        logger.info("Registered subject=%s  id=%s", subject, schema_id)
        return schema_id

    if resp.status_code == 409:
        lookup = requests.get(f"{base_url}/subjects/{subject}/versions/latest", timeout=10)
        schema_id = lookup.json().get("id")
        logger.info("Already exists subject=%s  id=%s", subject, schema_id)
        return schema_id

    logger.error("Failed to register %s: %s %s", subject, resp.status_code, resp.text)
    raise RuntimeError(f"Schema registration failed for {subject}")


def main():
    cfg = load_env()
    base_url = cfg["SCHEMA_REGISTRY_URL"]

    logger.info("Connecting to Schema Registry: %s", base_url)

    try:
        requests.get(f"{base_url}/subjects", timeout=10).raise_for_status()
    except Exception as exc:
        logger.error("Schema Registry unreachable: %s", exc)
        sys.exit(1)

    results = {}
    for subject, filename in SUBJECT_SCHEMA_MAP.items():
        schema_str = load_schema(filename)
        set_compatibility(base_url, subject)
        schema_id = register_schema(base_url, subject, schema_str)
        results[subject] = schema_id

    print("\n=== Schema Registration Summary ===")
    for subject, sid in results.items():
        print(f"  {subject:<30} id={sid}")
    print()


if __name__ == "__main__":
    main()

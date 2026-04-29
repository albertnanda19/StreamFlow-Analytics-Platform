import argparse
import json
import os
import random
import signal
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from threading import Event

import psycopg2
import psycopg2.extras

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from utils import get_logger, load_env, generate_uuid, now_ms, weighted_choice

logger = get_logger(__name__)

SCHEMA_DIR = Path(__file__).parent / "schemas"

INDONESIAN_CITIES = [
    ("Jakarta Selatan",   "DKI Jakarta",      "ID", "12190"),
    ("Jakarta Pusat",     "DKI Jakarta",      "ID", "10110"),
    ("Bandung",           "Jawa Barat",       "ID", "40111"),
    ("Surabaya",          "Jawa Timur",       "ID", "60111"),
    ("Yogyakarta",        "DI Yogyakarta",    "ID", "55111"),
    ("Medan",             "Sumatera Utara",   "ID", "20111"),
    ("Makassar",          "Sulawesi Selatan", "ID", "90111"),
    ("Semarang",          "Jawa Tengah",      "ID", "50111"),
    ("Palembang",         "Sumatera Selatan", "ID", "30111"),
    ("Denpasar",          "Bali",             "ID", "80111"),
    ("Bekasi",            "Jawa Barat",       "ID", "17111"),
    ("Tangerang",         "Banten",           "ID", "15111"),
    ("Bogor",             "Jawa Barat",       "ID", "16111"),
    ("Depok",             "Jawa Barat",       "ID", "16411"),
]

SEARCH_QUERIES = [
    "sepatu nike", "laptop gaming", "baju batik", "tas kulit",
    "parfum pria", "headphone bluetooth", "jaket kulit", "kamera mirrorless",
    "smartwatch murah", "buku python", "kursi gaming", "kulkas 2 pintu",
    "hp samsung terbaru", "sepeda lipat", "skincare korea",
]

ANDROID_UA = (
    "Mozilla/5.0 (Linux; Android 13; SM-G991B) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Mobile Safari/537.36"
)
IOS_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/17.0 Mobile/15E148 Safari/604.1"
)
DESKTOP_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def _load_avro_schema(filename: str) -> str:
    return (SCHEMA_DIR / filename).read_text()


class EcommerceEventProducer:
    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        postgres_conn_string: str,
        events_per_second: float = 10.0,
        order_rate: float = 0.15,
        pageview_rate: float = 0.70,
        status_update_rate: float = 0.10,
        inventory_rate: float = 0.05,
    ):
        self.bootstrap_servers    = bootstrap_servers
        self.schema_registry_url  = schema_registry_url
        self.postgres_conn_string = postgres_conn_string
        self.events_per_second    = events_per_second
        self.order_rate           = order_rate
        self.pageview_rate        = pageview_rate
        self.status_update_rate   = status_update_rate
        self.inventory_rate       = inventory_rate

        self.users: list[dict]    = []
        self.products: list[dict] = []
        self.recent_orders: deque = deque(maxlen=1000)

        self._stop_event = Event()
        self._stats: dict[str, int] = {
            "orders": 0, "order-status": 0,
            "pageviews": 0, "inventory": 0,
            "dlq": 0, "errors": 0,
        }

        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 5,
            "batch.size": 32768,
            "compression.type": "snappy",
            "retries": 0,
        })

        sr_client = SchemaRegistryClient({"url": schema_registry_url})

        self._serializers: dict[str, AvroSerializer] = {
            "orders":       AvroSerializer(sr_client, _load_avro_schema("order_placed.avsc")),
            "order-status": AvroSerializer(sr_client, _load_avro_schema("order_status_updated.avsc")),
            "pageviews":    AvroSerializer(sr_client, _load_avro_schema("pageview.avsc")),
            "inventory":    AvroSerializer(sr_client, _load_avro_schema("inventory_update.avsc")),
        }

    def load_reference_data(self) -> None:
        conn = psycopg2.connect(self.postgres_conn_string)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT user_id, email, name, country, segment FROM users LIMIT 2000")
                self.users = [dict(r) for r in cur.fetchall()]

                cur.execute("SELECT product_id, name, category, price FROM products LIMIT 2000")
                self.products = [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

        logger.info("Loaded reference data: users=%d products=%d", len(self.users), len(self.products))

        if not self.users or not self.products:
            raise RuntimeError("Reference data is empty — run postgres init.sql first")

    def generate_order_event(self, user: dict, products: list[dict]) -> dict:
        n_items = weighted_choice([1, 2, 3, 4, 5], [70, 20, 6, 3, 1])
        selected = random.sample(products, min(n_items, len(products)))

        items = []
        for p in selected:
            qty = weighted_choice([1, 2, 3], [70, 20, 10])
            discount = 0.0 if random.random() < 0.40 else round(random.uniform(0.01, 0.30), 2)
            items.append({
                "product_id":   str(p["product_id"]),
                "product_name": p["name"],
                "category":     p["category"],
                "quantity":     qty,
                "unit_price":   float(p["price"]),
                "discount_pct": discount,
            })

        subtotal     = sum(i["unit_price"] * i["quantity"] * (1 - i["discount_pct"]) for i in items)
        shipping_fee = random.uniform(0, 25_000)
        city_data    = random.choice(INDONESIAN_CITIES)

        payment_method = weighted_choice(
            ["EWALLET", "CREDIT_CARD", "BANK_TRANSFER", "COD", "DEBIT_CARD"],
            [40,         25,            20,               10,    5],
        )
        device_type = weighted_choice(["MOBILE", "DESKTOP", "TABLET"], [70, 25, 5])
        platform    = weighted_choice(
            ["IOS", "ANDROID", "WEB"],
            [25, 45, 30] if device_type == "MOBILE" else [0, 0, 100],
        )

        order_id = generate_uuid()
        self.recent_orders.append({
            "order_id": order_id,
            "user_id":  str(user["user_id"]),
            "status":   "PENDING",
        })

        return {
            "event_id":        generate_uuid(),
            "event_type":      "order_placed",
            "event_timestamp": now_ms(),
            "order_id":        order_id,
            "user_id":         str(user["user_id"]),
            "session_id":      generate_uuid(),
            "items":           items,
            "subtotal":        round(subtotal, 2),
            "shipping_fee":    round(shipping_fee, 2),
            "total_amount":    round(subtotal + shipping_fee, 2),
            "payment_method":  payment_method,
            "shipping_address": {
                "city":        city_data[0],
                "province":    city_data[1],
                "country":     city_data[2],
                "postal_code": city_data[3],
            },
            "device_type":  device_type,
            "platform":     platform,
            "coupon_code":  None,
            "metadata":     {"source": "web-simulator"},
        }

    def generate_pageview_event(self, user_or_none, products: list[dict]) -> dict:
        is_anonymous = random.random() < 0.30
        user_id = None if is_anonymous else str(user_or_none["user_id"])

        page_type = weighted_choice(
            ["HOME", "CATEGORY", "PRODUCT_DETAIL", "SEARCH", "CART", "CHECKOUT", "ORDER_CONFIRMATION", "PROFILE"],
            [15,      25,         35,                10,       8,      5,           2,                    0],
        )

        product_id   = None
        search_query = None
        if page_type == "PRODUCT_DETAIL" and products:
            product_id = str(random.choice(products)["product_id"])
        elif page_type == "SEARCH":
            search_query = random.choice(SEARCH_QUERIES)

        duration_map = {
            "HOME": (10, 60), "CATEGORY": (20, 120), "PRODUCT_DETAIL": (30, 300),
            "CART": (20, 180), "CHECKOUT": (60, 600), "ORDER_CONFIRMATION": (5, 30),
            "SEARCH": (15, 90), "PROFILE": (10, 60),
        }
        lo, hi = duration_map.get(page_type, (10, 60))

        device_type = weighted_choice(["MOBILE", "DESKTOP", "TABLET"], [70, 25, 5])
        ua_map = {"MOBILE": random.choice([ANDROID_UA, IOS_UA]), "DESKTOP": DESKTOP_UA, "TABLET": IOS_UA}

        return {
            "event_id":         generate_uuid(),
            "event_type":       "pageview",
            "event_timestamp":  now_ms(),
            "session_id":       generate_uuid(),
            "user_id":          user_id,
            "page_url":         f"https://streamflow.io/{page_type.lower().replace('_', '/')}",
            "page_type":        page_type,
            "referrer_url":     None,
            "product_id":       product_id,
            "search_query":     search_query,
            "device_type":      device_type,
            "user_agent":       ua_map[device_type],
            "ip_address":       f"103.{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}",
            "duration_seconds": random.randint(lo, hi),
            "metadata":         {},
        }

    def generate_status_update_event(self, pending_orders: deque) -> dict | None:
        if not pending_orders:
            return None

        order = random.choice(list(pending_orders))
        flow  = {
            "PENDING":    ("CONFIRMED", 0.80),
            "CONFIRMED":  ("PROCESSING", 0.90),
            "PROCESSING": ("SHIPPED", 0.95),
            "SHIPPED":    ("DELIVERED", 0.95),
        }

        prev_status = order.get("status", "PENDING")
        cancel_chance = 0.05 if prev_status in ("PENDING", "CONFIRMED") else 0.0

        if random.random() < cancel_chance:
            new_status = "CANCELLED"
        elif prev_status in flow:
            next_status, prob = flow[prev_status]
            new_status = next_status if random.random() < prob else prev_status
        else:
            return None

        order["status"] = new_status

        return {
            "event_id":         generate_uuid(),
            "event_type":       "order_status_updated",
            "event_timestamp":  now_ms(),
            "order_id":         order["order_id"],
            "user_id":          order["user_id"],
            "previous_status":  prev_status,
            "new_status":       new_status,
            "updated_by":       "system",
            "reason":           None,
            "metadata":         {},
        }

    def generate_inventory_event(self, products: list[dict]) -> dict:
        product = random.choice(products)

        change_reason = weighted_choice(
            ["SALE", "RESTOCK", "ADJUSTMENT", "RETURN", "DAMAGE"],
            [60,      25,        10,           4,         1],
        )

        prev_qty = random.randint(0, 500)
        if change_reason == "SALE":
            delta = -random.randint(1, 5)
        elif change_reason == "RESTOCK":
            delta = random.randint(50, 500)
        elif change_reason == "ADJUSTMENT":
            delta = random.randint(-10, 10)
        elif change_reason == "RETURN":
            delta = random.randint(1, 10)
        else:
            delta = -random.randint(1, 20)

        new_qty = max(0, prev_qty + delta)

        return {
            "event_id":          generate_uuid(),
            "event_type":        "inventory_update",
            "event_timestamp":   now_ms(),
            "product_id":        str(product["product_id"]),
            "warehouse_id":      f"WH-{random.choice(['JKT', 'BDG', 'SBY', 'MKS'])}-{random.randint(1,3):02d}",
            "previous_quantity": prev_qty,
            "new_quantity":      new_qty,
            "change_reason":     change_reason,
            "reference_id":      generate_uuid() if change_reason in ("SALE", "RESTOCK") else None,
            "metadata":          {},
        }

    def _delivery_report(self, err, msg):
        if err:
            logger.error("Delivery error topic=%s key=%s: %s", msg.topic(), msg.key(), err)
            self._stats["errors"] += 1

    def produce_event(self, topic: str, event: dict, key: str) -> bool:
        serializer = self._serializers.get(topic)

        for attempt in range(3):
            try:
                if serializer:
                    value = serializer(
                        event,
                        SerializationContext(topic, MessageField.VALUE),
                    )
                else:
                    value = json.dumps(event).encode()

                self._producer.produce(
                    topic=topic,
                    key=key.encode(),
                    value=value,
                    on_delivery=self._delivery_report,
                )
                self._producer.poll(0)
                self._stats[topic] = self._stats.get(topic, 0) + 1
                return True

            except Exception as exc:
                wait = 1.0 * (2 ** attempt)
                logger.warning("Produce attempt %d failed for topic=%s: %s — retry in %.1fs", attempt + 1, topic, exc, wait)
                time.sleep(wait)

        dlq_topic = f"dlq-{topic}" if f"dlq-{topic}" in ("dlq-orders", "dlq-pageviews") else "dlq-orders"
        try:
            dlq_payload = json.dumps({"original_topic": topic, "event": event, "error": "max_retries_exceeded"}).encode()
            self._producer.produce(topic=dlq_topic, key=key.encode(), value=dlq_payload)
            self._producer.poll(0)
            self._stats["dlq"] += 1
        except Exception:
            pass

        return False

    def _print_stats(self, elapsed: float) -> None:
        total = sum(v for k, v in self._stats.items() if k not in ("errors", "dlq"))
        tps   = total / elapsed if elapsed > 0 else 0
        error_rate = self._stats["errors"] / max(total, 1) * 100
        print(
            f"\n📊 Stats [{elapsed:.0f}s] | "
            f"orders={self._stats.get('orders', 0)} | "
            f"pageviews={self._stats.get('pageviews', 0)} | "
            f"status={self._stats.get('order-status', 0)} | "
            f"inventory={self._stats.get('inventory', 0)} | "
            f"dlq={self._stats.get('dlq', 0)} | "
            f"throughput={tps:.1f} evt/s | "
            f"error_rate={error_rate:.1f}%"
        )

    def run(self, duration_seconds: float | None = None, rate_multiplier: float = 1.0) -> None:
        self.load_reference_data()

        signal.signal(signal.SIGINT,  lambda s, f: self._stop_event.set())
        signal.signal(signal.SIGTERM, lambda s, f: self._stop_event.set())

        eps          = self.events_per_second * rate_multiplier
        interval     = 1.0 / eps
        start        = time.monotonic()
        last_stat_ts = start

        logger.info("Producer running at %.1f events/s (multiplier=%.1fx)", eps, rate_multiplier)

        while not self._stop_event.is_set():
            elapsed = time.monotonic() - start
            if duration_seconds and elapsed >= duration_seconds:
                break

            user = random.choice(self.users)
            roll = random.random()

            if roll < self.order_rate:
                event = self.generate_order_event(user, self.products)
                self.produce_event("orders", event, event["order_id"])

            elif roll < self.order_rate + self.pageview_rate:
                event = self.generate_pageview_event(user, self.products)
                self.produce_event("pageviews", event, event["event_id"])

            elif roll < self.order_rate + self.pageview_rate + self.status_update_rate:
                event = self.generate_status_update_event(self.recent_orders)
                if event:
                    self.produce_event("order-status", event, event["order_id"])

            else:
                event = self.generate_inventory_event(self.products)
                self.produce_event("inventory", event, event["product_id"])

            now = time.monotonic()
            if now - last_stat_ts >= 30:
                self._print_stats(now - start)
                last_stat_ts = now

            time.sleep(interval)

        logger.info("Shutting down — flushing producer...")
        self._producer.flush(30)
        self._print_stats(time.monotonic() - start)
        logger.info("Producer stopped.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="StreamFlow e-commerce event producer")
    parser.add_argument("--eps",      type=float, default=10.0,  help="Events per second")
    parser.add_argument("--duration", type=float, default=None,  help="Run for N seconds then exit")
    parser.add_argument(
        "--mode", choices=["normal", "burst", "slow"], default="normal",
        help="normal=1x, burst=5x for 60s, slow=0.5x",
    )
    return parser.parse_args()


def main():
    args = parse_args() if False else _parse_args()
    cfg  = load_env()

    pg_dsn = (
        f"host={cfg['POSTGRES_HOST']} port={cfg['POSTGRES_PORT']} "
        f"dbname={cfg['POSTGRES_SOURCE_DB']} "
        f"user={cfg['POSTGRES_USER']} password={cfg['POSTGRES_PASSWORD']}"
    )

    producer = EcommerceEventProducer(
        bootstrap_servers=cfg["KAFKA_BOOTSTRAP_SERVERS"],
        schema_registry_url=cfg["SCHEMA_REGISTRY_URL"],
        postgres_conn_string=pg_dsn,
        events_per_second=args.eps,
    )

    if args.mode == "burst":
        logger.info("BURST mode: 5x rate for 60s then normal")
        producer.run(duration_seconds=60, rate_multiplier=5.0)
        producer.run(duration_seconds=args.duration, rate_multiplier=1.0)
    elif args.mode == "slow":
        producer.run(duration_seconds=args.duration, rate_multiplier=0.5)
    else:
        producer.run(duration_seconds=args.duration, rate_multiplier=1.0)


if __name__ == "__main__":
    main()

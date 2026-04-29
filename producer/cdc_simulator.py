import random
import signal
import threading
import time

import psycopg2
import psycopg2.extras

from utils import get_logger, load_env

logger = get_logger(__name__)

STATUS_FLOW = {
    "pending":   "confirmed",
    "confirmed": "shipped",
    "shipped":   "delivered",
}

CANCEL_STATUSES = {"pending", "confirmed"}


class CDCSimulator(threading.Thread):
    def __init__(self, conn_string: str, interval_seconds: float = 30.0):
        super().__init__(daemon=True, name="cdc-simulator")
        self.conn_string       = conn_string
        self.interval_seconds  = interval_seconds
        self._stop_event       = threading.Event()
        self._cycle            = 0

    def stop(self) -> None:
        self._stop_event.set()

    def _get_connection(self):
        return psycopg2.connect(self.conn_string)

    def _advance_order_statuses(self, conn) -> int:
        updated = 0
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT order_id, status FROM orders
                WHERE status NOT IN ('delivered', 'cancelled', 'refunded')
                ORDER BY RANDOM()
                LIMIT 20
                """
            )
            orders = cur.fetchall()

        with conn.cursor() as cur:
            for order in orders:
                oid    = order["order_id"]
                status = order["status"]

                if status in CANCEL_STATUSES and random.random() < 0.03:
                    new_status = "cancelled"
                elif status in STATUS_FLOW:
                    new_status = STATUS_FLOW[status]
                else:
                    continue

                cur.execute(
                    "UPDATE orders SET status = %s, updated_at = NOW() WHERE order_id = %s",
                    (new_status, oid),
                )
                updated += 1

        conn.commit()
        return updated

    def _update_product_stock(self, conn) -> int:
        updated = 0
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE products
                SET stock_quantity = GREATEST(0, stock_quantity - %s)
                WHERE product_id IN (
                    SELECT product_id FROM products
                    WHERE stock_quantity > 10
                    ORDER BY RANDOM()
                    LIMIT 5
                )
                """,
                (random.randint(1, 10),),
            )
            updated = cur.rowcount

            cur.execute(
                """
                UPDATE products
                SET stock_quantity = stock_quantity + %s
                WHERE product_id IN (
                    SELECT product_id FROM products
                    WHERE stock_quantity < 50
                    ORDER BY RANDOM()
                    LIMIT 3
                )
                """,
                (random.randint(50, 200),),
            )
            updated += cur.rowcount

        conn.commit()
        return updated

    def _insert_sample_orders(self, conn) -> int:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users ORDER BY RANDOM() LIMIT 5")
            user_ids = [r[0] for r in cur.fetchall()]

        if not user_ids:
            return 0

        statuses = ["pending"] * 5
        inserted = 0

        with conn.cursor() as cur:
            for uid, status in zip(user_ids, statuses):
                amount = round(random.uniform(50_000, 2_000_000), 2)
                cur.execute(
                    """
                    INSERT INTO orders (user_id, status, total_amount)
                    VALUES (%s, %s, %s)
                    """,
                    (uid, status, amount),
                )
                inserted += 1

        conn.commit()
        return inserted

    def run(self) -> None:
        logger.info("CDC simulator started — interval=%.0fs", self.interval_seconds)

        while not self._stop_event.is_set():
            try:
                conn = self._get_connection()
                try:
                    if self._cycle % 3 == 0:
                        n = self._insert_sample_orders(conn)
                        logger.info("CDC: inserted %d sample orders", n)

                    n = self._advance_order_statuses(conn)
                    logger.info("CDC: advanced %d order statuses", n)

                    n = self._update_product_stock(conn)
                    logger.info("CDC: updated %d product stock rows", n)

                finally:
                    conn.close()

            except Exception as exc:
                logger.error("CDC simulator error: %s", exc)

            self._cycle += 1
            self._stop_event.wait(self.interval_seconds)

        logger.info("CDC simulator stopped.")


def main():
    cfg = load_env()
    pg_dsn = (
        f"host={cfg['POSTGRES_HOST']} port={cfg['POSTGRES_PORT']} "
        f"dbname={cfg['POSTGRES_SOURCE_DB']} "
        f"user={cfg['POSTGRES_USER']} password={cfg['POSTGRES_PASSWORD']}"
    )

    sim = CDCSimulator(conn_string=pg_dsn, interval_seconds=30.0)

    def _shutdown(signum, frame):
        logger.info("Received signal %d — stopping CDC simulator", signum)
        sim.stop()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    sim.start()
    sim.join()


if __name__ == "__main__":
    main()

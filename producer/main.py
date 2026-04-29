import signal
import threading

from cdc_simulator import CDCSimulator
from event_producer import EcommerceEventProducer, _parse_args
from utils import get_logger, load_env

logger = get_logger(__name__)


def main():
    args = _parse_args()
    cfg  = load_env()

    pg_dsn = (
        f"host={cfg['POSTGRES_HOST']} port={cfg['POSTGRES_PORT']} "
        f"dbname={cfg['POSTGRES_SOURCE_DB']} "
        f"user={cfg['POSTGRES_USER']} password={cfg['POSTGRES_PASSWORD']}"
    )

    cdc = CDCSimulator(conn_string=pg_dsn, interval_seconds=30.0)
    cdc.start()
    logger.info("CDC simulator started as background thread")

    producer = EcommerceEventProducer(
        bootstrap_servers=cfg["KAFKA_BOOTSTRAP_SERVERS"],
        schema_registry_url=cfg["SCHEMA_REGISTRY_URL"],
        postgres_conn_string=pg_dsn,
        events_per_second=args.eps,
    )

    def _shutdown(signum, frame):
        logger.info("Shutting down (signal %d)...", signum)
        cdc.stop()
        producer._stop_event.set()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    if args.mode == "burst":
        logger.info("BURST mode: 5x rate for 60s")
        producer.run(duration_seconds=60, rate_multiplier=5.0)
        if not producer._stop_event.is_set():
            producer.run(duration_seconds=args.duration, rate_multiplier=1.0)
    elif args.mode == "slow":
        producer.run(duration_seconds=args.duration, rate_multiplier=0.5)
    else:
        producer.run(duration_seconds=args.duration, rate_multiplier=1.0)

    cdc.stop()
    cdc.join(timeout=10)
    logger.info("All done.")


if __name__ == "__main__":
    main()

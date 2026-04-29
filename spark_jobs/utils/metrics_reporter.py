import logging
import os
import threading
import time
from dataclasses import dataclass, field

from pyspark.sql.streaming import StreamingQuery

logger = logging.getLogger(__name__)


@dataclass
class _BatchStats:
    batch_id: int = 0
    input_rows: int = 0
    processed_rows: int = 0
    batch_duration_ms: int = 0
    watermark: str = ""
    zero_input_count: int = 0


class StreamingMetricsReporter:
    _ALERT_DURATION_MS  = 60_000
    _ALERT_ZERO_BATCHES = 10

    def __init__(
        self,
        query: StreamingQuery,
        app_name: str,
        report_interval_seconds: int = 60,
        webhook_url: str | None = None,
        pushgateway_url: str | None = None,
    ):
        self._query            = query
        self._app_name         = app_name
        self._interval         = report_interval_seconds
        self._webhook_url      = webhook_url or os.getenv("ALERT_WEBHOOK_URL")
        self._pushgateway_url  = pushgateway_url or os.getenv("PROMETHEUS_PUSHGATEWAY_URL")
        self._stats            = _BatchStats()
        self._stop             = threading.Event()
        self._thread           = threading.Thread(
            target=self._run, daemon=True, name=f"metrics-{app_name}"
        )

    def start(self) -> None:
        self._thread.start()
        logger.info("MetricsReporter started for query: %s", self._app_name)

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=10)

    def _collect(self) -> None:
        progress = self._query.lastProgress
        if not progress:
            return

        self._stats.batch_id       = progress.get("batchId", 0)
        self._stats.input_rows     = int(progress.get("numInputRows", 0))
        self._stats.batch_duration_ms = int(progress.get("batchDuration", 0))

        rates = progress.get("processedRowsPerSecond", 0)
        self._stats.processed_rows = int(rates) if rates else 0

        wm = progress.get("eventTime", {}).get("watermark", "")
        self._stats.watermark = wm

        if self._stats.input_rows == 0:
            self._stats.zero_input_count += 1
        else:
            self._stats.zero_input_count = 0

    def _log_metrics(self) -> None:
        s = self._stats
        logger.info(
            "[%s] batch=%d input_rows=%d processed_rps=%d "
            "duration_ms=%d watermark=%s zero_batches=%d",
            self._app_name, s.batch_id, s.input_rows, s.processed_rows,
            s.batch_duration_ms, s.watermark, s.zero_input_count,
        )

    def _push_prometheus(self) -> None:
        if not self._pushgateway_url:
            return
        try:
            import requests
            s       = self._stats
            metrics = (
                f"# HELP spark_streaming_input_rows Number of input rows per batch\n"
                f"# TYPE spark_streaming_input_rows gauge\n"
                f'spark_streaming_input_rows{{app="{self._app_name}"}} {s.input_rows}\n'
                f'spark_streaming_batch_duration_ms{{app="{self._app_name}"}} {s.batch_duration_ms}\n'
                f'spark_streaming_zero_input_batches{{app="{self._app_name}"}} {s.zero_input_count}\n'
            )
            requests.post(
                f"{self._pushgateway_url}/metrics/job/{self._app_name}",
                data=metrics, timeout=5,
            )
        except Exception as exc:
            logger.debug("Pushgateway push failed: %s", exc)

    def _send_alert(self, message: str) -> None:
        logger.warning("ALERT [%s]: %s", self._app_name, message)
        if not self._webhook_url:
            return
        try:
            import requests
            requests.post(
                self._webhook_url,
                json={"text": f"🚨 StreamFlow Alert [{self._app_name}]: {message}"},
                timeout=5,
            )
        except Exception as exc:
            logger.debug("Webhook alert failed: %s", exc)

    def _check_alerts(self) -> None:
        s = self._stats
        if s.batch_duration_ms > self._ALERT_DURATION_MS:
            self._send_alert(
                f"Batch duration {s.batch_duration_ms}ms exceeds threshold {self._ALERT_DURATION_MS}ms"
            )
        if s.zero_input_count >= self._ALERT_ZERO_BATCHES:
            self._send_alert(
                f"No input rows for {s.zero_input_count} consecutive batches"
            )

    def _run(self) -> None:
        while not self._stop.is_set():
            self._stop.wait(self._interval)
            if self._stop.is_set():
                break
            try:
                self._collect()
                self._log_metrics()
                self._push_prometheus()
                self._check_alerts()
            except Exception as exc:
                logger.error("MetricsReporter error: %s", exc)

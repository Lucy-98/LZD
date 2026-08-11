"""Ghi log inference RA KHOI duong request.

Van de cua ban dau:
    /decide -> audit.log_inference() -> psycopg2.connect() -> INSERT -> close()
    Moi request mo mot ket noi Postgres MOI: TCP handshake + auth + insert
    + close ~ 5-20ms. Trong khi ngan sach doc Redis chi la 10ms va SLA tong
    la 100ms. Tu ghi log lai thanh thanh phan cham nhat cua he thong.

Cach lam o day (pattern chuan cho logging trong hot path):

    request  ──► queue.put_nowait()  (~2 micro giay, khong block)
                      │
                      ▼
              worker thread  ──► gom BATCH_SIZE dong ──► execute_values()
                                 tren MOT ket noi giu lau

  - Queue co gioi han: day thi VUT dong moi nhat va dem vao counter, tuyet doi
    khong bao gio lam cham hay lam hong request.
  - Sampling: mac dinh ghi 100% cho de quan sat khi hoc; chinh
    INFERENCE_LOG_SAMPLE_RATE=0.01 khi ban tai cao.
  - Ket noi chet -> tu ket noi lai, khong lam anh huong serving.
"""
from __future__ import annotations

import atexit
import os
import queue
import random
import threading
import time
from typing import Any

from prometheus_client import Counter, Gauge

from lzd_pipeline.common.logging_setup import get_logger

log = get_logger(__name__)

# --------------------------------------------------------------- metrics
INFERENCE_LOG_QUEUED = Counter(
    "lzd_inference_log_queued_total", "So ban ghi inference da xep hang"
)
INFERENCE_LOG_WRITTEN = Counter(
    "lzd_inference_log_written_total", "So ban ghi inference da ghi xuong Postgres"
)
INFERENCE_LOG_DROPPED = Counter(
    "lzd_inference_log_dropped_total", "So ban ghi bi vut", ["reason"]
)
INFERENCE_LOG_QUEUE_DEPTH = Gauge(
    "lzd_inference_log_queue_depth", "So ban ghi dang cho trong hang doi"
)

_INSERT_SQL = """
INSERT INTO ops.inference_log
    (user_id, feature_version, model_version, uplift_score,
     decision, latency_ms, cache_hit, features_missing)
VALUES %s
"""


class InferenceLogger:
    def __init__(
        self,
        max_queue: int = 10_000,
        batch_size: int = 200,
        flush_interval: float = 2.0,
        sample_rate: float | None = None,
    ) -> None:
        self.queue: queue.Queue[tuple] = queue.Queue(maxsize=max_queue)
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.sample_rate = (
            sample_rate
            if sample_rate is not None
            else float(os.environ.get("INFERENCE_LOG_SAMPLE_RATE", "1.0"))
        )
        self._conn = None
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name="inference-log-writer", daemon=True
        )
        self._thread.start()
        atexit.register(self.shutdown)
        log.info(
            "inference logger khoi dong",
            extra={"event": "inference_logger_start", "batch_size": batch_size,
                   "sample_rate": self.sample_rate, "max_queue": max_queue},
        )

    # ------------------------------------------------------- hot path
    def log(self, **row: Any) -> None:
        """Goi tu trong request. Khong bao gio raise, khong bao gio block."""
        if self.sample_rate < 1.0 and random.random() > self.sample_rate:
            INFERENCE_LOG_DROPPED.labels(reason="sampled_out").inc()
            return
        record = (
            row.get("user_id"),
            row.get("feature_version"),
            row.get("model_version"),
            row.get("uplift_score"),
            row.get("decision"),
            row.get("latency_ms"),
            row.get("cache_hit"),
            row.get("features_missing", 0),
        )
        try:
            self.queue.put_nowait(record)
            INFERENCE_LOG_QUEUED.inc()
        except queue.Full:
            # Tha mat log con hon lam cham serving
            INFERENCE_LOG_DROPPED.labels(reason="queue_full").inc()

    # -------------------------------------------------- background thread
    def _connect(self):
        from lzd_pipeline.common.clients import get_pg_connection

        conn = get_pg_connection()
        conn.autocommit = True
        return conn

    def _flush(self, batch: list[tuple]) -> None:
        if not batch:
            return
        from psycopg2.extras import execute_values

        for attempt in (1, 2):
            try:
                if self._conn is None or self._conn.closed:
                    self._conn = self._connect()
                with self._conn.cursor() as cur:
                    execute_values(cur, _INSERT_SQL, batch, page_size=len(batch))
                INFERENCE_LOG_WRITTEN.inc(len(batch))
                return
            except Exception as exc:
                try:
                    if self._conn is not None:
                        self._conn.close()
                except Exception:
                    pass
                self._conn = None
                if attempt == 2:
                    INFERENCE_LOG_DROPPED.labels(reason="db_error").inc(len(batch))
                    log.warning(
                        "khong ghi duoc inference_log",
                        extra={"event": "inference_log_write_failed",
                               "rows": len(batch), "error": str(exc)[:300]},
                    )

    def _run(self) -> None:
        batch: list[tuple] = []
        last_flush = time.monotonic()
        while not self._stop.is_set():
            timeout = max(0.05, self.flush_interval - (time.monotonic() - last_flush))
            try:
                batch.append(self.queue.get(timeout=timeout))
            except queue.Empty:
                pass

            INFERENCE_LOG_QUEUE_DEPTH.set(self.queue.qsize())
            due = time.monotonic() - last_flush >= self.flush_interval
            if len(batch) >= self.batch_size or (batch and due):
                self._flush(batch)
                batch = []
                last_flush = time.monotonic()

        # Flush not phan con lai khi tat
        while True:
            try:
                batch.append(self.queue.get_nowait())
            except queue.Empty:
                break
        self._flush(batch)

    def shutdown(self, timeout: float = 5.0) -> None:
        if self._stop.is_set():
            return
        self._stop.set()
        self._thread.join(timeout=timeout)
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        log.info("inference logger da dung", extra={"event": "inference_logger_stop"})


_logger: InferenceLogger | None = None
_lock = threading.Lock()


def get_inference_logger() -> InferenceLogger:
    global _logger
    if _logger is None:
        with _lock:
            if _logger is None:
                _logger = InferenceLogger()
    return _logger

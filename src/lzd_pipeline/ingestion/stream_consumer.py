"""Kafka -> (1) Data Lake parquet trong MinIO  (2) Redis realtime overlay.

Thu tu xu ly 1 micro-batch (RAT quan trong):

    poll  ->  validate  ->  ghi parquet len MinIO  ->  cap nhat Redis
          ->  COMMIT OFFSET (buoc cuoi cung)

Commit sau cung => neu chet giua chung, batch do se duoc doc lai
=> at-least-once. Trung lap duoc xu ly o tang sau:
    - lake  : dedup theo event_id trong dbt (staging)
    - redis : counter co the bi cong du -> nen overlay chi la tin hieu gan dung,
              con so chinh xac lay tu batch feature (nguon su that).

Chay: docker compose --profile stream up -d stream-consumer
"""
from __future__ import annotations

import io
import json
import os
import signal
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from lzd_pipeline.common.clients import get_kafka_consumer, get_kafka_producer, get_s3_client
from lzd_pipeline.common.config import get_settings
from lzd_pipeline.common.logging_setup import configure_logging, get_logger
from lzd_pipeline.common.metrics import (
    CONSUMER_BATCH_SIZE,
    CONSUMER_FLUSH_SECONDS,
    EVENTS_CONSUMED,
    EVENTS_DLQ,
    EVENT_E2E_LAG,
    LAKE_FILES_WRITTEN,
    LAKE_ROWS_WRITTEN,
    REALTIME_OVERLAY_KEYS,
    start_metrics_server,
)
from lzd_pipeline.features.online_store import OnlineFeatureStore
from lzd_pipeline.ingestion.schemas import EVENT_TO_COUNTER, LAKE_COLUMNS, validate_event

log = get_logger(__name__)

_running = True


def _stop(signum, frame):  # noqa: ARG001
    global _running
    _running = False
    log.info("nhan tin hieu dung", extra={"event": "shutdown_signal", "signal": signum})


class StreamConsumer:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.consumer = get_kafka_consumer()
        self.dlq_producer = get_kafka_producer(**{"enable.idempotence": False})
        self.s3 = get_s3_client()
        self.store = OnlineFeatureStore()
        self.bucket = self.settings.minio.bucket_lake
        self.max_records = int(os.environ.get("CONSUMER_BATCH_MAX_RECORDS", "2000"))
        self.max_seconds = float(os.environ.get("CONSUMER_BATCH_MAX_SECONDS", "30"))
        self.buffer: list[dict[str, Any]] = []
        self.last_flush = time.time()

    # ------------------------------------------------------------------
    def _to_dlq(self, raw_value: bytes, reason: str) -> None:
        EVENTS_DLQ.labels(reason=reason).inc()
        try:
            self.dlq_producer.produce(
                self.settings.kafka.topic_dlq,
                value=raw_value,
                headers=[("reason", reason.encode()), ("ts", str(time.time()).encode())],
            )
            self.dlq_producer.poll(0)
        except Exception as exc:
            log.error("khong ghi duoc DLQ",
                      extra={"event": "dlq_write_failed", "error": str(exc)})

    # ------------------------------------------------------------------
    def _write_parquet(self, rows: list[dict[str, Any]]) -> str | None:
        """Ghi 1 file parquet vao raw layer, partition theo dt/hour."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        if not rows:
            return None
        now = datetime.now(timezone.utc)
        table = pa.Table.from_pylist(
            [{col: row.get(col) for col in LAKE_COLUMNS} for row in rows]
        )
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)

        key = (
            f"raw/app_events/dt={now:%Y-%m-%d}/hour={now:%H}/"
            f"part-{now:%Y%m%d%H%M%S}-{os.getpid()}-{len(rows)}.parquet"
        )
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=buf.getvalue())
        LAKE_FILES_WRITTEN.labels(dataset="app_events").inc()
        LAKE_ROWS_WRITTEN.labels(dataset="app_events").inc(len(rows))
        return key

    # ------------------------------------------------------------------
    def _update_realtime(self, rows: list[dict[str, Any]]) -> int:
        """Cong don feature realtime cho tung user trong batch.

        Gop theo user truoc roi moi ghi => 1 pipeline thay vi N lenh Redis.
        """
        counters: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        last_seen: dict[str, float] = {}

        for row in rows:
            user_id = row["user_id"]
            counters[user_id]["rt_events_1h"] += 1
            field = EVENT_TO_COUNTER.get(row["event_type"])
            if field:
                counters[user_id][field] += 1
            if row["event_type"] == "order":
                counters[user_id]["rt_gmv_1h"] += float(row.get("price") or 0) * int(row.get("quantity") or 0)
            ts = float(row["event_ts"])
            last_seen[user_id] = max(last_seen.get(user_id, 0.0), ts)

        for user_id, fields in counters.items():
            self.store.incr_realtime_counters(user_id, dict(fields))
        # rt_last_event_ts la gia tri tuyet doi -> set chu khong cong don
        self.store.update_realtime_bulk(
            {uid: {"rt_last_event_ts": int(ts)} for uid, ts in last_seen.items()}
        )
        return len(counters)

    # ------------------------------------------------------------------
    def _flush(self) -> None:
        if not self.buffer:
            self.last_flush = time.time()
            return

        started = time.perf_counter()
        rows = self.buffer
        CONSUMER_BATCH_SIZE.observe(len(rows))

        try:
            key = self._write_parquet(rows)             # 1. lake truoc
            users = self._update_realtime(rows)         # 2. online store sau
            self.consumer.commit(asynchronous=False)    # 3. commit cuoi cung

            now = time.time()
            for row in rows:
                EVENT_E2E_LAG.observe(max(now - float(row["event_ts"]), 0))
            REALTIME_OVERLAY_KEYS.set(self.store.count_keys(self.store.spec.realtime_key("*")))

            elapsed = time.perf_counter() - started
            CONSUMER_FLUSH_SECONDS.observe(elapsed)
            log.info(
                "flush micro-batch",
                extra={"event": "batch_flushed", "rows": len(rows), "users": users,
                       "s3_key": key, "duration_ms": int(elapsed * 1000)},
            )
        except Exception as exc:
            # KHONG commit offset -> batch se duoc doc lai o vong sau
            log.error("flush that bai, se doc lai batch",
                      extra={"event": "batch_flush_failed", "rows": len(rows),
                             "error": str(exc)})
            raise
        finally:
            self.buffer = []
            self.last_flush = time.time()

    # ------------------------------------------------------------------
    def run(self) -> None:
        topic = self.settings.kafka.topic_events
        self.consumer.subscribe([topic])
        log.info("consumer khoi dong",
                 extra={"event": "consumer_start", "topic": topic,
                        "group": self.settings.kafka.consumer_group,
                        "max_records": self.max_records, "max_seconds": self.max_seconds})

        while _running:
            msg = self.consumer.poll(1.0)

            if msg is None:
                if time.time() - self.last_flush >= self.max_seconds:
                    self._flush()
                continue

            if msg.error():
                log.warning("loi kafka", extra={"event": "kafka_error", "error": str(msg.error())})
                continue

            raw = msg.value()
            try:
                payload = json.loads(raw.decode("utf-8"))
            except Exception:
                self._to_dlq(raw, "invalid_json")
                continue

            ok, reason = validate_event(payload)
            if not ok:
                self._to_dlq(raw, reason.split(":")[0])
                log.debug("event khong hop le",
                          extra={"event": "event_invalid", "reason": reason})
                continue

            payload["ingested_at"] = time.time()
            payload["kafka_partition"] = msg.partition()
            payload["kafka_offset"] = msg.offset()
            payload.setdefault("schema_version", 1)
            self.buffer.append(payload)
            EVENTS_CONSUMED.labels(event_type=payload["event_type"]).inc()

            if (len(self.buffer) >= self.max_records
                    or time.time() - self.last_flush >= self.max_seconds):
                self._flush()

        log.info("flush lan cuoi truoc khi thoat", extra={"event": "consumer_shutdown"})
        try:
            self._flush()
        finally:
            self.consumer.close()
            self.dlq_producer.flush(5)


def main() -> None:
    configure_logging(service=os.environ.get("SERVICE_NAME", "stream-consumer"))
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    start_metrics_server(int(os.environ.get("METRICS_PORT", "9106")))
    StreamConsumer().run()


if __name__ == "__main__":
    main()

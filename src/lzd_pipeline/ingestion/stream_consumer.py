"""Kafka -> MinIO raw parquet -> Redis realtime overlay.

Thu tu xu ly 1 micro-batch (RAT quan trong):

    poll  ->  validate  ->  ghi parquet len MinIO  ->  cap nhat Redis
          ->  COMMIT OFFSET (buoc cuoi cung)

Commit sau cung => neu chet giua chung, batch do se duoc doc lai
=> at-least-once. Trung lap duoc xu ly o tang sau:
    - lake  : dedup theo event_id trong dbt (staging)
    - redis : Lua event_id marker + counter mutation trong mot transaction,
              nen replay khong cong trung overlay.

Chay: docker compose --profile stream up -d stream-consumer
"""
from __future__ import annotations

import io
import hashlib
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
    LAKE_IDEMPOTENCY_CONFLICTS,
    LAKE_ROWS_WRITTEN,
    REALTIME_EVENTS_APPLIED,
    REALTIME_EVENTS_DEDUPLICATED,
    REALTIME_EVENTS_LATE,
    REALTIME_OVERLAY_KEYS,
    REALTIME_UPDATE_FAILURES,
    start_metrics_server,
)
from lzd_pipeline.features.online_store import OnlineFeatureStore, rt_bucket_start, rt_cutoff
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
        self.dlq_producer = get_kafka_producer()
        self.s3 = get_s3_client()
        self.store = OnlineFeatureStore()
        self.bucket = self.settings.minio.bucket_lake
        self.max_records = int(os.environ.get("CONSUMER_BATCH_MAX_RECORDS", "2000"))
        self.max_seconds = float(os.environ.get("CONSUMER_BATCH_MAX_SECONDS", "30"))
        self.buffer: list[dict[str, Any]] = []
        self.last_flush = time.time()

    # ------------------------------------------------------------------
    def _to_dlq(self, raw_value: bytes, reason: str) -> bool:
        EVENTS_DLQ.labels(reason=reason).inc()
        try:
            self.dlq_producer.produce(
                self.settings.kafka.topic_dlq,
                value=raw_value,
                headers=[("reason", reason.encode()), ("ts", str(time.time()).encode())],
            )
            self.dlq_producer.poll(0)
            if self.dlq_producer.flush(5) != 0:
                raise RuntimeError("DLQ delivery timed out")
            return True
        except Exception as exc:
            log.error("khong ghi duoc DLQ",
                      extra={"event": "dlq_write_failed", "error": str(exc)})
            return False

    def _commit_invalid_if_safe(self, msg, delivered: bool) -> None:
        """Commit a DLQ-only offset only when no valid row is waiting to land."""
        if delivered and not self.buffer:
            self.consumer.commit(message=msg, asynchronous=False)

    # ------------------------------------------------------------------
    def _write_parquet(self, rows: list[dict[str, Any]]) -> list[str]:
        """Land deterministic objects, one per Kafka partition/offset range."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        if not rows:
            return []
        by_partition: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            event_time = datetime.fromtimestamp(float(row["event_ts"]), tz=timezone.utc)
            group = (event_time.strftime("%Y-%m-%d"), event_time.strftime("%H"),
                     int(row["kafka_partition"]))
            by_partition[group].append(row)

        keys: list[str] = []
        topic = self.settings.kafka.topic_events
        for (event_date, event_hour, partition), partition_rows in sorted(by_partition.items()):
            partition_rows.sort(key=lambda row: int(row["kafka_offset"]))
            first = int(partition_rows[0]["kafka_offset"])
            last = int(partition_rows[-1]["kafka_offset"])
            table = pa.Table.from_pylist(
                [{col: row.get(col) for col in LAKE_COLUMNS} for row in partition_rows]
            )
            buf = io.BytesIO()
            pq.write_table(table, buf, compression="snappy")
            payload = buf.getvalue()
            digest = hashlib.sha256(payload).hexdigest()
            key = (
                f"raw/app_events/dt={event_date}/hour={event_hour}/"
                f"topic={topic}/partition={partition}/offset-{first}-{last}.parquet"
            )
            try:
                existing = self.s3.head_object(Bucket=self.bucket, Key=key)
            except Exception as exc:
                response = getattr(exc, "response", {})
                status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                code = str(response.get("Error", {}).get("Code", ""))
                if status != 404 and code not in {"404", "NoSuchKey", "NotFound"}:
                    raise
                existing = None
            if existing is not None:
                previous = (existing.get("Metadata") or {}).get("sha256")
                if previous != digest:
                    LAKE_IDEMPOTENCY_CONFLICTS.inc()
                    raise RuntimeError(
                        f"MinIO idempotency conflict for {key}: {previous!r} != {digest!r}"
                    )
            else:
                self.s3.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=payload,
                    Metadata={"sha256": digest},
                )
                LAKE_FILES_WRITTEN.labels(dataset="app_events").inc()
                LAKE_ROWS_WRITTEN.labels(dataset="app_events").inc(len(partition_rows))
            keys.append(key)
        return keys

    # ------------------------------------------------------------------
    def _update_realtime(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        """Apply events replay-safely using event time and an explicit watermark."""
        now = time.time()
        cfg = self.settings.feature_store
        result = {"applied": 0, "deduplicated": 0, "late_accepted": 0,
                  "late_dropped": 0, "future_dropped": 0}
        for row in rows:
            event_ts = float(row["event_ts"])
            age = now - event_ts
            if event_ts > now + cfg.realtime_future_skew_seconds:
                result["future_dropped"] += 1
                REALTIME_EVENTS_LATE.labels(action="future_dropped").inc()
                continue
            if rt_bucket_start(event_ts) < rt_cutoff(now):
                result["late_dropped"] += 1
                REALTIME_EVENTS_LATE.labels(action="dropped").inc()
                continue
            if age > cfg.realtime_allowed_lateness_seconds:
                result["late_accepted"] += 1
                REALTIME_EVENTS_LATE.labels(action="accepted").inc()

            counters: dict[str, float] = {"rt_events_1h": 1.0}
            field = EVENT_TO_COUNTER.get(row["event_type"])
            if field:
                counters[field] = counters.get(field, 0.0) + 1.0
            if row["event_type"] == "order":
                counters["rt_gmv_1h"] = (
                    float(row.get("price") or 0) * int(row.get("quantity") or 0)
                )
            try:
                applied = self.store.apply_realtime_event(
                    str(row["event_id"]), str(row["user_id"]), event_ts,
                    counters, session_id=str(row.get("session_id") or ""), now=now,
                )
            except Exception:
                REALTIME_UPDATE_FAILURES.inc()
                raise
            if applied:
                result["applied"] += 1
                REALTIME_EVENTS_APPLIED.inc()
            else:
                result["deduplicated"] += 1
                REALTIME_EVENTS_DEDUPLICATED.inc()
        return result

    # ------------------------------------------------------------------
    def _flush(self) -> None:
        if not self.buffer:
            self.last_flush = time.time()
            return

        started = time.perf_counter()
        rows = self.buffer
        CONSUMER_BATCH_SIZE.observe(len(rows))

        try:
            keys = self._write_parquet(rows)            # 1. lake truoc
            realtime = self._update_realtime(rows)      # 2. online store sau
            self.consumer.commit(asynchronous=False)    # 3. commit cuoi cung

            now = time.time()
            for row in rows:
                EVENT_E2E_LAG.observe(max(now - float(row["event_ts"]), 0))
            REALTIME_OVERLAY_KEYS.set(self.store.count_keys(self.store.spec.realtime_key("*")))

            elapsed = time.perf_counter() - started
            CONSUMER_FLUSH_SECONDS.observe(elapsed)
            log.info(
                "flush micro-batch",
                extra={"event": "batch_flushed", "rows": len(rows),
                       "realtime": realtime, "s3_keys": keys,
                       "duration_ms": int(elapsed * 1000)},
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
                delivered = self._to_dlq(raw, "invalid_json")
                self._commit_invalid_if_safe(msg, delivered)
                continue

            ok, reason = validate_event(payload)
            if not ok:
                delivered = self._to_dlq(raw, reason.split(":")[0])
                self._commit_invalid_if_safe(msg, delivered)
                log.debug("event khong hop le",
                          extra={"event": "event_invalid", "reason": reason})
                continue

            # Kafka record timestamp is stable across replay; wall-clock time
            # would change Parquet bytes and break deterministic object checks.
            _timestamp_type, kafka_timestamp_ms = msg.timestamp()
            payload["ingested_at"] = (
                kafka_timestamp_ms / 1000.0
                if kafka_timestamp_ms is not None and kafka_timestamp_ms >= 0
                else float(payload["event_ts"])
            )
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

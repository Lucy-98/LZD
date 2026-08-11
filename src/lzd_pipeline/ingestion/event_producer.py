"""Gia lap traffic Web/Mobile -> Kafka.

Thay cho he thong app that. Sinh event theo phien (session) de du lieu
realtime co hinh dang giong that: 1 user mo app -> xem vai trang -> them gio
-> doi khi dat don.

Chay: python -m lzd_pipeline.ingestion.event_producer
Dung: docker compose --profile stream up -d event-producer
"""
from __future__ import annotations

import json
import os
import random
import signal
import time
import uuid
from typing import Any

from lzd_pipeline.common.clients import get_kafka_producer
from lzd_pipeline.common.config import get_settings
from lzd_pipeline.common.logging_setup import configure_logging, get_logger
from lzd_pipeline.common.metrics import EVENTS_PRODUCED, start_metrics_server
from lzd_pipeline.ingestion.schemas import AppEvent

log = get_logger(__name__)

PLATFORMS = ("web", "android", "ios")
CATEGORIES = [f"cat_{i:03d}" for i in range(1, 41)]

# Xac suat chuyen trang thai trong 1 phien
SESSION_FLOW = [
    ("app_open", 1.00),
    ("page_view", 0.95),
    ("search", 0.45),
    ("page_view", 0.80),
    ("voucher_view", 0.35),
    ("add_to_cart", 0.30),
    ("voucher_claim", 0.15),
    ("checkout", 0.12),
    ("order", 0.08),
]

_running = True


def _stop(signum, frame):  # noqa: ARG001
    global _running
    _running = False
    log.info("nhan tin hieu dung", extra={"event": "shutdown_signal", "signal": signum})


class EventProducer:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.producer = get_kafka_producer()
        self.topic = self.settings.kafka.topic_events
        self.eps = float(os.environ.get("PRODUCER_EVENTS_PER_SECOND", "25"))
        self.user_pool = int(os.environ.get("PRODUCER_USER_POOL", "20000"))
        self.corrupt_rate = float(os.environ.get("PRODUCER_CORRUPT_RATE", "0.01"))
        self.sent = 0

    # user_id trung khong gian voi seed data (U0000000 ...) -> feature batch va
    # feature realtime cua cung 1 user gap nhau duoc tren Redis
    def _pick_user(self) -> str:
        # Phan bo lech (20% user tao 80% traffic) giong thuc te
        if random.random() < 0.8:
            idx = random.randint(0, max(int(self.user_pool * 0.2) - 1, 0))
        else:
            idx = random.randint(0, self.user_pool - 1)
        return f"U{idx:07d}"

    def _make_session(self, user_id: str) -> list[AppEvent]:
        session_id = uuid.uuid4().hex[:16]
        platform = random.choice(PLATFORMS)
        now = time.time()
        events: list[AppEvent] = []
        offset = 0.0
        for event_type, prob in SESSION_FLOW:
            if random.random() > prob:
                continue
            offset += random.uniform(0.5, 12.0)
            price = round(random.lognormvariate(3.2, 0.8), 2) if event_type in (
                "add_to_cart", "checkout", "order") else 0.0
            events.append(
                AppEvent(
                    event_id=uuid.uuid4().hex,
                    user_id=user_id,
                    event_type=event_type,
                    event_ts=now + offset,
                    session_id=session_id,
                    platform=platform,
                    item_id=f"item_{random.randint(1, 500000)}",
                    category_id=random.choice(CATEGORIES),
                    price=price,
                    quantity=random.randint(1, 3) if price else 0,
                )
            )
        return events

    def _corrupt(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Co tinh lam hong event de test DLQ + alert cua Grafana."""
        mode = random.choice(["drop_user", "bad_ts", "unknown_type"])
        if mode == "drop_user":
            payload.pop("user_id", None)
        elif mode == "bad_ts":
            payload["event_ts"] = "hom-qua"
        else:
            payload["event_type"] = "teleport"
        return payload

    def _delivery_report(self, err, msg) -> None:
        if err is not None:
            log.error("gui kafka that bai",
                      extra={"event": "produce_failed", "error": str(err)})

    def run(self) -> None:
        log.info(
            "producer khoi dong",
            extra={"event": "producer_start", "topic": self.topic,
                   "eps": self.eps, "user_pool": self.user_pool,
                   "corrupt_rate": self.corrupt_rate},
        )
        interval = 1.0 / max(self.eps, 0.1)
        while _running:
            batch = self._make_session(self._pick_user())
            for evt in batch:
                payload = evt.to_dict()
                if self.corrupt_rate > 0 and random.random() < self.corrupt_rate:
                    payload = self._corrupt(dict(payload))
                try:
                    self.producer.produce(
                        self.topic,
                        # Key = user_id => moi event cua 1 user vao cung partition
                        # => thu tu duoc bao dam trong pham vi 1 user.
                        key=str(payload.get("user_id", "unknown")),
                        value=json.dumps(payload).encode("utf-8"),
                        on_delivery=self._delivery_report,
                    )
                    EVENTS_PRODUCED.labels(event_type=payload.get("event_type", "unknown")).inc()
                    self.sent += 1
                except BufferError:
                    self.producer.poll(0.5)
                self.producer.poll(0)
                time.sleep(interval)

            if self.sent % 500 < len(batch):
                log.info("da gui event",
                         extra={"event": "produce_progress", "total_sent": self.sent})

        log.info("flush truoc khi thoat", extra={"event": "producer_flush"})
        self.producer.flush(10)


def main() -> None:
    configure_logging(service=os.environ.get("SERVICE_NAME", "event-producer"))
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    start_metrics_server(int(os.environ.get("METRICS_PORT", "9105")))
    EventProducer().run()


if __name__ == "__main__":
    main()

"""HYBRID TRIGGER ENGINE — Promotion Decision Engine.

Hệ thống quyết định phân phối voucher dựa trên kết hợp:
1. Event-Driven Path: Lắng nghe Kafka event (add_to_cart, checkout) -> đánh giá ngay.
2. Polling Path: Định kỳ 5 phút quét các user active gần đây trên Redis -> đánh giá loạt.

Đọc 1 RTT Redis (Batch features fs:* + Realtime rt:*) -> Chạy Uplift Model ->
Trả về quyết định phân phối voucher (voucher_30, voucher_15, no_voucher) và ghi audit log.
"""
from __future__ import annotations

import json
import os
import threading
import time
from typing import Any

from lzd_pipeline.common.clients import get_kafka_consumer, get_kafka_producer, get_redis
from lzd_pipeline.common.config import get_settings
from lzd_pipeline.common.logging_setup import get_logger
from lzd_pipeline.features.online_store import OnlineFeatureStore
from lzd_pipeline.features.spec import load_feature_spec
from lzd_pipeline.serving.decide_input import build_model_row
from lzd_pipeline.serving.model_loader import get_model

log = get_logger(__name__)

# Ngưỡng uplift phân loại voucher
VOUCHER_HIGH_THRESHOLD = float(os.environ.get("VOUCHER_HIGH_THRESHOLD", "0.05"))
VOUCHER_MID_THRESHOLD = float(os.environ.get("VOUCHER_MID_THRESHOLD", "0.02"))


def determine_voucher_code(uplift_score: float | None) -> tuple[str, str]:
    """Chuyển điểm số uplift sang quyết định và mã voucher cụ thể."""
    if uplift_score is None:
        return "NO_DECISION", "no_voucher"
    if uplift_score >= VOUCHER_HIGH_THRESHOLD:
        return "SEND_VOUCHER", "voucher_30"
    if uplift_score >= VOUCHER_MID_THRESHOLD:
        return "SEND_VOUCHER", "voucher_15"
    return "NO_VOUCHER", "no_voucher"


class TriggerEngine:
    def __init__(self, store: OnlineFeatureStore | None = None) -> None:
        self.settings = get_settings()
        self.store = store or OnlineFeatureStore()
        self.spec = load_feature_spec()
        self.model = get_model()
        self._running = False
        self._consumer = None
        self._producer = None

    def evaluate_user(
        self, user_id: str, context: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Đánh giá 1 user trong 1 RTT: đọc Redis -> Uplift Model -> Quyết định."""
        context = context or {}
        started = time.perf_counter()

        # 1. Đọc Redis Batch + Realtime trong 1 RTT atomic Lua script
        version, batch_raw, rt_raw = self.store.read_for_serving(user_id)
        cache_hit = bool(batch_raw)

        # 2. Gộp features
        realtime = self.store.aggregate_realtime(rt_raw)
        merged, missing, supplied, rt_applied = build_model_row(
            self.spec, self.model.feature_order, batch_raw, realtime, context
        )

        # 3. Chạy model dự đoán
        try:
            score = self.model.predict_uplift([merged])[0]
        except Exception as exc:
            log.error("loi predict uplift", extra={"event": "trigger_predict_error", "user_id": user_id, "error": str(exc)})
            score = None

        # 4. Quyết định & phân loại voucher
        decision, voucher_code = determine_voucher_code(score)
        latency_ms = round((time.perf_counter() - started) * 1000, 2)

        result = {
            "user_id": user_id,
            "decision": decision,
            "voucher_code": voucher_code,
            "uplift_score": score,
            "feature_version": version,
            "model_version": self.model.version,
            "cache_hit": cache_hit,
            "latency_ms": latency_ms,
            "features_used": merged if os.environ.get("DEBUG_TRIGGER") else None,
        }

        log.info(
            "trigger_evaluation",
            extra={
                "event": "trigger_eval",
                "user_id": user_id,
                "decision": decision,
                "voucher_code": voucher_code,
                "score": score,
                "latency_ms": latency_ms,
            },
        )
        return result

    def poll_active_users_5m(self) -> list[dict[str, Any]]:
        """Polling path: quét Redis tìm các user có tương tác trong 5 phút qua."""
        r = self.store.r
        now = time.time()
        cutoff_5m = now - 300  # 5 phút

        results = []
        try:
            keys = r.keys("rt:u:*")
            active_uids = []
            for k in keys:
                uid = k.split("rt:u:")[-1] if isinstance(k, str) else k.decode().split("rt:u:")[-1]
                last_ts = r.hget(k, "rt_last_event_ts")
                if last_ts:
                    try:
                        if float(last_ts) >= cutoff_5m:
                            active_uids.append(uid)
                    except (ValueError, TypeError):
                        pass

            for uid in active_uids:
                results.append(self.evaluate_user(uid))
        except Exception as exc:
            log.warning("loi poll active users 5m", extra={"event": "poll_active_error", "error": str(exc)})

        return results

    def start_background_workers(self) -> None:
        """Khởi động worker lắng nghe Kafka và worker Polling 5 phút."""
        self._running = True

        # 1. Polling worker (chạy mỗi 5 phút = 300s)
        def _polling_loop():
            while self._running:
                try:
                    self.poll_active_users_5m()
                except Exception as e:
                    log.error("error in polling worker", extra={"error": str(e)})
                time.sleep(300)

        t_poll = threading.Thread(target=_polling_loop, daemon=True, name="trigger-polling-5m")
        t_poll.start()
        log.info("trigger engine background workers started")

    def stop(self) -> None:
        self._running = False

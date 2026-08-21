"""Hybrid trigger engine for the serving API.

The bundled immutable 30F model is used when its manifest and sentinel pass.
If verification fails, the engine keeps the merge path observable but emits
``NO_DECISION`` instead of falling back to a synthetic score.
"""
from __future__ import annotations

import os
import threading
import time
from typing import Any

from lzd_pipeline.common.config import get_settings
from lzd_pipeline.common.logging_setup import get_logger
from lzd_pipeline.features.online_store import OnlineFeatureStore
from lzd_pipeline.features.spec import load_feature_spec
from lzd_pipeline.serving.decide_input import build_model_row
from lzd_pipeline.serving.model_loader import get_model

log = get_logger(__name__)


def determine_voucher_code(
    uplift_score: float | None,
    threshold: float | None = None,
) -> tuple[str, str]:
    """Map an uplift score to a decision and voucher code."""
    if uplift_score is None:
        return "NO_DECISION", "no_voucher"
    threshold = get_settings().uplift_threshold if threshold is None else threshold
    if uplift_score >= threshold:
        return "SEND_VOUCHER", "voucher_30"
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
        """Evaluate one user by merging Redis state and scoring if available."""
        context = context or {}
        started = time.perf_counter()

        version, batch_raw, rt_raw = self.store.read_for_serving(user_id)
        cache_hit = bool(batch_raw)

        realtime = self.store.aggregate_realtime(rt_raw)
        merged, missing, supplied, rt_applied = build_model_row(
            self.spec, self.model.feature_order, batch_raw, realtime, context
        )

        if not self.model.is_configured:
            score = None
        else:
            try:
                score = self.model.predict_uplift([merged])[0]
            except Exception as exc:
                log.error(
                    "loi predict uplift",
                    extra={
                        "event": "trigger_predict_error",
                        "user_id": user_id,
                        "error": str(exc),
                    },
                )
                score = None

        decision, voucher_code = determine_voucher_code(
            score, self.settings.uplift_threshold
        )
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
        """Poll Redis for users with activity in the last five minutes."""
        r = self.store.r
        now = time.time()
        cutoff_5m = now - 300

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
            log.warning(
                "loi poll active users 5m",
                extra={"event": "poll_active_error", "error": str(exc)},
            )

        return results

    def start_background_workers(self) -> None:
        """Start the polling worker."""
        self._running = True

        def _polling_loop() -> None:
            while self._running:
                try:
                    self.poll_active_users_5m()
                except Exception as exc:
                    log.error("error in polling worker", extra={"error": str(exc)})
                time.sleep(300)

        t_poll = threading.Thread(
            target=_polling_loop,
            daemon=True,
            name="trigger-polling-5m",
        )
        t_poll.start()
        log.info("trigger engine background workers started")

    def stop(self) -> None:
        self._running = False

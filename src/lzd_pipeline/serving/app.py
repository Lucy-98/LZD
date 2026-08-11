"""INFERENCE SERVICE (FastAPI).

Duong di 1 request (SLA < 100ms):

    POST /decide {user_id}
      -> doc fs:meta:active_version                     (Redis, ~0.2ms)
      -> HGETALL fs:{v}:u:{uid} + rt:u:{uid} (1 RTT)    (Redis, ~1-3ms)
      -> merge theo feature_spec.yml, dien default
      -> model.predict_uplift()                          (TODO teammate)
      -> so voi nguong -> SEND_VOUCHER / NO_VOUCHER

Phan feature (doc Redis, merge, do latency, log) DA XONG.
Phan model la khung - xem serving/model_loader.py.

Endpoint:
    GET  /health          - liveness
    GET  /ready           - readiness (Redis + co active_version chua)
    GET  /metrics         - Prometheus scrape
    GET  /features/{uid}  - DEBUG: xem dung feature ma model se nhan
    POST /decide          - quyet dinh phat voucher cho 1 user
    POST /decide/batch    - cho nhieu user
    GET  /store/info      - trang thai feature store (version, so key, tuoi)
"""
from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

from lzd_pipeline.common.config import get_settings
from lzd_pipeline.common.logging_setup import configure_logging, get_logger
from lzd_pipeline.common.metrics import (
    FEATURES_MISSING,
    FEATURE_LOOKUP,
    FEATURE_LOOKUP_LATENCY,
    INFERENCE_DECISIONS,
    INFERENCE_LATENCY,
    INFERENCE_REQUESTS,
)
from lzd_pipeline.features.online_store import OnlineFeatureStore
from lzd_pipeline.features.spec import load_feature_spec
from lzd_pipeline.serving.inference_logger import get_inference_logger
from lzd_pipeline.serving.model_loader import get_model

configure_logging(service=os.environ.get("SERVICE_NAME", "inference-api"))
log = get_logger(__name__)

_store: OnlineFeatureStore | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Lam nong truoc khi nhan traffic, don sach khi tat.

    Neu de request DAU TIEN phai tu khoi tao (ket noi Redis, doc spec, nap
    model) thi no se ton vai tram ms va lam p99 xau mot cach kho hieu.
    """
    for step, fn in (
        ("redis", lambda: store().r.ping()),
        ("feature_spec", load_feature_spec),
        ("model", get_model),
        ("inference_logger", get_inference_logger),
    ):
        try:
            fn()
            log.info("warm-up xong", extra={"event": "warmup_ok", "step": step})
        except Exception as exc:
            # Khong chan container khoi dong: /health van song de debug,
            # /ready se bao do va bao chinh xac thieu gi.
            log.error("warm-up that bai",
                      extra={"event": "warmup_failed", "step": step, "error": str(exc)[:300]})

    yield

    # Flush not log inference dang nam trong hang doi truoc khi tat
    get_inference_logger().shutdown()
    log.info("inference-api da dung", extra={"event": "api_stopped"})


app = FastAPI(
    title="LZD Uplift Inference Service",
    description="Quyet dinh phat voucher realtime dua tren uplift score",
    version="0.1.0",
    lifespan=lifespan,
)


def store() -> OnlineFeatureStore:
    global _store
    if _store is None:
        _store = OnlineFeatureStore()
    return _store


# ===========================================================================
# Schema request/response
# ===========================================================================
class DecideRequest(BaseModel):
    user_id: str = Field(..., examples=["U0000123"])
    # Cho phep client gui them tin hieu realtime chua kip qua Kafka
    context: dict[str, Any] = Field(default_factory=dict)
    debug: bool = False


class DecideResponse(BaseModel):
    user_id: str
    decision: str
    uplift_score: float | None
    threshold: float
    feature_version: str | None
    model_version: str
    cache_hit: bool
    features_missing: int
    latency_ms: float
    features: dict[str, Any] | None = None


class BatchDecideRequest(BaseModel):
    user_ids: list[str]


# ===========================================================================
# Health / readiness / metrics
# ===========================================================================
@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": get_settings().service_name}


@app.get("/ready")
def ready() -> dict[str, Any]:
    try:
        store().r.ping()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"redis khong san sang: {exc}")
    version = store().get_active_version()
    if not version:
        raise HTTPException(
            status_code=503,
            detail="chua co fs:meta:active_version - chay DAG 40_sync_features_to_redis truoc",
        )
    return {"status": "ready", "feature_version": version,
            "model_version": get_model().version}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ===========================================================================
# Debug: xem chinh xac feature model se nhan
# ===========================================================================
@app.get("/features/{user_id}")
def get_features(user_id: str, version: str | None = None) -> dict[str, Any]:
    spec = load_feature_spec()
    started = time.perf_counter()
    if version:
        batch_raw, rt_raw = store().get_features(user_id, version=version)
        active = version
    else:
        active, batch_raw, rt_raw = store().read_for_serving(user_id)
    realtime = store().aggregate_realtime(rt_raw)
    merged, missing = spec.merge(batch_raw, realtime)
    return {
        "user_id": user_id,
        "feature_version": active,
        "lookup_ms": round((time.perf_counter() - started) * 1000, 3),
        "cache_hit": bool(batch_raw),
        "features_missing": missing,
        "n_batch_fields": len(batch_raw),
        "n_realtime_fields": len(rt_raw),
        "raw_batch": batch_raw,
        # raw_realtime = cac o 5 phut con nguyen; realtime_window = sau khi gap
        "raw_realtime": rt_raw,
        "realtime_window": realtime,
        "merged": merged,
    }


@app.get("/store/info")
def store_info() -> dict[str, Any]:
    s = store()
    version = s.get_active_version()
    return {
        "active_version": version,
        "all_versions": s.list_versions(),
        "status": s.get_version_status(version) if version else {},
        "age_seconds": s.feature_store_age_seconds(),
        "realtime_keys": s.count_keys(s.spec.realtime_key("*")),
        "batch_keys": s.count_keys(s.spec.batch_key(version, "*")) if version else 0,
        "model": {"name": get_model().name, "version": get_model().version,
                  "is_stub": get_model().is_stub},
    }


# ===========================================================================
# Duong quyet dinh chinh
# ===========================================================================
@app.post("/decide", response_model=DecideResponse)
def decide(req: DecideRequest) -> DecideResponse:
    settings = get_settings()
    spec = load_feature_spec()
    model = get_model()
    started = time.perf_counter()

    # ---- 1. Doc feature tu Redis --------------------------------------
    lookup_started = time.perf_counter()
    try:
        # MOT round-trip, atomic: doc active_version + hash batch + hash
        # realtime trong cung mot lenh Lua. Xem read_for_serving().
        version, batch_raw, rt_raw = store().read_for_serving(req.user_id)
    except Exception as exc:
        INFERENCE_REQUESTS.labels(status="error").inc()
        log.error("loi doc feature store",
                  extra={"event": "feature_lookup_error", "user_id": req.user_id,
                         "error": str(exc)})
        raise HTTPException(status_code=503, detail=f"feature store loi: {exc}")
    FEATURE_LOOKUP_LATENCY.observe(time.perf_counter() - lookup_started)

    cache_hit = bool(batch_raw)
    FEATURE_LOOKUP.labels(result="hit" if cache_hit else "miss").inc()

    # ---- 2. Merge: batch <- realtime <- context tu client ---------------
    # aggregate_realtime() gap cac o 5 phut thanh gia tri cua so 1 gio -
    # dung cong thuc ma dbt dung luc build training set.
    realtime = store().aggregate_realtime(rt_raw)
    merged, missing = spec.merge(batch_raw, {**realtime, **req.context})
    FEATURES_MISSING.observe(missing)

    # ---- 3. Suy luan ---------------------------------------------------
    try:
        score = model.predict_uplift([merged])[0]
        status = "ok"
    except NotImplementedError:
        # Model that chua san sang -> tra ve rang minh khong quyet dinh duoc
        score = None
        status = "model_not_ready"
    except Exception as exc:
        INFERENCE_REQUESTS.labels(status="error").inc()
        log.error("loi suy luan", extra={"event": "predict_error",
                                         "user_id": req.user_id, "error": str(exc)})
        raise HTTPException(status_code=500, detail=f"model loi: {exc}")

    # ---- 4. Quyet dinh -------------------------------------------------
    threshold = settings.uplift_threshold
    if score is None:
        decision = "NO_DECISION"
    else:
        decision = "SEND_VOUCHER" if score >= threshold else "NO_VOUCHER"

    latency = time.perf_counter() - started
    INFERENCE_LATENCY.observe(latency)
    INFERENCE_REQUESTS.labels(status=status).inc()
    INFERENCE_DECISIONS.labels(decision=decision).inc()

    latency_ms = round(latency * 1000, 3)
    # Xep hang (~vai micro giay), worker thread ghi Postgres theo batch.
    # KHONG duoc goi audit.log_inference() truc tiep o day: no mo ket noi
    # Postgres moi cho tung request -> +5-20ms vao duong serving.
    get_inference_logger().log(
        user_id=req.user_id,
        feature_version=version,
        model_version=model.version,
        uplift_score=score,
        decision=decision,
        latency_ms=latency_ms,
        cache_hit=cache_hit,
        features_missing=missing,
    )
    log.info(
        "quyet dinh",
        extra={"event": "decision", "user_id": req.user_id, "decision": decision,
               "uplift_score": score, "feature_version": version,
               "model_version": model.version, "cache_hit": cache_hit,
               "features_missing": missing, "latency_ms": latency_ms},
    )

    return DecideResponse(
        user_id=req.user_id,
        decision=decision,
        uplift_score=score,
        threshold=threshold,
        feature_version=version,
        model_version=model.version,
        cache_hit=cache_hit,
        features_missing=missing,
        latency_ms=latency_ms,
        features=merged if req.debug else None,
    )


@app.post("/decide/batch")
def decide_batch(req: BatchDecideRequest) -> dict[str, Any]:
    """Scoring hang loat - 1 pipeline Redis cho toan bo user."""
    model = get_model()
    started = time.perf_counter()
    features_map = store().mget_features(req.user_ids)
    rows = [features_map[uid] for uid in req.user_ids]
    try:
        scores = model.predict_uplift(rows)
    except NotImplementedError:
        scores = [None] * len(rows)

    threshold = get_settings().uplift_threshold
    results = []
    for uid, score in zip(req.user_ids, scores):
        decision = ("NO_DECISION" if score is None
                    else "SEND_VOUCHER" if score >= threshold else "NO_VOUCHER")
        INFERENCE_DECISIONS.labels(decision=decision).inc()
        results.append({"user_id": uid, "uplift_score": score, "decision": decision})

    latency = time.perf_counter() - started
    INFERENCE_LATENCY.observe(latency)
    INFERENCE_REQUESTS.labels(status="ok").inc()
    return {
        "count": len(results),
        "latency_ms": round(latency * 1000, 3),
        "per_user_ms": round(latency * 1000 / max(len(results), 1), 4),
        "model_version": model.version,
        "results": results,
    }


@app.post("/admin/reload-model")
def reload_model() -> dict[str, Any]:
    """Nap lai model sau khi co ban moi tren Registry (khong can restart)."""
    model = get_model(force_reload=True)
    return {"model_name": model.name, "model_version": model.version,
            "is_stub": model.is_stub}

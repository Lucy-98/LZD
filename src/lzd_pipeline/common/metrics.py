"""Metric Prometheus dung chung.

Hai kieu day metric:
  1) Service chay lien tuc (producer/consumer/api) -> tu mo HTTP /metrics,
     Prometheus scrape truc tiep.  -> `start_metrics_server()`
  2) Task Airflow chay roi thoat  -> khong kip bi scrape, nen PUSH len
     Pushgateway.                  -> `push_batch_metrics()`

Ten metric deu bat dau bang `lzd_` de tach khoi metric cua ha tang.
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Iterator

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    push_to_gateway,
    start_http_server,
)

from lzd_pipeline.common.config import get_settings
from lzd_pipeline.common.logging_setup import get_logger

log = get_logger(__name__)

# ===========================================================================
# 1. Metric cua service long-running (registry mac dinh)
# ===========================================================================

# --- ingestion -------------------------------------------------------------
EVENTS_PRODUCED = Counter(
    "lzd_events_produced_total", "So event da ban vao Kafka", ["event_type"]
)
EVENTS_CONSUMED = Counter(
    "lzd_events_consumed_total", "So event da consume tu Kafka", ["event_type"]
)
EVENTS_DLQ = Counter(
    "lzd_events_dlq_total", "So event bi day vao dead letter queue", ["reason"]
)
LAKE_FILES_WRITTEN = Counter(
    "lzd_lake_files_written_total", "So file parquet da ghi xuong MinIO", ["dataset"]
)
LAKE_ROWS_WRITTEN = Counter(
    "lzd_lake_rows_written_total", "So dong da ghi xuong data lake", ["dataset"]
)
CONSUMER_BATCH_SIZE = Histogram(
    "lzd_consumer_batch_size", "So record moi micro-batch",
    buckets=(1, 10, 50, 100, 500, 1000, 2000, 5000),
)
CONSUMER_FLUSH_SECONDS = Histogram(
    "lzd_consumer_flush_seconds", "Thoi gian flush 1 micro-batch (s)",
    buckets=(0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30),
)
EVENT_E2E_LAG = Histogram(
    "lzd_event_end_to_end_lag_seconds",
    "Do tre tu luc event sinh ra den luc feature len Redis (s)",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 300),
)
REALTIME_OVERLAY_KEYS = Gauge(
    "lzd_realtime_overlay_keys", "Uoc luong so key rt:u:* dang co tren Redis"
)

# --- serving ---------------------------------------------------------------
INFERENCE_REQUESTS = Counter(
    "lzd_inference_requests_total", "So request inference", ["status"]
)
INFERENCE_DECISIONS = Counter(
    "lzd_inference_decisions_total", "Quyet dinh phat voucher", ["decision"]
)
INFERENCE_LATENCY = Histogram(
    "lzd_inference_latency_seconds", "Tong thoi gian xu ly 1 request (s)",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 1),
)
FEATURE_LOOKUP_LATENCY = Histogram(
    "lzd_feature_lookup_seconds", "Thoi gian doc feature tu Redis (s)",
    buckets=(0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1),
)
FEATURE_LOOKUP = Counter(
    "lzd_feature_lookup_total", "So lan doc feature", ["result"]  # hit|miss|partial
)
FEATURES_MISSING = Histogram(
    "lzd_features_missing_count", "So feature phai dung default vi thieu",
    buckets=(0, 1, 2, 5, 10, 20, 50, 100),
)
MODEL_INFO = Gauge(
    "lzd_model_info", "Model dang duoc load (gia tri luon = 1)",
    ["model_name", "model_version", "source"],
)


def start_metrics_server(port: int | None = None) -> None:
    """Mo endpoint /metrics cho Prometheus scrape."""
    port = port or get_settings().metrics_port
    start_http_server(port)
    log.info("metrics server dang chay", extra={"event": "metrics_server_start", "port": port})


@contextmanager
def observe(histogram: Histogram) -> Iterator[None]:
    started = time.perf_counter()
    try:
        yield
    finally:
        histogram.observe(time.perf_counter() - started)


# ===========================================================================
# 2. Metric cua task batch (Airflow) -> Pushgateway
# ===========================================================================

# Ten metric ma DAG hay day len - khai bao o day de thong nhat voi dashboard
BATCH_METRIC_NAMES = {
    "rows_processed": "lzd_batch_rows_processed",
    "duration_seconds": "lzd_batch_duration_seconds",
    "feature_sync_total_shards": "lzd_feature_sync_total_shards",
    "feature_sync_completed_shards": "lzd_feature_sync_completed_shards",
    "feature_sync_written_rows": "lzd_feature_sync_written_rows",
    "feature_sync_expected_rows": "lzd_feature_sync_expected_rows",
    "feature_sync_duration_seconds": "lzd_feature_sync_duration_seconds",
    "feature_sync_write_rate": "lzd_feature_sync_write_rate_rows_per_sec",
    "feature_store_age_seconds": "lzd_feature_store_age_seconds",
    "feature_validation_mismatch_ratio": "lzd_feature_validation_mismatch_ratio",
    "feature_versions_present": "lzd_feature_versions_present",
    "table_freshness_hours": "lzd_table_freshness_hours",
}


def push_batch_metrics(
    job: str,
    metrics: dict[str, float],
    labels: dict[str, str] | None = None,
    gauge_help: str = "LZD batch pipeline metric",
) -> None:
    """Day 1 nhom gauge len Pushgateway.

    job    : nhan dinh danh (thuong la dag_id) - Pushgateway group theo cai nay
    labels : label phu (feature_version, dag_id, task_id...)
    """
    settings = get_settings()
    registry = CollectorRegistry()
    labels = labels or {}
    label_names = sorted(labels)

    for name, value in metrics.items():
        metric_name = name if name.startswith("lzd_") else f"lzd_{name}"
        gauge = Gauge(metric_name, gauge_help, label_names, registry=registry)
        if label_names:
            gauge.labels(*[labels[k] for k in label_names]).set(float(value))
        else:
            gauge.set(float(value))

    try:
        # Khong dung grouping_key trung ten voi label cua metric (Pushgateway se tu choi).
        push_to_gateway(
            settings.pushgateway_url.replace("http://", ""),
            job=job,
            registry=registry,
        )
        log.info(
            "day metric len pushgateway",
            extra={"event": "metrics_pushed", "job": job, "metrics": list(metrics)},
        )
    except Exception as exc:  # khong duoc lam fail task chi vi metric
        log.warning(
            "khong day duoc metric",
            extra={"event": "metrics_push_failed", "job": job, "error": str(exc)},
        )


def push_info_metric(job: str, name: str, labels: dict[str, str]) -> None:
    """Day 1 metric dang 'info' (gia tri = 1, thong tin nam o label).

    Vd: lzd_feature_store_active_version_info{feature_version="v20260805"} 1
    """
    push_batch_metrics(job, {name: 1.0}, labels=labels)

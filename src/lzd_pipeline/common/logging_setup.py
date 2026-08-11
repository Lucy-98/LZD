"""Structured logging (JSON) - de Promtail parse va Grafana/Loki loc theo field.

Cach dung:
    from lzd_pipeline.common.logging_setup import get_logger
    log = get_logger(__name__)
    log.info("sync bat dau", extra={"event": "sync_start", "feature_version": "v20260805"})

Moi dong log ra stdout co dang:
    {"ts": "...", "level": "INFO", "logger": "...", "service": "stream-consumer",
     "message": "sync bat dau", "event": "sync_start", "feature_version": "v20260805"}

Trong Grafana Explore query duoc: {component="stream-consumer"} | json | event="sync_start"
"""
from __future__ import annotations

import logging
import os
import sys
from typing import Any

try:
    from pythonjsonlogger import jsonlogger

    _HAS_JSON_LOGGER = True
except ImportError:  # fallback khi chay test local khong cai lib
    _HAS_JSON_LOGGER = False


_CONFIGURED = False


class _ContextFilter(logging.Filter):
    """Gan them field co dinh vao moi log record."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def filter(self, record: logging.LogRecord) -> bool:
        record.service = self.service
        # Airflow set nhung bien nay khi chay task -> giup trace tu Grafana ve dung task
        record.dag_id = os.environ.get("AIRFLOW_CTX_DAG_ID", "")
        record.task_id = os.environ.get("AIRFLOW_CTX_TASK_ID", "")
        record.airflow_run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "")
        return True


def configure_logging(service: str | None = None, level: str | None = None) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    service = service or os.environ.get("SERVICE_NAME", "lzd-pipeline")
    level = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()

    handler = logging.StreamHandler(sys.stdout)
    if _HAS_JSON_LOGGER:
        formatter: logging.Formatter = jsonlogger.JsonFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s",
            rename_fields={"asctime": "ts", "levelname": "level", "name": "logger"},
            timestamp=False,
        )
    else:
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s"
        )
    handler.setFormatter(formatter)
    handler.addFilter(_ContextFilter(service))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    # Giam nhieu tu thu vien
    for noisy in ("botocore", "boto3", "s3transfer", "urllib3", "kafka", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _CONFIGURED = True


def get_logger(name: str, **context: Any) -> logging.LoggerAdapter:
    """Tra ve logger da gan san context (vd feature_version, shard_id)."""
    configure_logging()
    return _PassthroughAdapter(logging.getLogger(name), context)


class _PassthroughAdapter(logging.LoggerAdapter):
    """LoggerAdapter giu nguyen `extra` cua tung loi goi log (mac dinh se bi ghi de)."""

    def process(self, msg: Any, kwargs: dict) -> tuple[Any, dict]:
        extra = dict(self.extra or {})
        extra.update(kwargs.get("extra") or {})
        kwargs["extra"] = extra
        return msg, kwargs

"""Inference model boundary.

The repository currently ships no bundled uplift artifact. The real model
will be supplied later from the external training repo and must match the
30-feature contract before this boundary is enabled.

This module keeps the API honest: it does not fall back to a synthetic score
or any legacy local artifact. Until a real model is wired in, the serving API
can still exercise Redis merge, realtime overlay and logging, but every
decision remains `NO_DECISION`.
"""
from __future__ import annotations

import os
import threading
from abc import ABC, abstractmethod
from typing import Any, Sequence

from lzd_pipeline.common.logging_setup import get_logger
from lzd_pipeline.common.metrics import MODEL_INFO

log = get_logger(__name__)

MODEL_NAME = os.environ.get("MODEL_NAME", "uplift_voucher")
MODEL_STAGE = os.environ.get("MODEL_STAGE", "pending")
EXPECTED_FEATURE_COUNT = 30


class UpliftModel(ABC):
    """Base interface for the serving-side uplift model."""

    name: str = MODEL_NAME
    version: str = "unconfigured"
    feature_order: list[str] = []
    expected_feature_count: int = EXPECTED_FEATURE_COUNT

    @abstractmethod
    def predict_uplift(self, feature_rows: Sequence[dict[str, Any]]) -> list[float]:
        """Return one uplift score per row."""

    @property
    def is_configured(self) -> bool:
        return False

    @property
    def is_stub(self) -> bool:
        return False


class UnconfiguredModel(UpliftModel):
    """Placeholder until the external 30F artifact is installed."""

    version = "unconfigured-30f-pending"
    expected_feature_count = EXPECTED_FEATURE_COUNT

    def predict_uplift(self, feature_rows: Sequence[dict[str, Any]]) -> list[float]:
        raise NotImplementedError(
            "uplift model is not configured yet; install the 30F artifact before "
            "enabling inference"
        )


_model: UpliftModel | None = None
_lock = threading.Lock()


def _load_model() -> UpliftModel:
    model = UnconfiguredModel()
    log.warning(
        "uplift model chua duoc cau hinh",
        extra={
            "event": "model_not_configured",
            "model_name": model.name,
            "model_version": model.version,
            "expected_feature_count": model.expected_feature_count,
        },
    )
    return model


def get_model(force_reload: bool = False) -> UpliftModel:
    """Return the serving model boundary, cached per process."""
    global _model
    if force_reload or _model is None:
        with _lock:
            if force_reload or _model is None:
                _model = _load_model()
                MODEL_INFO.labels(
                    model_name=_model.name,
                    model_version=_model.version,
                    stage=MODEL_STAGE,
                ).set(1)
    return _model

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

import hashlib
import json
import math
import os
import threading
from abc import ABC, abstractmethod
from typing import Any, Sequence

from lzd_pipeline.common.logging_setup import get_logger
from lzd_pipeline.common.metrics import MODEL_INFO
from lzd_pipeline.serving.feature_contract import build_matrix, load_contract

log = get_logger(__name__)

MODEL_NAME = os.environ.get("MODEL_NAME", "uplift_voucher")
MODEL_STAGE = os.environ.get("MODEL_STAGE", "pending")
EXPECTED_FEATURE_COUNT = 30
DEFAULT_MODEL_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
    "models", "uplift_voucher_30f",
)


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


class LightGBMUpliftModel(UpliftModel):
    """Immutable LightGBM text artifact guarded by its handoff contract."""

    def __init__(self, model_dir: str | None = None) -> None:
        self.model_dir = model_dir or os.environ.get("MODEL_DIR", DEFAULT_MODEL_DIR)
        self.contract = None
        self.booster = None
        self.version = "unloaded"
        self.feature_order: list[str] = []

    @property
    def is_configured(self) -> bool:
        return self.booster is not None and self.contract is not None

    def _verify_manifest(self) -> dict[str, Any]:
        root = os.path.abspath(self.model_dir)
        with open(os.path.join(root, "import_manifest.json"), encoding="utf-8") as fh:
            manifest = json.load(fh)
        if manifest.get("feature_count") != EXPECTED_FEATURE_COUNT:
            raise ValueError("model manifest feature_count must be 30")
        for name, expected in manifest.get("sha256", {}).items():
            path = os.path.join(root, name)
            with open(path, "rb") as fh:
                actual = hashlib.sha256(fh.read()).hexdigest()
            if actual != expected:
                raise ValueError(f"artifact checksum mismatch: {name}")
        return manifest

    def load(self) -> "LightGBMUpliftModel":
        import lightgbm as lgb

        manifest = self._verify_manifest()
        contract_path = os.path.join(self.model_dir, "feature_contract.json")
        self.contract = load_contract(contract_path)
        self.feature_order = list(self.contract.order)
        if len(self.feature_order) != EXPECTED_FEATURE_COUNT:
            raise ValueError(f"model contract has {len(self.feature_order)} features, expected 30")
        self.booster = lgb.Booster(model_file=os.path.join(self.model_dir, "model_booster.txt"))
        if self.booster.num_feature() != EXPECTED_FEATURE_COUNT:
            raise ValueError(
                f"LightGBM has {self.booster.num_feature()} features, expected 30"
            )
        sentinel = manifest.get("sentinel") or {}
        if sentinel:
            default_row = {name: self.contract.defaults[name] for name in self.contract.order}
            actual = float(self.booster.predict(build_matrix(self.contract, [default_row]))[0])
            expected = float(sentinel["expected_uplift"])
            tolerance = float(sentinel.get("absolute_tolerance", 1e-12))
            if not math.isclose(actual, expected, rel_tol=0.0, abs_tol=tolerance):
                raise ValueError(
                    f"model sentinel mismatch: expected {expected}, got {actual}"
                )
        self.version = f"{self.contract.version}-{manifest['source_commit'][:12]}"
        return self

    def predict_uplift(self, feature_rows: Sequence[dict[str, Any]]) -> list[float]:
        if not self.is_configured:
            raise RuntimeError("model has not been loaded")
        if not feature_rows:
            return []
        values = self.booster.predict(build_matrix(self.contract, feature_rows))
        result = [float(value) for value in values]
        if not all(math.isfinite(value) for value in result):
            raise ValueError("model returned a non-finite uplift score")
        return result


_model: UpliftModel | None = None
_lock = threading.Lock()


def _load_model() -> UpliftModel:
    try:
        model = LightGBMUpliftModel().load()
        log.info(
            "uplift model da nap",
            extra={"event": "model_loaded", "model_name": model.name,
                   "model_version": model.version, "feature_count": len(model.feature_order)},
        )
        return model
    except Exception as exc:
        model = UnconfiguredModel()
        log.error(
            "uplift model khong hop le; serving fail-closed",
            extra={"event": "model_not_configured", "error": str(exc),
                   "model_version": model.version,
                   "expected_feature_count": model.expected_feature_count},
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

"""=========================== KHUNG CHO TEAMMATE ===========================

Nap model tu MLflow Model Registry (artifact nam tren MinIO).

Da lam san:
  - interface `UpliftModel` ma API dang goi
  - `StubModel` de API chay duoc ngay ca khi chua co model that
    (tra ve score gia lap, model_version = "stub") -> ban van do duoc
    do tre Redis, cache hit, luu luong... truoc khi model san sang
  - vong doi load / reload / health

Teammate can:
  1. Bo `MODEL_STAGE`/`MODEL_URI` vao env
  2. Dien phan `MlflowUpliftModel.load()` va `.predict()`
  3. Xoa StubModel khoi `get_model()`
=========================================================================="""
from __future__ import annotations

import os
import threading
from abc import ABC, abstractmethod
from typing import Any, Sequence

from lzd_pipeline.common.config import get_settings
from lzd_pipeline.common.logging_setup import get_logger
from lzd_pipeline.common.metrics import MODEL_INFO

log = get_logger(__name__)

MODEL_NAME = os.environ.get("MODEL_NAME", "uplift_voucher")
MODEL_STAGE = os.environ.get("MODEL_STAGE", "Production")


class UpliftModel(ABC):
    """Interface duy nhat ma inference-api phu thuoc vao."""

    name: str = MODEL_NAME
    version: str = "unknown"
    feature_order: list[str] = []

    @abstractmethod
    def predict_uplift(self, feature_rows: Sequence[dict[str, Any]]) -> list[float]:
        """Nhan list dict feature (da merge batch + realtime) -> list uplift score."""

    @property
    def is_stub(self) -> bool:
        return False


class StubModel(UpliftModel):
    """Model gia - CHI de he thong chay duoc truoc khi co model that.

    Score = ham bam on dinh theo user + vai feature => co phan bo hop ly,
    tai lap duoc, va TUYET DOI khong duoc dung de ra quyet dinh that.
    """

    version = "stub-0"

    def __init__(self, feature_order: list[str] | None = None) -> None:
        self.feature_order = feature_order or []

    def predict_uplift(self, feature_rows: Sequence[dict[str, Any]]) -> list[float]:
        out = []
        for row in feature_rows:
            base = sum(
                float(row.get(f, 0) or 0)
                for f in ("f30", "f31", "f37", "rt_events_1h", "rt_order_1h")
            )
            out.append(round(((base * 0.017) % 0.2) - 0.05, 6))
        return out

    @property
    def is_stub(self) -> bool:
        return True


class MlflowUpliftModel(UpliftModel):
    """TODO(model): nap model that tu MLflow Registry."""

    def __init__(self, model_name: str = MODEL_NAME, stage: str = MODEL_STAGE) -> None:
        self.name = model_name
        self.stage = stage
        self._model = None

    def load(self) -> "MlflowUpliftModel":
        # ------------------------------------------------------------------
        # TODO(model): bo comment va chinh flavor cho dung (sklearn/pytorch/...)
        #
        # import mlflow
        # from mlflow.tracking import MlflowClient
        #
        # mlflow.set_tracking_uri(get_settings().mlflow_tracking_uri)
        # client = MlflowClient()
        # mv = client.get_model_version_by_alias(self.name, self.stage)  # hoac get_latest_versions
        # self._model = mlflow.sklearn.load_model(f"models:/{self.name}/{mv.version}")
        # self.version = str(mv.version)
        #
        # # Doc lai dung thu tu feature da luu luc train -> chong skew
        # local = mlflow.artifacts.download_artifacts(
        #     run_id=mv.run_id, artifact_path="feature_list.json")
        # self.feature_order = json.load(open(local))["features"]
        # ------------------------------------------------------------------
        raise NotImplementedError("TODO(model): nap model tu MLflow Registry")

    def predict_uplift(self, feature_rows: Sequence[dict[str, Any]]) -> list[float]:
        # TODO(model):
        #   import pandas as pd
        #   x = pd.DataFrame(feature_rows)[self.feature_order]
        #   return self._model.predict(x).tolist()
        raise NotImplementedError("TODO(model): suy luan uplift score")


# ===========================================================================
# Vong doi model trong process API
# ===========================================================================
_model: UpliftModel | None = None
_lock = threading.Lock()


def get_model(force_reload: bool = False) -> UpliftModel:
    global _model
    if _model is not None and not force_reload:
        return _model
    with _lock:
        if _model is not None and not force_reload:
            return _model
        try:
            model: UpliftModel = MlflowUpliftModel().load()
            log.info("nap model tu registry",
                     extra={"event": "model_loaded", "model_version": model.version})
        except Exception as exc:
            from lzd_pipeline.features.spec import load_feature_spec

            model = StubModel(feature_order=load_feature_spec().all_names)
            log.warning(
                "chua co model that -> dung StubModel",
                extra={"event": "model_stub", "reason": str(exc)[:200]},
            )
        MODEL_INFO.labels(
            model_name=model.name, model_version=model.version, stage=MODEL_STAGE
        ).set(1)
        _model = model
        return _model

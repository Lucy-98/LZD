"""Nap model uplift da chot tu artifact notebook trong Docker image.

Nguon model duy nhat:

    /opt/project/models/uplift_voucher/model_booster.txt
    /opt/project/models/uplift_voucher/feature_contract.json

Hai file duoc ``COPY`` boi ``docker/python-service/Dockerfile``. Serving
khong tai model tu registry, khong bind mount artifact tu host va khong co
fallback sang model gia. Doi model = build, tag va deploy mot image moi.

Model la DRLearner (LightGBM, 350 cay, 76 dac trung) tu notebook
``lzd-uplifting-model``. Dau vao gom 55 cot tu Redis, 7 cot ``fe_*`` service
tu tinh va 14 cot dien trung vi theo hop dong. Thu tu cot la mot phan cua hop
dong; LightGBM co the tra du doan sai ma khong bao loi neu thu tu bi lech.
"""
from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Sequence

from lzd_pipeline.common.logging_setup import get_logger
from lzd_pipeline.common.metrics import MODEL_INFO
from lzd_pipeline.serving.feature_contract import (
    ModelFeatureContract,
    build_matrix,
    load_contract,
)

log = get_logger(__name__)

MODEL_NAME = "uplift_voucher"
MODEL_DIR = Path(__file__).resolve().parents[3] / "models" / MODEL_NAME
MODEL_BOOSTER = MODEL_DIR / "model_booster.txt"
MODEL_CONTRACT = MODEL_DIR / "feature_contract.json"


class UpliftModel(ABC):
    """Interface duy nhat ma inference-api phu thuoc vao."""

    name: str = MODEL_NAME
    version: str = "unknown"
    feature_order: list[str] = []

    @abstractmethod
    def predict_uplift(self, feature_rows: Sequence[dict[str, Any]]) -> list[float]:
        """Nhan feature da merge va tra uplift score."""


class LightGBMUpliftModel(UpliftModel):
    """Model notebook da chot, doc tu booster text doc lap phien ban Python."""

    def __init__(
        self,
        booster_path: Path | str = MODEL_BOOSTER,
        contract_path: Path | str = MODEL_CONTRACT,
    ) -> None:
        self.name = MODEL_NAME
        self.booster_path = Path(booster_path)
        self.contract_path = Path(contract_path)
        self._booster: Any = None
        self._contract: ModelFeatureContract | None = None

    @staticmethod
    def _read_booster(path: Path) -> Any:
        """Doc bang Python de ho tro ca duong dan non-ASCII tren Windows."""
        import lightgbm as lgb

        if not path.is_file():
            raise FileNotFoundError(f"khong thay booster trong image: {path}")
        return lgb.Booster(model_str=path.read_text(encoding="utf-8"))

    def load(self) -> "LightGBMUpliftModel":
        if not self.contract_path.is_file():
            raise FileNotFoundError(
                f"khong thay feature contract trong image: {self.contract_path}"
            )

        contract = load_contract(self.contract_path)
        booster = self._read_booster(self.booster_path)
        if booster.num_feature() != len(contract.order):
            raise ValueError(
                f"booster nhan {booster.num_feature()} cot nhung hop dong khai bao "
                f"{len(contract.order)}; dung serving de tranh du doan sai im lang"
            )

        self._contract = contract
        self._booster = booster
        self.feature_order = list(contract.redis_columns)
        self.version = f"{contract.model_name}-{contract.version}"
        return self

    def predict_uplift(self, feature_rows: Sequence[dict[str, Any]]) -> list[float]:
        if self._booster is None or self._contract is None:
            raise RuntimeError("model chua duoc load()")
        if not feature_rows:
            return []
        matrix = build_matrix(self._contract, feature_rows)
        return [float(value) for value in self._booster.predict(matrix)]


_model: UpliftModel | None = None
_lock = threading.Lock()


def get_model() -> UpliftModel:
    """Nap model trong image mot lan cho moi worker process."""
    global _model
    if _model is not None:
        return _model

    with _lock:
        if _model is not None:
            return _model
        model = LightGBMUpliftModel().load()
        MODEL_INFO.labels(
            model_name=model.name,
            model_version=model.version,
            source="docker_image",
        ).set(1)
        log.info(
            "nap model notebook tu Docker image",
            extra={
                "event": "model_loaded",
                "source": "docker_image",
                "model_version": model.version,
                "path": str(model.booster_path),
            },
        )
        _model = model
        return _model

"""Nap model uplift cho inference-api.

    DRLearner (LightGBM, 350 cay, 76 dac trung)
    nguon: notebook `lzd-uplifting-model`, artifact `model_booster.txt`

Chuoi nap, theo dung thu tu:

    1. MlflowUpliftModel     MLflow Model Registry (artifact tren MinIO)
    2. LightGBMUpliftModel   booster bundled trong `models/uplift_voucher/`
    3. StubModel             CUOI CUNG, luon kem canh bao

`StubModel` giu lai co chu dich: no cho phep do tre Redis / cache hit / luu
luong truoc khi registry san sang. 🚫 Score cua no KHONG duoc dung de ra
quyet dinh that.

★ Dau vao model KHONG phai 55 cot ma la 76:

        55  tu Redis (fs_2026_08_v2)
         7  fe_* — service tu tinh
        14  dien mac dinh (trung vi train+val)

Toan bo viec dung vector nam o `feature_contract.py`. Thu tu cot LA MOT PHAN
cua hop dong: LightGBM khong kiem ten cot, sai thu tu se cho du doan sai ma
khong bao loi.
"""
from __future__ import annotations

import os
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Sequence

from lzd_pipeline.common.config import get_settings
from lzd_pipeline.common.logging_setup import get_logger
from lzd_pipeline.common.metrics import MODEL_INFO
from lzd_pipeline.serving.feature_contract import (
    ModelFeatureContract,
    build_matrix,
    load_contract,
)

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


#: Booster LightGBM bundled trong repo — duong dan nay la FALLBACK khi
#: MLflow Registry chua co model version nao.
LOCAL_MODEL_DIR = Path(__file__).resolve().parents[3] / "models" / "uplift_voucher"
LOCAL_BOOSTER = LOCAL_MODEL_DIR / "model_booster.txt"


class LightGBMUpliftModel(UpliftModel):
    """DRLearner (LightGBM, 350 cay, 76 dac trung) — model that.

    ★ VI SAO DUNG `model_booster.txt` CHU KHONG PHAI `model.pkl`
    ----------------------------------------------------------------------
    `[MEASURED]` Hai artifact cho ket qua TRUNG KHIT BIT-FOR-BIT
    (`max|lech| = 0.0`, `array_equal = True` tren 3,000 mau). `predict_cate`
    cua ban `.pkl` chi la mot dong `return self.model.predict(X)`.

    Nhung `.pkl` duoc dong goi bang cloudpickle, ma cloudpickle nhung code
    object — thu KHONG tuong thich giua cac ban Python. Nap no bang Python
    3.14 do `TypeError: code() argument 13 must be str, not int`. Dung no se
    khoa CA image `lzd/python-service` (producer + consumer + api) xuong
    3.10.9. Booster la dinh dang text cua LightGBM, doc duoc o moi ban Python.

    => Cung model, cung dau ra, khong rang buoc phien ban.
    """

    def __init__(self, booster_path: Path | str = LOCAL_BOOSTER) -> None:
        self.name = MODEL_NAME
        self.booster_path = Path(booster_path)
        self._booster: Any = None
        self._contract: ModelFeatureContract | None = None

    # -- vong doi ----------------------------------------------------------
    @staticmethod
    def _read_booster(path: Path) -> Any:
        """Nap booster qua `model_str`, KHONG qua `model_file`.

        LightGBM mo file o tang thu vien C, ma tang do khong xu ly duoc duong
        dan non-ASCII tren Windows — repo nay nam trong "OneDrive\\Máy tính"
        nen `model_file=` do `Could not open`. Doc bang Python roi truyen chuoi
        thi tranh han tang C, va cung hoat dong voi file MLflow tai ve.
        """
        import lightgbm as lgb

        if not path.exists():
            raise FileNotFoundError(f"khong thay booster: {path}")
        return lgb.Booster(model_str=path.read_text(encoding="utf-8"))

    def load(self) -> "LightGBMUpliftModel":
        self._contract = load_contract()
        self._booster = self._read_booster(self.booster_path)

        n_model = self._booster.num_feature()
        n_contract = len(self._contract.order)
        if n_model != n_contract:
            raise ValueError(
                f"booster nhan {n_model} cot nhung hop dong khai bao {n_contract}. "
                "🚫 KHONG duoc chay tiep: LightGBM khong kiem ten cot, sai so chieu "
                "chi lo ra bang du doan sai."
            )

        # `feature_order` la 55 cot DE phai cap. 7 cot fe_* service tu tinh,
        # 14 cot con lai dien mac dinh — API khong can biet chung.
        self.feature_order = list(self._contract.redis_columns)
        self.version = f"{self._contract.model_name}-{self._contract.version}"
        return self

    # -- suy luan ----------------------------------------------------------
    def predict_uplift(self, feature_rows: Sequence[dict[str, Any]]) -> list[float]:
        if self._booster is None or self._contract is None:
            raise RuntimeError("model chua duoc load()")
        if not feature_rows:
            return []
        matrix = build_matrix(self._contract, feature_rows)
        return [float(v) for v in self._booster.predict(matrix)]


class MlflowUpliftModel(LightGBMUpliftModel):
    """Nap booster tu MLflow Model Registry (artifact nam tren MinIO).

    Ke thua toan bo phan suy luan cua `LightGBMUpliftModel` — chi khac cho
    LAY FILE tu dau. Nho vay duong registry va duong local khong the lech
    nhau ve cach dung vector dau vao.
    """

    def __init__(self, model_name: str = MODEL_NAME, stage: str = MODEL_STAGE) -> None:
        super().__init__()
        self.name = model_name
        self.stage = stage

    def load(self) -> "MlflowUpliftModel":
        import mlflow
        from mlflow.tracking import MlflowClient

        mlflow.set_tracking_uri(get_settings().mlflow_tracking_uri)
        client = MlflowClient()
        try:
            mv = client.get_model_version_by_alias(self.name, self.stage)
        except Exception:
            versions = client.get_latest_versions(self.name)
            if not versions:
                raise FileNotFoundError(
                    f"registry chua co version nao cho model {self.name!r}"
                ) from None
            mv = max(versions, key=lambda v: int(v.version))

        # Booster + hop dong duoc log CUNG MOT run => khong the lech nhau.
        self.booster_path = Path(mlflow.artifacts.download_artifacts(
            run_id=mv.run_id, artifact_path="model/model_booster.txt"))
        contract_path = Path(mlflow.artifacts.download_artifacts(
            run_id=mv.run_id, artifact_path="model/feature_contract.json"))

        self._contract = load_contract(contract_path)
        self._booster = self._read_booster(self.booster_path)
        self.feature_order = list(self._contract.redis_columns)
        self.version = str(mv.version)
        return self


# ===========================================================================
# Vong doi model trong process API
# ===========================================================================
_model: UpliftModel | None = None
_lock = threading.Lock()


def _load_chain() -> UpliftModel:
    """Registry -> booster bundled -> StubModel.

    Thu tu nay co y: registry la nguon dung o production; booster bundled cho
    phep chay ngay khi chua dung MLflow; StubModel la buoc CUOI CUNG va luon
    kem canh bao — no KHONG duoc dung de ra quyet dinh that.
    """
    errors: list[str] = []

    try:
        model = MlflowUpliftModel().load()
        log.info("nap model tu MLflow registry",
                 extra={"event": "model_loaded", "source": "mlflow_registry",
                        "model_version": model.version})
        return model
    except Exception as exc:
        errors.append(f"registry: {type(exc).__name__}: {str(exc)[:120]}")

    try:
        model = LightGBMUpliftModel().load()
        log.info("nap booster bundled trong repo",
                 extra={"event": "model_loaded", "source": "local_booster",
                        "model_version": model.version,
                        "path": str(model.booster_path),
                        "note": "registry khong dung duoc: " + errors[0]})
        return model
    except Exception as exc:
        errors.append(f"booster: {type(exc).__name__}: {str(exc)[:120]}")

    from lzd_pipeline.features.spec import load_feature_spec

    stub = StubModel(feature_order=load_feature_spec().batch_names)
    log.warning(
        "🚫 KHONG nap duoc model that -> dung StubModel. Score KHONG dung de "
        "ra quyet dinh that.",
        extra={"event": "model_stub", "reasons": errors},
    )
    return stub


def get_model(force_reload: bool = False) -> UpliftModel:
    global _model
    if _model is not None and not force_reload:
        return _model
    with _lock:
        if _model is not None and not force_reload:
            return _model
        model = _load_chain()
        MODEL_INFO.labels(
            model_name=model.name, model_version=model.version, stage=MODEL_STAGE
        ).set(1)
        _model = model
        return _model

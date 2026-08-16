"""Sinh `feature_contract.json` cho model do `train.py` huan luyen.

★ VI SAO TRAIN PHAI TU SINH HOP DONG
--------------------------------------------------------------------------
`serving/model_loader.py` nap model tu registry bang DUNG hai artifact:

    model/model_booster.txt
    model/feature_contract.json

Thieu hop dong thi `MlflowUpliftModel.load()` nem loi va chuoi fallback tut
xuong booster bundled — nghia la DAG train chay xanh, register xong, goi
reload xong, ma API van dang phuc vu model cu. Hong im lang, dung loai hong
kho phat hien nhat. Nen hop dong duoc log CUNG RUN voi booster.

★ HOP DONG NAY KHAC HOP DONG CUA NOTEBOOK
--------------------------------------------------------------------------
Hai model an hai vector khac nhau — day la su that ve du lieu, khong phai
thu de dung nhat:

    notebook  76 cot = 55 batch + 7 fe_* service tu tinh + 14 dien mac dinh
    train.py  62 cot = 55 batch + 7 rt_* realtime          (khong co cot bu)

Trung hop kho chiu: notebook cung dem duoc so 62 ("62/76 cot lay tu redis"),
nhung 62 do la 55+7 fe_*, con 62 o day la 55+7 rt_*. Hai tap KHAC NHAU. Dung
doi chieu hai con so nay voi nhau.

Vi moi cot deu doc thang tu Redis nen hop dong nay khong co cot dan xuat va
khong co cot dien mac dinh: `build_row()` chi viec lay dung 62 gia tri ma
`spec.merge()` da tron san (batch tu `fs:`, realtime tu `rt:`).

★ GIA TRI MAC DINH LA TRUNG VI, KHONG PHAI 0
--------------------------------------------------------------------------
`gia_tri_mac_dinh` chi duoc dung khi feature store thieu cot hoac tra ve NaN.
Dat 0.0 cho mot cot co trung vi 0.965 la day mau ra ngoai mien huan luyen,
model van tra ve so nhung la so vo nghia. Nen mac dinh lay TRUNG VI do tren
chinh frame vua train.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from lzd_pipeline.common.logging_setup import get_logger
from lzd_pipeline.features.spec import FeatureSpec
from lzd_pipeline.training import dataset as ds

log = get_logger(__name__)

CONTRACT_FILENAME = "feature_contract.json"

#: `nguon_gia_tri` ma `feature_contract.load_contract()` hieu la "doc tu store".
_SOURCE_REDIS = "redis"


def _column_stats(frame, columns: list[str]) -> dict[str, dict[str, float]]:
    """Trung vi + khoang cua tung cot, doc tren chinh frame vua huan luyen."""
    import math

    stats: dict[str, dict[str, float]] = {}
    for col in columns:
        series = frame[col]
        median = float(series.median()) if len(series) else 0.0
        lo = float(series.min()) if len(series) else 0.0
        hi = float(series.max()) if len(series) else 0.0
        # Cot toan NaN cho median = NaN; ghi NaN vao JSON la sinh ra token
        # `NaN` khong hop le voi json.loads o phia doc.
        stats[col] = {
            "median": 0.0 if math.isnan(median) else median,
            "min": 0.0 if math.isnan(lo) else lo,
            "max": 0.0 if math.isnan(hi) else hi,
        }
    return stats


def build_contract(
    frame,
    spec: FeatureSpec,
    *,
    version: str,
    model_name: str = "DRLearner",
    run_id: str | None = None,
) -> dict[str, Any]:
    """Hop dong mo ta dung vector ma model vua train nhan vao.

    `frame` la training frame (da loc theu spec) — dung de do trung vi lam
    gia tri mac dinh. Thu tu cot lay tu `ds.feature_columns(spec)`, DUNG thu
    tu da dua vao `model.fit()`.
    """
    columns = ds.feature_columns(spec)
    stats = _column_stats(frame, columns)

    return {
        "phien_ban": version,
        "mo_hinh": model_name,
        "file_mo_hinh": "model_booster.txt",
        "nguon": "train.py",
        "mlflow_run_id": run_id,
        "feature_spec_version": spec.version,
        "so_cot_mo_hinh_nhan_vao": len(columns),
        "so_dac_trung_tu_redis": len(columns),
        "so_dac_trung_dien_mac_dinh": 0,
        "cach_dung": (
            "Dung vector du "
            f"{len(columns)} cot theo thu_tu_dua_vao_mo_hinh. Moi cot deu doc tu "
            "feature store: cot batch tu fs:{version}:u:{user_id}, cot rt_* tu "
            "overlay realtime. Khong co cot dien mac dinh."
        ),
        "canh_bao_thu_tu": (
            "Sai thu tu cot se cho du doan sai ma KHONG bao loi. Luon dung "
            "vector theo thu_tu_dua_vao_mo_hinh."
        ),
        "chia_viec": {
            "DE_tinh_va_luu_redis": len(columns),
            # Rong co y: model nay khong dung cot fe_* nao, service khong phai
            # tu tinh gi ca.
            "AI_service_tu_tinh": [],
        },
        "dac_trung": [
            {
                "ten": col,
                "thu_tu_dua_vao_mo_hinh": i,
                "nguon_gia_tri": _SOURCE_REDIS,
                "nguon": "goc",
                "cong_thuc": None,
                "kieu": "lien_tuc",
                "gia_tri_mac_dinh": stats[col]["median"],
                "khoang": [stats[col]["min"], stats[col]["max"]],
            }
            for i, col in enumerate(columns)
        ],
    }


def write_contract(contract: dict[str, Any], directory: Path | str) -> Path:
    """Ghi hop dong canh booster, roi doc lai de KIEM TRUOC KHI log.

    Doc lai bang chinh `load_contract()` cua serving: neu hop dong sai thi no
    hong ngay o day, luc con sua duoc — chu khong phai luc inference-api
    khoi dong luc 3 gio sang.
    """
    from lzd_pipeline.serving.feature_contract import load_contract

    path = Path(directory) / CONTRACT_FILENAME
    path.write_text(
        json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    parsed = load_contract(path)
    if len(parsed.order) != int(contract["so_cot_mo_hinh_nhan_vao"]):
        raise ValueError(
            f"hop dong vua ghi doc lai ra {len(parsed.order)} cot, khai bao "
            f"{contract['so_cot_mo_hinh_nhan_vao']}"
        )
    log.info(
        "da ghi feature contract",
        extra={
            "event": "training_contract_written",
            "path": str(path),
            "n_columns": len(parsed.order),
        },
    )
    return path

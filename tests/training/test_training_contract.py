"""Hop dong dac trung do `train.py` sinh ra — `training/contract.py`.

★ Rui ro ma lop test nay canh: hong IM LANG giua train va serve.

`MlflowUpliftModel.load()` doi hai artifact trong cung run. Thieu
`feature_contract.json` thi no nem loi, chuoi fallback tut xuong booster
bundled, va DAG train van xanh — model moi khong bao gio duoc phuc vu ma
khong co gi bao. Nen o day kiem: hop dong sinh ra phai NAP DUOC bang chinh
`load_contract()` cua serving, va phai dung thu tu cot da dua vao `fit()`.
"""
from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pytest

SPEC_PATH = str(Path(__file__).resolve().parents[2] / "config" / "features" / "feature_spec.yml")
os.environ.setdefault("FEATURE_SPEC_PATH", SPEC_PATH)

from lzd_pipeline.features.spec import load_feature_spec  # noqa: E402
from lzd_pipeline.serving.feature_contract import (  # noqa: E402
    ContractError,
    build_matrix,
    load_contract,
)
from lzd_pipeline.training import dataset as ds  # noqa: E402
from lzd_pipeline.training.contract import build_contract, write_contract  # noqa: E402


class _Column:
    """Phan API cua `pandas.Series` ma `build_contract` thuc su dung.

    `requirements-dev.txt` co y KHONG cai pandas — bo test chay bang numpy
    thuan. Dung `importorskip("pandas")` o day se lam ca file im lang bo qua
    trong chinh moi truong dev chuan, tuc la khong kiem gi ca.
    """

    def __init__(self, values) -> None:
        self._v = np.asarray(values, dtype=float)

    def __len__(self) -> int:
        return len(self._v)

    def median(self) -> float:
        return float(np.median(self._v))

    def min(self) -> float:
        return float(self._v.min())

    def max(self) -> float:
        return float(self._v.max())


class _Frame(dict):
    """`frame[col]` -> `_Column`. `build_contract` khong dung gi hon."""

    def __getitem__(self, key) -> _Column:
        return _Column(super().__getitem__(key))


@pytest.fixture(scope="module")
def spec():
    return load_feature_spec(SPEC_PATH)


@pytest.fixture(scope="module")
def frame(spec):
    """Frame gia, du de do trung vi: cot thu i mang gia tri i, i+2, i+4."""
    cols = ds.feature_columns(spec)
    return _Frame({c: [i, i + 2.0, i + 4.0] for i, c in enumerate(cols)})


@pytest.fixture
def contract(frame, spec, tmp_path):
    raw = build_contract(frame, spec, version="test-1", run_id="r0")
    return load_contract(write_contract(raw, tmp_path))


def test_nap_duoc_bang_loader_cua_serving(contract, spec):
    """Phep kiem chinh: serving doc duoc dung file ma training ghi ra."""
    assert len(contract.order) == len(ds.feature_columns(spec))
    assert contract.model_name == "DRLearner"


def test_thu_tu_cot_dung_bang_thu_tu_luc_fit(contract, spec):
    """Sai thu tu => LightGBM tra ve so sai ma khong bao loi. Xem contract."""
    assert list(contract.order) == ds.feature_columns(spec)


def test_khong_co_cot_dan_xuat_va_cot_dien_mac_dinh(contract):
    """Model 62 cot doc thang tu store: khong cot fe_*, khong cot bu.

    Khac han hop dong 76 cot cua notebook — do la ly do `_validate` khong
    duoc doi hoi tap cot dan xuat phai bang dung 7 cot fe_*.
    """
    assert contract.derived_columns == ()
    assert contract.default_columns == ()
    assert len(contract.redis_columns) == len(contract.order)


def test_gia_tri_mac_dinh_la_trung_vi_khong_phai_0(contract, frame):
    """0.0 cho mot cot co trung vi khac 0 la day mau ra ngoai mien train."""
    for name, value in contract.defaults.items():
        assert value == pytest.approx(frame[name].median())
    assert any(v != 0.0 for v in contract.defaults.values())


def test_build_matrix_lay_dung_gia_tri_theo_thu_tu(contract):
    row = {name: float(i) for i, name in enumerate(contract.order)}
    matrix = build_matrix(contract, [row])
    assert matrix == [[float(i) for i in range(len(contract.order))]]


def test_cot_thieu_trong_row_rot_ve_trung_vi(contract):
    """Feature store miss mot cot => dien trung vi, khong dien 0."""
    missing = contract.order[0]
    row = {name: 0.0 for name in contract.order if name != missing}
    assert build_matrix(contract, [row])[0][0] == pytest.approx(
        contract.defaults[missing]
    )


def test_hop_dong_khai_bao_cot_dan_xuat_khong_tinh_duoc_thi_bi_tu_choi(
    frame, spec, tmp_path
):
    """Noi rang buoc derived KHONG duoc lam mat phep kiem quan trong: hop dong
    doi mot cot ma `compute_derived` khong biet tinh thi phai hong ngay."""
    import json

    raw = build_contract(frame, spec, version="test-2")
    raw["chia_viec"]["AI_service_tu_tinh"] = ["fe_khong_ton_tai"]
    raw["dac_trung"].append({
        "ten": "fe_khong_ton_tai",
        "thu_tu_dua_vao_mo_hinh": len(raw["dac_trung"]),
        "nguon_gia_tri": "redis",
        "nguon": "dan_xuat",
        "cong_thuc": "khong co cai dat",
        "kieu": "lien_tuc",
        "gia_tri_mac_dinh": 0.0,
    })
    raw["so_cot_mo_hinh_nhan_vao"] += 1
    raw["chia_viec"]["DE_tinh_va_luu_redis"] = len(raw["dac_trung"]) - 1

    path = tmp_path / "bad_contract.json"
    path.write_text(json.dumps(raw, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(ContractError, match="khong tinh duoc"):
        load_contract(path)

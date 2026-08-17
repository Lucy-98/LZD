"""Dung vector dau vao model — `serving/decide_input.py`.

★ Rui ro ma lop test nay canh: HAI TANG MAC DINH CHONG LEN NHAU.

    spec.merge()             dien 0.0     (feature_spec.yml)
    feature_contract._num()  dien TRUNG VI (hop dong model)

0.0 la mot gia tri CO MAT, nen no che mat trung vi. `[MEASURED]` 28/55 cot
chung lech nhau — `f1` spec 0.0 vs trung vi 172.0. Hau qua da gap: `/decide`
tra -0.128802 con `/decide/batch` tra +0.003643 cho CUNG mot user, va moi user
cache-miss deu bi cham tren vector nam ngoai mien huan luyen.

Nen phep kiem quan trong nhat o day khong phai "co chay khong" ma la: cot
THIEU phai vang mat khoi row, de hop dong con co hoi dien trung vi cua no.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
os.environ.setdefault("FEATURE_SPEC_PATH", str(ROOT / "config" / "features" / "feature_spec.yml"))

from lzd_pipeline.features.spec import load_feature_spec  # noqa: E402
from lzd_pipeline.serving.decide_input import (  # noqa: E402
    ContextError,
    build_model_row,
    check_context,
)


class _FakeModel:
    """Chi can `feature_order` — day la thu API phai lay tu store."""

    def __init__(self, feature_order):
        self.feature_order = list(feature_order)


@pytest.fixture(scope="module")
def spec():
    return load_feature_spec()


@pytest.fixture
def model_76(spec):
    """Model notebook: nhan 55 cot batch tu store, KHONG cot rt_* nao."""
    return _FakeModel([n for n in spec.all_names if not n.startswith("rt_")])


# ===========================================================================
# Cot thieu phai VANG MAT, khong duoc dien 0.0
# ===========================================================================
def test_cot_thieu_khong_duoc_co_mat_trong_row(spec, model_76):
    """Phep kiem trung tam: store rong => row rong, KHONG phai row toan 0.0."""
    row, missing, supplied, _ = build_model_row(spec, model_76.feature_order, {}, {}, {})
    assert row == {}
    assert missing == len(spec.all_names)
    assert supplied == 0


def test_khong_dien_0_cho_cot_co_trung_vi_khac_0(spec, model_76):
    """`f1` trung vi 172.0 — dien 0.0 la day mau ra ngoai mien huan luyen."""
    row, _, _, _ = build_model_row(spec, model_76.feature_order, {"f2": "5"}, {}, {})
    assert "f1" not in row          # thieu => de hop dong dien 172.0
    assert row["f2"] == pytest.approx(5.0)


def test_gia_tri_rong_cung_tinh_la_thieu(spec, model_76):
    """Redis tra chuoi rong cho field chua ghi — khong duoc coi la 0.0."""
    row, missing, _, _ = build_model_row(spec, model_76.feature_order, {"f1": ""}, {}, {})
    assert "f1" not in row
    assert missing == len(spec.all_names)


# ===========================================================================
# Thu tu uu tien: context > realtime > batch
# ===========================================================================
def test_context_ghi_de_realtime_va_batch(spec, model_76):
    row, _, _, _ = build_model_row(
        spec, model_76.feature_order,
        {"rt_order_1h": "1"}, {"rt_order_1h": "2"}, {"rt_order_1h": 3},
    )
    assert row["rt_order_1h"] == 3


def test_realtime_ghi_de_batch(spec, model_76):
    """Du lieu moi hon thang du lieu cu."""
    row, _, _, _ = build_model_row(
        spec, model_76.feature_order, {"rt_order_1h": "1"}, {"rt_order_1h": "2"}, {}
    )
    assert row["rt_order_1h"] == 2


# ===========================================================================
# realtime_applied — su that ve event
# ===========================================================================
def test_model_76_cot_khong_bao_gio_nhan_duoc_realtime(spec, model_76):
    """Model notebook khong co o nao cho rt_* => luon bang 0, du client gui du.

    Day chinh la cai `features_missing` KHONG noi ra duoc: no giam khi client
    gui rt_*, nghe nhu event da co tac dung, trong khi model khong dung den.
    """
    rt = {n: 9 for n in spec.all_names if n.startswith("rt_")}
    row, _, _, realtime_applied = build_model_row(spec, model_76.feature_order, {}, rt, {})
    assert realtime_applied == 0
    assert any(n.startswith("rt_") for n in row)   # co trong row, nhung...
    assert not any(n.startswith("rt_") for n in model_76.feature_order)  # ...model khong doi
# ===========================================================================
# context validation
# ===========================================================================
def test_khoa_la_bi_tu_choi_422(spec):
    """Bo qua im lang nghia la client tuong da gui tin hieu ma khong he co."""
    with pytest.raises(ContextError) as exc:
        check_context(spec, {"khoa_bia": 1, "rt_order_1h": 2})
    assert exc.value.unknown_keys == ["khoa_bia"]


def test_context_hop_le_di_qua(spec):
    check_context(spec, {"rt_order_1h": 2, "rt_gmv_1h": 10.0})


def test_context_rong_di_qua(spec):
    check_context(spec, {})

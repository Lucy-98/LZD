"""Model uplift that — hop dong 76 cot, thu tu, gia tri mac dinh, suy luan.

Model: DRLearner (LightGBM, 350 cay), ban giao tu `lzd-uplifting-model`.

★ Rui ro lon nhat cua lop nay KHONG phai crash — ma la DU DOAN SAI IM LANG.
LightGBM nhan mang so, khong nhan ten cot. Dua sai thu tu thi no van chay, van
tra ve so, chi la so sai. Nen phan lon file nay kiem THU TU va NGUON GIA TRI
chu khong kiem "co chay khong".
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from lzd_pipeline.reconstruction.feature_set import load_feature_set
from lzd_pipeline.serving.feature_contract import (
    ContractError,
    build_matrix,
    build_row,
    compute_derived,
    load_contract,
)

pytest.importorskip("lightgbm", reason="can lightgbm de nap booster")

ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = ROOT / "models" / "uplift_voucher"


@pytest.fixture(scope="module")
def contract():
    return load_contract()


@pytest.fixture(scope="module")
def model():
    from lzd_pipeline.serving.model_loader import LightGBMUpliftModel

    return LightGBMUpliftModel().load()


# ===========================================================================
# Hop dong
# ===========================================================================
def test_hop_dong_dung_76_cot(contract):
    assert len(contract.order) == 76
    assert len(contract.redis_columns) == 55
    assert len(contract.derived_columns) == 7
    assert len(contract.default_columns) == 14
    assert 55 + 7 + 14 == 76


def test_thu_tu_lien_tuc_va_khong_trung(contract):
    """`thu_tu_dua_vao_mo_hinh` phai la 0..75 lien tuc."""
    assert [s.index for s in contract.slots] == list(range(76))
    assert len(set(contract.order)) == 76


def test_55_cot_redis_khop_selected_feature_set(contract):
    """★ Cau noi giua model va pipeline.

    Model doi dung 55 cot ma `fs_2026_08_v2` sinh ra. Lech mot cot la feature
    store thieu dung thu model can, va no se bi thay bang gia tri mac dinh —
    im lang.
    """
    assert set(contract.redis_columns) == set(load_feature_set().columns)


def test_ba_nguon_gia_tri_khong_chong_lan(contract):
    redis = set(contract.redis_columns)
    derived = set(contract.derived_columns)
    default = set(contract.default_columns)
    assert not redis & derived and not redis & default and not derived & default
    assert redis | derived | default == set(contract.order)


def test_14_cot_mac_dinh_dung_cot_da_biet(contract):
    """9/14 la muc one-hot ma pipeline CO tinh (`intermediate_only`).

    Giu mac dinh la quyet dinh CO Y de du doan trung ban da benchmark. Test
    nay khoa danh sach lai: doi no phai la quyet dinh tuong minh.
    """
    assert set(contract.default_columns) == {
        "f7", "f14", "f36", "f48", "f49", "f50", "f51",
        "f55", "f56", "f60", "f61", "f63", "f66", "f75",
    }
    fs = load_feature_set()
    da_tinh = set(contract.default_columns) & fs.intermediate_only
    assert da_tinh == {"f48", "f49", "f50", "f51", "f55", "f56", "f60", "f61", "f63"}


# ===========================================================================
# Dung vector
# ===========================================================================
def _row(contract, value: float = 0.5) -> dict[str, float]:
    return {c: value for c in contract.redis_columns}


def test_vector_dung_76_chieu(contract):
    assert len(build_row(contract, _row(contract))) == 76


def test_cot_thieu_thi_dung_TRUNG_VI_khong_phai_0(contract):
    """🚫 Mac dinh ve 0.0 se day mau ra ngoai mien huan luyen.

    `f36` co trung vi 0.965; dien 0.0 cho no la mot gia tri hop le nhung
    KHONG dai dien — model se cham vao vung it thay luc train.
    """
    vec = build_row(contract, {})            # khong cap cot nao
    for slot in contract.slots:
        if not slot.derived:
            assert vec[slot.index] == pytest.approx(slot.default), slot.name
    assert contract.defaults["f36"] == pytest.approx(0.9650470018386841)


def test_cot_mac_dinh_KHONG_bi_ghi_de_boi_input(contract):
    """14 cot mac dinh la hang so — dua chung vao input cung khong duoc doi.

    Neu cho ghi de, hai moi truong cap khac nhau se cho du doan khac nhau ma
    hop dong noi la 'khong anh huong'.
    """
    row = _row(contract)
    row.update({c: 999.0 for c in contract.default_columns})
    vec = build_row(contract, row)
    for name in contract.default_columns:
        slot = contract.slot_of(name)
        assert vec[slot.index] == pytest.approx(slot.default)


def test_gia_tri_NaN_va_None_roi_ve_mac_dinh(contract):
    row = _row(contract)
    row["f1"], row["f2"] = None, float("nan")
    vec = build_row(contract, row)
    assert vec[contract.slot_of("f1").index] == pytest.approx(contract.defaults["f1"])
    assert vec[contract.slot_of("f2").index] == pytest.approx(contract.defaults["f2"])


def test_thu_tu_dict_dau_vao_khong_anh_huong(contract):
    row = _row(contract)
    assert build_row(contract, row) == build_row(contract, dict(reversed(list(row.items()))))


# ===========================================================================
# 7 dac trung dan xuat
# ===========================================================================
def test_cong_thuc_dan_xuat_dung_contract(contract):
    row = {"f1": 10.0, "f2": 4.0, "f9": 3.0, "f26": 2.0, "f27": 1.0, "f16": 1.0}
    d = compute_derived(row, contract.defaults)
    assert d["fe_ratio_f1_f2"] == pytest.approx(10.0 / 5.0)
    assert d["fe_ratio_f9_f27"] == pytest.approx(3.0 / 2.0)
    assert d["fe_inter_f9_f26"] == pytest.approx(6.0)


def test_hai_cot_dan_xuat_LUON_bang_0_o_production(contract):
    """`fe_ratio_f14_f16` va `fe_inter_f14_f27` deu nhan `f14`, ma `f14` nam
    trong 14 cot mac dinh (= 0.0). Day la HE QUA cua hop dong, khong phai loi
    — ghi lai de khong ai mat cong debug.
    """
    assert contract.defaults["f14"] == 0.0
    d = compute_derived(_row(contract, 7.0), contract.defaults)
    assert d["fe_ratio_f14_f16"] == 0.0
    assert d["fe_inter_f14_f27"] == 0.0


def test_fe_flag_sum_chi_con_f68_la_bien(contract):
    """f66 va f75 la hang so mac dinh (1.0 moi cai) => tong = 2 + f68."""
    for f68 in (0.0, 1.0):
        row = _row(contract)
        row["f68"] = f68
        assert compute_derived(row, contract.defaults)["fe_flag_sum"] == pytest.approx(2.0 + f68)


def test_cai_dat_dan_xuat_phu_het_contract(contract):
    assert set(compute_derived({}, contract.defaults)) == set(contract.derived_columns)


# ===========================================================================
# Nap model + suy luan
# ===========================================================================
def test_booster_khop_so_cot_hop_dong(model, contract):
    assert model._booster.num_feature() == len(contract.order) == 76


def test_feature_order_la_55_cot_DE_phai_cap(model, contract):
    """API chi doi DE cap 55 cot; 7 dan xuat va 14 mac dinh la viec cua service."""
    assert model.feature_order == list(contract.redis_columns)
    assert len(model.feature_order) == 55


def test_version_mang_thong_tin_hop_dong(model):
    assert model.version == "DRLearner-20260813"


def test_suy_luan_ra_so_thuc(model, contract):
    scores = model.predict_uplift([_row(contract, v) for v in (0.1, 0.5, 0.9)])
    assert len(scores) == 3
    assert all(isinstance(s, float) for s in scores)


def test_input_rong_tra_ve_rong(model):
    assert model.predict_uplift([]) == []


def test_tat_dinh(model, contract):
    rows = [_row(contract, 0.3)]
    assert model.predict_uplift(rows) == model.predict_uplift(rows)


def test_doi_mot_cot_quan_trong_thi_doi_score(model, contract):
    """Chong loi 'vector dung dung nhung toan gia tri mac dinh'."""
    a = _row(contract, 0.2)
    b = dict(a, f30=1.4, f5=9.0, f2=3.0)
    assert model.predict_uplift([a])[0] != model.predict_uplift([b])[0]


# ===========================================================================
# ★ Doi chieu voi ban goc — bit-for-bit
# ===========================================================================
GOLDEN = MODEL_DIR / "golden_predictions.json"


def test_trung_khit_voi_ban_goc(model, contract):
    """★ Bai kiem quan trong nhat cua file nay.

    `golden_predictions.json` sinh tu `model.pkl` GOC chay tren Python 3.10.9
    (moi truong dong goi cua notebook). Duong serving nay chay Python khac,
    dung artifact khac (`model_booster.txt`), va tu dung vector 76 cot.

    Trung khit => ca ba thu deu dung: thu tu cot, gia tri mac dinh, cong thuc
    dan xuat. Lech => mot trong ba sai, va test noi ro sai cho nao.
    """
    golden = json.loads(GOLDEN.read_text(encoding="utf-8"))
    assert golden["contract_version"] == contract.version

    scores = model.predict_uplift(golden["rows"])
    assert len(scores) == len(golden["scores"])
    for i, (got, want) in enumerate(zip(scores, golden["scores"])):
        assert got == pytest.approx(want, abs=0.0, rel=0.0), (
            f"row {i}: booster={got} vs model.pkl goc={want}"
        )


def test_golden_phu_du_bien_the(contract):
    golden = json.loads(GOLDEN.read_text(encoding="utf-8"))
    assert len(golden["rows"]) >= 100
    assert set(golden["rows"][0]) == set(contract.redis_columns)


# ===========================================================================
# Hop dong hong thi phai NO, khong duoc doan
# ===========================================================================
def test_hop_dong_thieu_cot_thi_bao_loi(tmp_path, contract):
    raw = json.loads((MODEL_DIR / "feature_contract.json").read_text(encoding="utf-8"))
    raw["dac_trung"] = raw["dac_trung"][:-1]          # bo mot cot -> thung thu tu
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ContractError):
        load_contract(bad)


def test_hop_dong_sai_so_cot_khai_bao_thi_bao_loi(tmp_path):
    raw = json.loads((MODEL_DIR / "feature_contract.json").read_text(encoding="utf-8"))
    raw["so_cot_mo_hinh_nhan_vao"] = 99
    bad = tmp_path / "bad2.json"
    bad.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ContractError, match="so cot lech"):
        load_contract(bad)

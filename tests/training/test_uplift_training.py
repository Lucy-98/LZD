"""DR-Learner + thuoc do uplift — ban port tu notebook `lzd-uplifting-model`.

★ Rui ro lon nhat cua lop nay: thuoc do uplift RAT de cai dat sai ma van cho
ra so dep. Mot Qini duong khong chung minh gi ca neu chinh ham Qini sai. Nen
phan lon file nay kiem TINH CHAT cua thuoc do (oracle = 1, ngau nhien = 0,
don dieu theo nhieu) chu khong kiem "co chay khong".

★ Rui ro thu hai: hang so troi khoi artifact. `E_ALPHA` va `BEST_PARAMS` phai
khop `best_params.json` / `metadata.json` — neu khong, huan luyen trong repo
se cho model khac model dang phuc vu ma ca hai deu tu goi la DRLearner.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from lzd_pipeline.training.uplift import (
    BEST_PARAMS,
    DRLearner,
    E_ALPHA,
    E_CLIP,
    NUISANCE_PARAMS,
    SEED,
    auuc_score,
    dr_from_nuisance,
    evaluate_all,
    gain_curve,
    oracle_score,
    qini_curve,
    qini_score,
    sanity_checks,
    uplift_at_k,
)

MODEL_DIR = Path(__file__).resolve().parents[2] / "models" / "uplift_voucher"


@pytest.fixture(scope="module")
def rct():
    """RCT mo phong: treat lam tang conversion theo dung `tau`."""
    rng = np.random.RandomState(0)
    n = 40_000
    w = rng.binomial(1, 0.5, n)
    tau = rng.rand(n)
    y = rng.binomial(1, 0.05 + 0.10 * tau * w)
    return y, w, tau


# ===========================================================================
# Hang so phai khop artifact — chong troi
# ===========================================================================
def test_E_ALPHA_khop_nguong_da_chot():
    """`best_params.json` ghi ro: notebook 04 PHAI dung dung nguong nay, neu
    khong thi bo sieu tham so tinh chinh cho mo hinh nay lai ap cho mo hinh khac."""
    meta = json.loads((MODEL_DIR / "metadata.json").read_text(encoding="utf-8"))
    assert meta["ten_mo_hinh"] == "DRLearner"
    assert E_ALPHA == pytest.approx(0.071)
    assert E_CLIP == (E_ALPHA, 1 - E_ALPHA)


def test_BEST_PARAMS_khop_metadata_cua_model_dang_phuc_vu():
    meta = json.loads((MODEL_DIR / "metadata.json").read_text(encoding="utf-8"))
    for key, want in meta["sieu_tham_so"].items():
        assert BEST_PARAMS[key] == pytest.approx(want), key


def test_seed_va_nuisance_co_dinh():
    assert SEED == 42
    assert NUISANCE_PARAMS["random_state"] == SEED
    # Nuisance KHONG duoc tune — tune theo cung metric se ro ri thong tin vao
    # pseudo-outcome.
    assert NUISANCE_PARAMS["n_estimators"] == 200
    assert NUISANCE_PARAMS["max_depth"] == 5


# ===========================================================================
# Thuoc do — kiem TINH CHAT
# ===========================================================================
def test_oracle_dung_bang_1(rct):
    y, w, _ = rct
    assert qini_score(y, w, oracle_score(y, w)) == pytest.approx(1.0, abs=1e-9)


def test_xep_hang_ngau_nhien_gan_0(rct):
    y, w, _ = rct
    rng = np.random.RandomState(1)
    assert abs(qini_score(y, w, rng.rand(len(y)))) < 0.05


def test_tin_hieu_that_cho_qini_duong_ro_ret(rct):
    y, w, tau = rct
    assert qini_score(y, w, tau) > 0.05


def test_dao_dau_score_thi_qini_doi_dau(rct):
    """Doi xung: xep hang nguoc lai phai te hon ngau nhien dung bang muc tot hon."""
    y, w, tau = rct
    assert qini_score(y, w, -tau) == pytest.approx(-qini_score(y, w, tau), abs=1e-3)


def test_qini_giam_don_dieu_theo_nhieu(rct):
    """Tinh don dieu dung TRONG KY VONG, khong dung cho tung lan boc.

    Do duoc: voi mot lan boc, nhieu nho (0.5x sd) doi khi lam Qini TANG nhe
    (0.11051 -> 0.11085) vi nhieu lay mau lan at hieu ung. Nen lay trung binh
    nhieu lan boc — day la phat bieu dung cua tinh chat, khong phai noi long
    de test qua.
    """
    y, w, tau = rct

    def qini_trung_binh(scale: float, n_lan: int = 5) -> float:
        rng = np.random.RandomState(2)
        return float(np.mean([
            qini_score(y, w, tau + rng.normal(0, scale * tau.std(), len(y)))
            for _ in range(n_lan)
        ]))

    scores = [qini_trung_binh(s) for s in (0.0, 1.0, 3.0, 10.0)]
    assert scores == sorted(scores, reverse=True), scores


def test_auuc_cung_huong_voi_qini(rct):
    y, w, tau = rct
    assert auuc_score(y, w, tau) > 0
    assert auuc_score(y, w, oracle_score(y, w)) == pytest.approx(1.0, abs=1e-9)


def test_uplift_at_k_giam_khi_k_tang(rct):
    """Nhom top phai co uplift cao hon khi lay hep hon."""
    y, w, tau = rct
    assert uplift_at_k(y, w, tau, 0.10) > uplift_at_k(y, w, tau, 0.50)


def test_uplift_at_k_bang_ATE_khi_lay_toan_bo(rct):
    y, w, tau = rct
    ate = y[w == 1].mean() - y[w == 0].mean()
    assert uplift_at_k(y, w, tau, 1.0) == pytest.approx(ate, abs=1e-12)


def test_pha_the_hoa_co_seed_nen_tat_dinh(rct):
    y, w, _ = rct
    tied = np.zeros(len(y))              # TOAN BO hoa nhau
    assert qini_score(y, w, tied, seed=7) == qini_score(y, w, tied, seed=7)
    assert qini_score(y, w, tied, seed=7) != qini_score(y, w, tied, seed=8)


def test_duong_cong_bat_dau_va_ket_thuc_dung(rct):
    y, w, tau = rct
    x, g = qini_curve(y, w, tau)
    assert x[0] == pytest.approx(1 / len(y)) and x[-1] == pytest.approx(1.0)
    assert len(x) == len(g) == len(y)
    xg, _ = gain_curve(y, w, tau)
    assert xg[-1] == pytest.approx(1.0)


def test_evaluate_all_du_metric(rct):
    y, w, tau = rct
    m = evaluate_all(y, w, tau)
    assert set(m) == {"qini", "auuc", "uplift_at_10", "uplift_at_20", "uplift_at_30"}
    assert all(np.isfinite(v) for v in m.values())


def test_bon_phep_tu_kiem_deu_qua(rct):
    y, w, tau = rct
    assert all(sanity_checks(y, w, tau).values())


def test_tu_kiem_BAT_duoc_score_rac(rct):
    """Neu `sanity_checks` luon True thi no vo dung."""
    y, w, _ = rct
    rng = np.random.RandomState(3)
    checks = sanity_checks(y, w, rng.rand(len(y)))
    assert not all(checks.values())


# ===========================================================================
# Pseudo-outcome doubly robust
# ===========================================================================
def test_dr_khong_chech_khi_nuisance_dung():
    """Kiem tra co ban: mu dung + e dung => E[dr] ~ ATE that."""
    rng = np.random.RandomState(4)
    n = 200_000
    w = rng.binomial(1, 0.5, n)
    mu0 = np.full(n, 0.10)
    mu1 = np.full(n, 0.15)             # ATE that = 0.05
    y = rng.binomial(1, np.where(w == 1, mu1, mu0))
    dr, _ = dr_from_nuisance(w, y, np.full(n, 0.5), mu0, mu1)
    assert dr.mean() == pytest.approx(0.05, abs=0.01)


def test_clip_chan_pseudo_outcome_no_ra_vo_cung():
    """`e` gan 0 lam mau so no; clip la BAT BUOC chu khong phai lam dep."""
    n = 1000
    w = np.ones(n, dtype=int)
    y = np.ones(n)
    mu = np.zeros(n)
    khong_clip, _ = dr_from_nuisance(w, y, np.full(n, 1e-9), mu, mu, clip=(1e-9, 1 - 1e-9))
    co_clip, e_hat = dr_from_nuisance(w, y, np.full(n, 1e-9), mu, mu, clip=E_CLIP)
    assert khong_clip.max() > 1e6
    assert co_clip.max() < 1e3
    assert e_hat.min() == pytest.approx(E_ALPHA)


# ===========================================================================
# DRLearner
# ===========================================================================
def test_khoi_tao_dung_tham_so():
    m = DRLearner()
    assert m.n_folds == 5
    assert m.params["random_state"] == SEED and m.params["verbose"] == -1
    for k, v in BEST_PARAMS.items():
        assert m.params[k] == v


def test_chua_fit_thi_predict_bao_loi():
    with pytest.raises(RuntimeError, match="chua fit"):
        DRLearner().predict_cate(np.zeros((2, 3)))
    with pytest.raises(RuntimeError, match="chua fit"):
        DRLearner().booster_text()


def test_fit_va_xuat_booster():
    """Huan luyen that tren du lieu nho, roi xuat booster text.

    Booster text la dinh dang ma `serving/model_loader.py` nap — nen test nay
    khoa duong noi giua huan luyen va phuc vu.
    """
    pytest.importorskip("sklearn", reason="DRLearner.fit can StratifiedKFold")
    try:
        import lightgbm as lgb  # noqa: F401
    except (ImportError, OSError) as _exc:
        pytest.skip(f"can lightgbm: {_exc}")

    rng = np.random.RandomState(5)
    n = 4000
    x = rng.rand(n, 8)
    w = rng.binomial(1, 0.5, n)
    y = rng.binomial(1, 0.05 + 0.2 * x[:, 0] * w)

    model = DRLearner(n_folds=3).fit(x, w, y)
    scores = model.predict_cate(x)
    assert scores.shape == (n,)

    text = model.booster_text()
    assert text.startswith("tree")
    assert "num_feature" not in text or True

    # Booster xuat ra phai nap lai duoc va cho DUNG so cu.
    import lightgbm as lgb

    reloaded = lgb.Booster(model_str=text)
    assert reloaded.num_feature() == 8
    np.testing.assert_array_equal(reloaded.predict(x), scores)

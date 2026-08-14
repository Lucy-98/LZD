"""Constructive H1 solver — no CO THAT SU dat argmin khong?

docs/RECONSTRUCTION_SPEC.md §4.4b · §4.4c · §4.4a-1

★ Y TUONG CUA FILE NAY
  `constructive.solve_h1` khong duyet khong gian nghiem, no DUNG thang mot
  nghiem. Khang dinh "nghiem do thuoc argmin" vi vay phai duoc kiem bang mot
  nguon doc lap — chinh la `engine.feasible_candidates` + `engine.optimal_pool`
  (exhaustive enumeration, §4.4c). Voi `window_days` nho, exhaustive chay duoc
  va cho ta su that tuyet doi de doi chieu.

  🚫 KHONG kiem bang cach goi lai chinh `h1_objective_bound` roi so voi no —
     do la so dau ra cua mot ham voi chinh no.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lzd_pipeline.reconstruction import engine
from lzd_pipeline.reconstruction.candidate import SESSION_CAPACITY
from lzd_pipeline.reconstruction.constructive import (
    Infeasible,
    h1_objective_bound,
    solve_h1,
)
from lzd_pipeline.reconstruction.semantics import H1Branch, DecodedTarget

H1 = H1Branch()
_REF = datetime(2026, 8, 1, 23, 59, 59, tzinfo=timezone.utc)

#: Target du nho de exhaustive enumeration chay duoc.
SMALL = [
    pytest.param(dict(n5=1, n11=1, n18=1, n30=3, d1=0, d2=0, window_days=5), id="thua-event"),
    pytest.param(dict(n5=1, n11=1, n18=1, n30=5, d1=0, d2=0, window_days=5), id="vua-du"),
    pytest.param(dict(n5=1, n11=1, n18=1, n30=5, d1=4, d2=2, window_days=5), id="hai-moc-recency"),
    pytest.param(dict(n5=1, n11=1, n18=1, n30=4, d1=9, d2=9, window_days=5), id="recency-ngoai-cua-so"),
    pytest.param(dict(n5=2, n11=1, n18=1, n19=1, n30=3, d1=1, d2=1, window_days=5), id="co-f19"),
    # ★ Truong hop lam GREEDY CU LECH khoi argmin: T_in > CAP*k nen rai deu
    #   tao them session thua. Do duoc tren 6.38% target that.
    pytest.param(dict(n5=6, n11=6, n18=5, n30=3, d1=0, d2=0, window_days=3), id="dac-buoc-pack"),
    pytest.param(dict(n5=4, n11=4, n18=4, n30=2, d1=0, d2=0, window_days=2), id="dac-2-ngay"),
]


# ===========================================================================
# Doi chieu voi exhaustive enumeration — nguon su that doc lap
# ===========================================================================
@pytest.mark.parametrize("kw", SMALL)
def test_nghiem_dung_ra_nam_trong_optimal_pool(kw):
    """§4.4b — day la khang dinh trung tam cua module `constructive`."""
    d = DecodedTarget(**kw)
    pool = engine.optimal_pool(engine.feasible_candidates(d, H1), H1)
    assert pool, "fixture phai co nghiem, neu khong test nay vo nghia"

    got = solve_h1(d, target_id="T", seed=42)
    assert got in pool


@pytest.mark.parametrize("kw", SMALL)
def test_can_duoi_chung_minh_bang_dung_argmin_that(kw):
    """`h1_objective_bound` phai trung khop voi argmin do exhaustive tim ra."""
    d = DecodedTarget(**kw)
    pool = engine.optimal_pool(engine.feasible_candidates(d, H1), H1)
    assert h1_objective_bound(d) == H1.objective(pool[0])


@pytest.mark.parametrize("kw", SMALL)
def test_moi_seed_deu_cho_nghiem_toi_uu(kw):
    """§4.4a-1 — seed chon TRONG pool, khong duoc keo nghiem ra khoi pool."""
    d = DecodedTarget(**kw)
    pool = engine.optimal_pool(engine.feasible_candidates(d, H1), H1)
    for seed in range(12):
        assert solve_h1(d, target_id="T", seed=seed) in pool


# ===========================================================================
# Hoi quy: hai benh cua greedy round-robin cu
# ===========================================================================
def test_khong_con_don_ngay_active_ve_mep_gan_nhat():
    """Greedy cu luon lay ngay 0,1,2,... => moi user co mot khoi lien ke o mep
    cua so. Do la phan bo temporal suy bien ma §4.4 noi seed sinh ra de tranh.
    """
    d = DecodedTarget(n5=2, n11=2, n18=1, n30=15, d1=27, d2=27)
    days = sorted(solve_h1(d, target_id="user-1", seed=42).day_offsets)

    assert 27 in days                       # ngay bi ep van phai co
    in_window = [x for x in days if x < d.window_days]
    assert len(in_window) == d.n30
    # Benh cu: in_window == [0,1,2,...,13,27]. Doi hoi phan bo TRAI RONG hon.
    assert in_window != sorted(range(d.n30 - 1)) + [27]
    assert max(in_window) - min(in_window) > d.n30


def test_cac_user_khac_nhau_khong_dung_chung_mot_tap_ngay():
    """Neu tap ngay khong phu thuoc user thi Gate F (sanity phan bo) vo nghia."""
    d = DecodedTarget(n5=1, n11=1, n18=1, n30=5, d1=40, d2=40)
    day_sets = {
        tuple(sorted(solve_h1(d, target_id=f"user-{i}", seed=42).day_offsets))
        for i in range(25)
    }
    assert len(day_sets) > 1


def test_sessions_dat_can_duoi_ly_thuyet_ngay_ca_khi_day_event():
    """T_in > CAP*k: phai PACK, khong duoc rai deu."""
    d = DecodedTarget(n5=6, n11=6, n18=5, n30=3, d1=0, d2=0, window_days=3)
    c = solve_h1(d, target_id="T", seed=1)

    _, sessions_min = h1_objective_bound(d)
    assert c.sessions == sessions_min
    # Rai deu (greedy cu) cho 5 session; pack cho 4.
    assert c.sessions < 5
    assert c.total_events > SESSION_CAPACITY * d.n30


# ===========================================================================
# Tat dinh — §4.5
# ===========================================================================
def test_cung_input_cho_cung_nghiem():
    d = DecodedTarget(n5=3, n11=2, n18=1, n19=2, n30=7, d1=12, d2=3)
    a = solve_h1(d, target_id="T", seed=7)
    b = solve_h1(d, target_id="T", seed=7)
    assert a.key() == b.key()


def test_doi_seed_thi_doi_nghiem_nhung_van_toi_uu():
    d = DecodedTarget(n5=3, n11=2, n18=1, n30=7, d1=12, d2=3)
    a = solve_h1(d, target_id="T", seed=7)
    b = solve_h1(d, target_id="T", seed=8)
    assert a.key() != b.key()
    assert H1.objective(a) == H1.objective(b) == h1_objective_bound(d)


# ===========================================================================
# Bien scope: f19 chi ton tai o v2
# ===========================================================================
def test_target_khong_co_f19_thi_khong_phat_evt_f19():
    """Neu solver van phat EVT_F19 cho target v1, forward engine se dem duoc
    n19 > 0 trong khi target khong co cot f19 de doi chieu => lech am tham."""
    d = DecodedTarget(n5=2, n11=1, n18=1, n30=3, d1=1, d2=1)
    c = solve_h1(d, target_id="T", seed=1)

    assert c.count_of("f19") == 0
    events = engine.materialize(c, target_id="T", seed=1, reference_ts=_REF)
    assert "EVT_F19" not in {e.event_type for e in events}


def test_target_co_f19_thi_phat_dung_so_luong():
    d = DecodedTarget(n5=2, n11=1, n18=1, n19=4, n30=3, d1=1, d2=1)
    c = solve_h1(d, target_id="T", seed=1)

    assert c.count_of("f19") == 4
    events = engine.materialize(c, target_id="T", seed=1, reference_ts=_REF)
    assert sum(1 for e in events if e.event_type == "EVT_F19") == 4


# ===========================================================================
# Quarantine — 🚫 KHONG sua target de cho pass
# ===========================================================================
def test_q_forced_days_thi_bao_infeasible():
    """n30 < |forced_days| — mau thuan trong CHINH target (8/926,669 dong)."""
    d = DecodedTarget(n5=1, n11=1, n18=1, n30=1, d1=5, d2=2)
    with pytest.raises(Infeasible, match="Q_FORCED_DAYS"):
        solve_h1(d, target_id="T", seed=1)


def test_n30_lon_hon_cua_so_thi_bao_infeasible():
    d = DecodedTarget(n5=1, n11=1, n18=1, n30=5, d1=0, d2=0, window_days=4)
    with pytest.raises(Infeasible, match="window_days"):
        solve_h1(d, target_id="T", seed=1)

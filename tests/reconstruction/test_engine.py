"""Slice 2 — TEST-07 truoc, roi TEST-01/02/03/04/06.

TEST-07 di dau vi no kiem `semantic_branch` co THUC SU di vao engine khong,
hay chi ton tai trong config.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lzd_pipeline.reconstruction import engine
from lzd_pipeline.reconstruction.candidate import GEN_FREE, Candidate, Slot
from lzd_pipeline.reconstruction.semantics import (
    REASON_F5,
    REASON_F11,
    REASON_F18,
    REASON_F30,
    DecodedTarget,
    H1Branch,
    H2Branch,
    build_branch,
)

REF_TS = datetime(2026, 8, 1, tzinfo=timezone.utc)


# ===========================================================================
# TEST-07 · ★ H1/H2 objective differentiation
#
#   candidate A:  unexplained = 1,  active_days = 10,  sessions = 2
#   candidate B:  unexplained = 2,  active_days =  3,  sessions = 1
#
#   H1 = (unexplained, sessions):  A=(1,2)  B=(2,1)  -> 1<2 -> chon A
#   H2 = (active_days, sessions):  A=(10,2) B=(3,1)  -> 3<10 -> chon B
# ===========================================================================
@pytest.fixture
def candidate_a() -> Candidate:
    """10 ngay active, 1 FREE, 10 event => sessions = 10*ceil(1/5) = 10.

    De dat sessions=2 ta don event vao 2 ngay... nhung active_days phai =10.
    => dung 10 ngay, moi ngay 1 event => sessions=10.
    Fixture duoi day dieu chinh so lieu cho khop CONG THUC, giu nguyen
    QUAN HE thu tu ma TEST-07 can.
    """
    slots = [Slot(GEN_FREE, 0, 1)] + [Slot(REASON_F5, d, 1) for d in range(1, 10)]
    return Candidate.of(slots)


@pytest.fixture
def candidate_b() -> Candidate:
    """3 ngay active, 2 FREE."""
    slots = [Slot(GEN_FREE, 0, 1), Slot(GEN_FREE, 1, 1), Slot(REASON_F5, 2, 1)]
    return Candidate.of(slots)


def test_07_metric_cua_fixture_dung_nhu_thiet_ke(candidate_a, candidate_b):
    assert (candidate_a.unexplained_events, candidate_a.active_days) == (1, 10)
    assert (candidate_b.unexplained_events, candidate_b.active_days) == (2, 3)


def test_07_h1_va_h2_chon_NGUOC_nhau(candidate_a, candidate_b):
    """★ Chung minh phat hien §4.2 khong chi nam tren giay."""
    h1, h2 = H1Branch(), H2Branch()
    pool = [candidate_a, candidate_b]

    assert h1.objective(candidate_a) < h1.objective(candidate_b)   # H1 -> A
    assert h2.objective(candidate_b) < h2.objective(candidate_a)   # H2 -> B

    assert engine.optimal_pool(pool, h1) == (candidate_a,)
    assert engine.optimal_pool(pool, h2) == (candidate_b,)


def test_07_objective_tuple_dung_thanh_phan(candidate_a):
    """§4.2b — thu tu trong tuple LA MOT PHAN CUA CONTRACT."""
    assert H1Branch().objective(candidate_a) == (
        candidate_a.unexplained_events, candidate_a.sessions
    )
    assert H2Branch().objective(candidate_a) == (
        candidate_a.active_days, candidate_a.sessions
    )


def test_07_active_days_bi_ghim_duoi_h1_nen_khong_vao_objective():
    """§4.2a — feature equality ep active_days == n30 => metric hang so."""
    d = DecodedTarget(n5=2, n11=1, n18=1, n30=2, d1=1, d2=1, window_days=4)
    h1 = H1Branch()
    feasible = engine.feasible_candidates(d, h1)
    assert feasible, "fixture phai co nghiem"
    # MOI nghiem kha thi deu co active_days == n30 => dua vao objective la vo nghia
    assert {c.active_days for c in feasible} == {d.n30}


# ===========================================================================
# TEST-01 · optimal POOL dung — so POOL, khong so nghiem
# ===========================================================================
@pytest.fixture
def small_target() -> DecodedTarget:
    return DecodedTarget(n5=2, n11=1, n18=1, n30=2, d1=1, d2=1, window_days=4)


def _exhaustive_argmin(feasible, branch):
    best = min(branch.objective(c) for c in feasible)
    return tuple(sorted((c for c in feasible if branch.objective(c) == best), key=lambda c: c.key()))


@pytest.mark.parametrize("branch", [H1Branch(), H2Branch()])
def test_01_pool_khop_exhaustive_argmin(small_target, branch):
    feasible = engine.feasible_candidates(small_target, branch)
    assert feasible, "fixture phai co nghiem"
    expected = _exhaustive_argmin(feasible, branch)
    actual = engine.optimal_pool(feasible, branch)
    assert actual == expected


def test_01_pool_khong_phai_nghiem_dau_tien(small_target):
    """Bat loi: solver tra nghiem kha thi DAU TIEN thay vi toi uu."""
    h1 = H1Branch()
    feasible = engine.feasible_candidates(small_target, h1)
    pool = engine.optimal_pool(feasible, h1)
    best = h1.objective(pool[0])
    assert all(h1.objective(c) == best for c in pool)
    # co nghiem kha thi TE HON pool => pool that su la argmin, khong phai ca tap
    assert any(h1.objective(c) > best for c in feasible)


def test_01_pool_tat_dinh_khong_phu_thuoc_thu_tu_dau_vao(small_target):
    h1 = H1Branch()
    feasible = engine.feasible_candidates(small_target, h1)
    assert engine.optimal_pool(feasible, h1) == engine.optimal_pool(list(reversed(feasible)), h1)


# ===========================================================================
# TEST-03 · doi seed — CHAT LUONG khong doi
# ===========================================================================
def test_03_doi_seed_khong_doi_chat_luong(small_target):
    h1 = H1Branch()
    pool = engine.optimal_pool(engine.feasible_candidates(small_target, h1), h1)

    picks = [
        engine.select_from_pool(pool, target_id="T", seed=s)
        for s in (1, 2, 3, 99, 12345)
    ]
    objs = {h1.objective(p) for p in picks}
    assert len(objs) == 1, "seed khong duoc anh huong CHAT LUONG nghiem"
    assert all(p in pool for p in picks), "nghiem chon phai NAM TRONG optimal pool"
    # 🚫 KHONG assert cac witness khac nhau — |pool|==1 la binh thuong (§4.4b)


def test_03_cung_seed_cho_cung_nghiem(small_target):
    h1 = H1Branch()
    pool = engine.optimal_pool(engine.feasible_candidates(small_target, h1), h1)
    a = engine.select_from_pool(pool, target_id="T", seed=7)
    b = engine.select_from_pool(pool, target_id="T", seed=7)
    assert a is b or a.key() == b.key()


# ===========================================================================
# TEST-04 · ★ mutant seed-before-optimization PHAI fail
# ===========================================================================
def _mutant_solve(d, branch, *, target_id, seed):
    """🚫 BIEN THE SAI: seed -> random FEASIBLE -> roi moi nhin objective."""
    feasible = engine.feasible_candidates(d, branch)
    return engine.select_from_pool(feasible, target_id=target_id, seed=seed)


def test_04_mutant_chon_duoc_nghiem_SUBOPTIMAL(small_target):
    """Neu mutant KHONG BAO GIO lo, nghia la BO TEST hong, khong phai code dung.

    Fixture phai co du nhieu candidate suboptimal de mutant cham vao chung.
    """
    h1 = H1Branch()
    feasible = engine.feasible_candidates(small_target, h1)
    pool = engine.optimal_pool(feasible, h1)
    assert len(feasible) > len(pool), "fixture phai co candidate SUBOPTIMAL"

    best = h1.objective(pool[0])
    picks = [_mutant_solve(small_target, h1, target_id="T", seed=s) for s in range(60)]
    assert any(h1.objective(p) > best for p in picks), (
        "mutant khong lo => fixture chua du manh (SPEC §15 TEST-04)"
    )


def test_04_engine_that_khong_bao_gio_chon_suboptimal(small_target):
    h1 = H1Branch()
    pool = engine.optimal_pool(engine.feasible_candidates(small_target, h1), h1)
    best = h1.objective(pool[0])
    for s in range(60):
        out = engine.solve(small_target, h1, target_id="T", seed=s, reference_ts=REF_TS)
        assert out.objective_summary.objective_value == best


def test_04_select_from_pool_khong_the_nhan_tap_feasible_day_du(small_target):
    """INVARIANT 3 duoc thuc thi bang CAU TRUC HAM.

    `select_from_pool` chi nhan `pool`. Trong `solve()`, doi so cua no la ket
    qua cua `optimal_pool()` — khong co duong nao truyen tap feasible vao.
    """
    import inspect

    src = inspect.getsource(engine.solve)
    assert "optimal_pool(feasible" in src
    assert "select_from_pool(pool" in src
    # P2 phai dung TRUOC P3 trong than ham
    assert src.index("optimal_pool(") < src.index("select_from_pool(")


# ===========================================================================
# TEST-06 · logical_slot / event_id doc lap traversal
# ===========================================================================
def test_06_event_id_doc_lap_thu_tu_slot():
    """Dao thu tu slot dau vao — event_id phai giong het."""
    slots = [Slot(REASON_F5, d, 2) for d in (0, 1, 2)]
    a = engine.materialize(Candidate.of(slots), target_id="T", seed=1, reference_ts=REF_TS)
    b = engine.materialize(
        Candidate.of(list(reversed(slots))), target_id="T", seed=1, reference_ts=REF_TS
    )
    assert {e.event_id for e in a} == {e.event_id for e in b}


def test_06_anh_xa_slot_id_toi_event_id_on_dinh():
    slots = [Slot(REASON_F5, 0, 3), Slot(REASON_F11, 0, 2)]
    ev = engine.materialize(Candidate.of(slots), target_id="T", seed=1, reference_ts=REF_TS)
    m = {e.candidate_slot_id: e.event_id for e in ev}
    ev2 = engine.materialize(
        Candidate.of(list(reversed(slots))), target_id="T", seed=1, reference_ts=REF_TS
    )
    assert {e.candidate_slot_id: e.event_id for e in ev2} == m


def test_06_occurrence_tinh_trong_pham_vi_tung_gen_reason():
    """§4.3a-2 — f5 danh so rieng, f11 danh so rieng."""
    slots = [Slot(REASON_F5, 0, 3), Slot(REASON_F11, 0, 2)]
    ev = engine.materialize(Candidate.of(slots), target_id="T", seed=1, reference_ts=REF_TS)
    occ = {}
    for e in ev:
        occ.setdefault(e.gen_reason, []).append(e.occurrence)
    assert sorted(occ[REASON_F5]) == [0, 1, 2]
    assert sorted(occ[REASON_F11]) == [0, 1]


def test_06_occurrence_khong_dung_event_ts():
    """Tranh vong dinh nghia: event_ts phu thuoc sub_index, nen occurrence
    phai dung (day_offset, sub_index) chu KHONG dung event_ts."""
    slots = [Slot(REASON_F5, 5, 1), Slot(REASON_F5, 1, 1)]
    ev = engine.materialize(Candidate.of(slots), target_id="T", seed=1, reference_ts=REF_TS)
    by_day = {e.day_offset: e.occurrence for e in ev}
    assert by_day[1] < by_day[5], "occurrence phai theo day_offset tang dan"


# ===========================================================================
# TEST-02 · replay determinism
# ===========================================================================
def test_02_cung_input_cho_cung_witness(small_target):
    h1 = H1Branch()
    a = engine.solve(small_target, h1, target_id="T", seed=42, reference_ts=REF_TS)
    b = engine.solve(small_target, h1, target_id="T", seed=42, reference_ts=REF_TS)
    assert [(e.event_id, e.event_ts) for e in a.events] == [
        (e.event_id, e.event_ts) for e in b.events
    ]


def test_02_thu_tu_output_la_canonical(small_target):
    out = engine.solve(small_target, H1Branch(), target_id="T", seed=42, reference_ts=REF_TS)
    keys = [(e.event_ts, e.event_type, e.event_id) for e in out.events]
    assert keys == sorted(keys)


# ===========================================================================
# Gate B · moi event Track A nam TRUOC reference_ts
# ===========================================================================
def test_gate_b_moi_event_truoc_reference_ts(small_target):
    out = engine.solve(small_target, H1Branch(), target_id="T", seed=3, reference_ts=REF_TS)
    assert out.events
    assert all(e.event_ts < REF_TS for e in out.events)


# ===========================================================================
# Bat bien khac
# ===========================================================================
def test_d1_phai_lon_hon_hoac_bang_d2():
    with pytest.raises(ValueError, match="d1 >= d2"):
        DecodedTarget(n5=1, n11=1, n18=1, n30=1, d1=2, d2=5)


def test_forced_days_ngoai_cua_so_khong_ep_active():
    d = DecodedTarget(n5=1, n11=1, n18=1, n30=1, d1=100, d2=100, window_days=30)
    assert d.forced_days == frozenset()


def test_quarantine_khi_forced_days_vuot_n30():
    """Q_FORCED_DAYS — hai ngay bi ep active nhung n30=1."""
    d = DecodedTarget(n5=1, n11=1, n18=1, n30=1, d1=3, d2=1, window_days=10)
    out = engine.solve(d, H1Branch(), target_id="T", seed=1, reference_ts=REF_TS)
    assert out.status == "QUARANTINED"
    assert out.reason_code == "NO_FEASIBLE_CANDIDATE"
    assert out.events == ()


def test_h2_khong_bao_gio_sinh_free_event():
    d = DecodedTarget(n5=1, n11=1, n18=1, n30=2, d1=1, d2=1, window_days=3)
    out = engine.solve(d, H2Branch(), target_id="T", seed=1, reference_ts=REF_TS)
    assert out.status == "SOLVED"
    assert out.objective_summary.unexplained_events == 0
    assert all(e.gen_reason != GEN_FREE for e in out.events)


def test_build_branch_tu_choi_ten_la():
    with pytest.raises(ValueError, match="H1"):
        build_branch(None)

"""Slice 3 — cau hoi trung tam:

    "Mot T0 state da duoc Track A xac nhan co the ban giao sang Track B de sinh
     tuong lai ma Track B KHONG THE nhin nguoc vao target hay reconstruction
     internals hay khong?"

Phu: TEST-10b (capability boundary tren live_generator) · handoff ·
     state integrity · future-only · TEST-08 (phan engine) · TEST-02 (worker).
"""
from __future__ import annotations

import inspect
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from lzd_pipeline.reconstruction import capability, engine, live
from lzd_pipeline.reconstruction.handoff import (
    GateReport,
    to_customer_state,
)
from lzd_pipeline.reconstruction.live import (
    RuleBasedBehaviour,
    TemporalViolation,
    live_generator,
)
from lzd_pipeline.reconstruction.semantics import DecodedTarget, H1Branch
from lzd_pipeline.reconstruction.state import (
    CustomerState,
    HandoffRefused,
    Provenance,
)

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
RECON = SRC_ROOT / "lzd_pipeline" / "reconstruction"
REF_TS = datetime(2026, 8, 1, tzinfo=timezone.utc)

#: 🚫 Track B khong duoc reach toi bat ky module nao trong day (INVARIANT 4).
TRACK_B_FORBIDDEN = (
    "lzd_pipeline.reconstruction.target",
    "lzd_pipeline.reconstruction.canonical",
    "lzd_pipeline.reconstruction.feature_set",
    "lzd_pipeline.reconstruction.handoff",
    "lzd_pipeline.reconstruction.engine",
    "lzd_pipeline.reconstruction.semantics",
    "lzd_pipeline.reconstruction.candidate",
)

ALL_GREEN = GateReport(gate_a=True, gate_a_t3=True, gate_b=True, gate_c=True)


@pytest.fixture
def decoded() -> DecodedTarget:
    return DecodedTarget(n5=2, n11=1, n18=1, n30=2, d1=1, d2=1, window_days=4)


@pytest.fixture
def outcome(decoded):
    return engine.solve(decoded, H1Branch(), target_id="T-1", seed=42, reference_ts=REF_TS)


def _prov():
    return Provenance(
        source_type="RECONSTRUCTED",
        generation_run_id="run-1",
        root_generation_id="run-1",
        parent_target_id="T-1",
    )


def _state(outcome, decoded, gates=ALL_GREEN, **over):
    kw = dict(
        customer_id="C1",
        target_id="T-1",
        reference_ts=REF_TS,
        attributes={"synthetic_category_515": 12},
        semantic_branch="H1",
        semantic_status="UNIDENTIFIED",
        provenance=_prov(),
    )
    kw.update(over)
    return to_customer_state(outcome, decoded, gates, **kw)


# ===========================================================================
# ★ TEST-10b · CAPABILITY BOUNDARY tren live_generator
# ===========================================================================
def test_10b_live_module_khong_reachable_toi_track_a():
    """Cau hoi trung tam cua Slice 3, tra loi bang do thi import BAC CAU."""
    capability.assert_cannot_reach(
        entry=RECON / "live.py",
        forbidden=TRACK_B_FORBIDDEN,
        src_root=SRC_ROOT,
        why=(
            "live.py la Track B. Neu no reach duoc target repository, "
            "source_target_id tro thanh lookup key va INVARIANT 4 vo."
        ),
    )


def test_10b_state_module_cung_khong_reachable():
    """`state.py` la mat tien cua boundary — no cung phai mu."""
    capability.assert_cannot_reach(
        entry=RECON / "state.py",
        forbidden=TRACK_B_FORBIDDEN,
        src_root=SRC_ROOT,
    )


def test_10b_handoff_DUOC_PHEP_reach_track_a():
    """Kiem nguoc lai: handoff nam PHIA TRACK A nen duoc phep.

    Neu test nay fail thi checker dang bao dong gia — nguy hiem khong kem bo lot.
    """
    seen, _ = capability.reachable_modules(RECON / "handoff.py", SRC_ROOT)
    assert "lzd_pipeline.reconstruction.engine" in seen


# ===========================================================================
# TEST-10a · live_generator khong nhan target
# ===========================================================================
def test_10a_live_generator_khong_co_tham_so_target():
    sig = inspect.signature(live_generator)
    assert "target" not in sig.parameters
    assert "reconstruction_target" not in sig.parameters
    ann = {str(p.annotation) for p in sig.parameters.values()}
    assert not any("ReconstructionTarget" in a for a in ann)


def test_10a_behaviour_model_khong_nhan_target():
    sig = inspect.signature(RuleBasedBehaviour.next_events)
    assert "target" not in sig.parameters
    assert set(sig.parameters) == {"self", "state", "t_from", "t_to", "rng_seed"}


# ===========================================================================
# Handoff · §3.4a
# ===========================================================================
def test_handoff_solved_tao_ra_state(outcome, decoded):
    assert outcome.status == "SOLVED"
    st = _state(outcome, decoded)
    assert isinstance(st, CustomerState)
    assert st.as_of_ts == REF_TS
    assert st.source_target_id == "T-1"
    assert st.counters["f30"] == decoded.n30


def test_handoff_quarantined_bi_tu_choi(decoded):
    """T0 chua xac nhan => moi event tuong lai bam vao trang thai sai."""
    bad = DecodedTarget(n5=1, n11=1, n18=1, n30=1, d1=3, d2=1, window_days=10)
    out = engine.solve(bad, H1Branch(), target_id="T-x", seed=1, reference_ts=REF_TS)
    assert out.status == "QUARANTINED"
    with pytest.raises(HandoffRefused, match="chua duoc xac nhan"):
        _state(out, bad)


@pytest.mark.parametrize("gate", ["gate_a", "gate_a_t3", "gate_b", "gate_c"])
def test_handoff_bi_tu_choi_khi_bat_ky_gate_nao_fail(outcome, decoded, gate):
    gates = GateReport(**{**ALL_GREEN.__dict__, gate: False})
    with pytest.raises(HandoffRefused, match="Gate chua pass"):
        _state(outcome, decoded, gates=gates)


def test_gate_report_khong_mac_dinh_true():
    """Mac dinh True se bien ban giao thanh khong dieu kien — dung thu §3.4a cam."""
    params = inspect.signature(GateReport).parameters
    assert all(p.default is inspect.Parameter.empty for p in params.values())


def test_state_last_event_ts_lay_tu_witness(outcome, decoded):
    st = _state(outcome, decoded)
    assert st.last_event_ts == max(e.event_ts for e in outcome.events)
    assert st.last_event_ts < st.as_of_ts       # Gate B


# ===========================================================================
# State integrity · chi field duoc phep qua boundary
# ===========================================================================
def test_state_khong_mang_gia_tri_feature(outcome, decoded):
    st = _state(outcome, decoded)
    blob = repr(st).lower()
    for bad in ("target_hash", "feature_payload", "selected_feature_set", "values="):
        assert bad not in blob


def test_state_chi_mang_counter_va_attribute(outcome, decoded):
    st = _state(outcome, decoded)
    assert set(st.counters) == {"f5", "f11", "f18", "f30"}
    assert set(st.attributes) == {"synthetic_category_515"}


# ===========================================================================
# Future-only · Gate B cho Track B
# ===========================================================================
def test_moi_event_track_b_nam_tu_as_of_ts_tro_di(outcome, decoded):
    st = _state(outcome, decoded)
    evs = list(live_generator(st, RuleBasedBehaviour(), REF_TS, REF_TS + timedelta(days=3)))
    assert evs
    assert all(e.event_ts >= st.as_of_ts for e in evs)


def test_khong_duoc_sinh_nguoc_vao_cua_so_track_a(outcome, decoded):
    st = _state(outcome, decoded)
    with pytest.raises(TemporalViolation, match="cua so cua Track A"):
        list(live_generator(st, RuleBasedBehaviour(), REF_TS - timedelta(days=1), REF_TS))


def test_track_a_va_track_b_khong_giao_nhau_ve_thoi_gian(outcome, decoded):
    st = _state(outcome, decoded)
    future = list(live_generator(st, RuleBasedBehaviour(), REF_TS, REF_TS + timedelta(days=2)))
    assert max(e.event_ts for e in outcome.events) < REF_TS <= min(e.event_ts for e in future)


def test_event_id_hai_track_khong_bao_gio_dung_nhau(outcome, decoded):
    st = _state(outcome, decoded)
    future = list(live_generator(st, RuleBasedBehaviour(), REF_TS, REF_TS + timedelta(days=2)))
    assert not {e.event_id for e in outcome.events} & {e.event_id for e in future}


def test_track_b_tat_dinh_theo_rng_seed(outcome, decoded):
    st = _state(outcome, decoded)
    a = [e.event_id for e in live_generator(st, RuleBasedBehaviour(), REF_TS, REF_TS + timedelta(days=2), rng_seed=5)]
    b = [e.event_id for e in live_generator(st, RuleBasedBehaviour(), REF_TS, REF_TS + timedelta(days=2), rng_seed=5)]
    assert a == b and a


def test_t2_level_dieu_khien_hanh_vi_track_b(outcome, decoded):
    state = _state(outcome, decoded)
    changed = replace(state, attributes={"synthetic_category_515": 13})
    a = list(live_generator(
        state, RuleBasedBehaviour(), REF_TS, REF_TS + timedelta(days=2), rng_seed=5
    ))
    b = list(live_generator(
        changed, RuleBasedBehaviour(), REF_TS, REF_TS + timedelta(days=2), rng_seed=5
    ))
    assert [(e.event_type, e.event_ts) for e in a] != [
        (e.event_type, e.event_ts) for e in b
    ]


def test_rule_based_khong_can_ancestor_model_ids():
    """P-4 — rule-based la co che sinh hop le, model id rong LA CHINH DANG."""
    p = Provenance(
        source_type="SYNTHETIC", generation_run_id="r", root_generation_id="r0",
        behaviour_policy_version=RuleBasedBehaviour().policy_version,
    )
    assert p.ancestor_model_ids == ()


def test_future_event_mang_provenance_day_du(outcome, decoded):
    st = _state(outcome, decoded)
    event = next(iter(live_generator(
        st, RuleBasedBehaviour(), REF_TS, REF_TS + timedelta(days=1)
    )))
    assert event.provenance.source_type == "SYNTHETIC"
    assert event.provenance.parent_run_id == st.provenance.generation_run_id
    assert event.provenance.behaviour_policy_version == "rule_v1"


# ===========================================================================
# TEST-08 · semantic_status KHONG doi output (phan engine)
# ===========================================================================
def test_08_engine_khong_bao_gio_nhin_thay_semantic_status():
    """Thuc thi bang CHU KY, khong bang kiem tra runtime."""
    assert "semantic_status" not in inspect.signature(engine.solve).parameters
    for fn in (engine.optimal_pool, engine.select_from_pool, engine.materialize):
        assert "semantic_status" not in inspect.signature(fn).parameters


def test_08_doi_status_khong_doi_witness(decoded):
    """Cung target + branch + seed, khac status => witness GIONG HET."""
    a = engine.solve(decoded, H1Branch(), target_id="T", seed=9, reference_ts=REF_TS)
    b = engine.solve(decoded, H1Branch(), target_id="T", seed=9, reference_ts=REF_TS)
    st_u = _state(a, decoded, semantic_status="UNIDENTIFIED")
    st_s = _state(b, decoded, semantic_status="H1_SUPPORTED")
    assert [e.event_id for e in a.events] == [e.event_id for e in b.events]
    assert st_u.counters == st_s.counters and st_u.attributes == st_s.attributes


# ===========================================================================
# TEST-02 · replay determinism qua WORKER COUNT
# ===========================================================================
@pytest.mark.parametrize("workers", [1, 8])
def test_02_ket_qua_khong_doi_theo_so_worker(decoded, workers):
    def run(i):
        return engine.solve(
            decoded, H1Branch(), target_id=f"T{i}", seed=i, reference_ts=REF_TS
        )

    with ThreadPoolExecutor(max_workers=workers) as ex:
        got = list(ex.map(run, range(12)))
    return_keys = [[(e.event_id, e.event_ts) for e in o.events] for o in got]

    with ThreadPoolExecutor(max_workers=1) as ex:
        base = list(ex.map(run, range(12)))
    assert return_keys == [[(e.event_id, e.event_ts) for e in o.events] for o in base]

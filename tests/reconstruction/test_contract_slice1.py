"""Proof-of-contract prototype — slice 1.

Phu: feature-set artifact (INVARIANT 1) · canonical hash (§6) ·
     encoding bijection (§8) · TEST-05 · TEST-09.

TEST-01/02/03/04/06/07/08 can solver engine -> slice sau.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lzd_pipeline.reconstruction import canonical, encoding
from lzd_pipeline.reconstruction.feature_set import DEFAULT_PATH, load_feature_set
from lzd_pipeline.reconstruction.target import (
    LeakageViolation,
    ReconstructionTarget,
    ScopeViolation,
    SolverConfig,
    TamperDetected,
    build_target,
)

REF_TS = datetime(2026, 8, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def fs():
    return load_feature_set()


def _values(fs, fill=1.0) -> dict[str, float]:
    return {c: fill for c in fs.columns}


# ===========================================================================
# Feature set artifact — INVARIANT 1
# ===========================================================================
def test_scope_la_dung_30_cot(fs):
    assert fs.id == "fs_2026_08_v4"
    assert len(fs.columns) == 30
    assert len(fs.tiers["T1"]) == 2
    assert len(fs.tiers["T2"]) == 7
    assert len(fs.tiers["T3"]) == 21


def test_invariant_1_do_artifact_chot_chu_khong_phai_code(fs):
    """Con so scope phai nam trong CHINH artifact, khong hard-code trong code.

    Neu no nam trong code thi moi lan mo scope, cach de nhat de vuot assert la
    xoa assert. Nam trong yaml thi doi scope BAT BUOC hien ra o git diff cua
    hop dong, kem theo mot `id` moi.
    """
    assert fs.expected_column_count == len(fs.columns)


def test_gate_a_va_gate_a_t3_tach_rieng(fs):
    """§12 — gop hai gate lam ti le pass cao gia tao nho copy."""
    assert len(fs.gate_a_columns) == 9        # T1 + T2, thuc su reconstruct
    assert len(fs.gate_a_t3_columns) == 21   # T3, chi copy
    assert not set(fs.gate_a_columns) & set(fs.gate_a_t3_columns)


def test_v3_artifact_la_active_contract(fs):
    assert fs.id == "fs_2026_08_v4"
    assert fs.expected_column_count == 30


def test_cot_trung_gian_khong_lot_vao_target(fs):
    """§1.2 — muc khong duoc chon cua g2/g3/g4/g6 la dau ra trung gian."""
    assert not fs.intermediate_only & fs.column_set
    assert not fs.intermediate_only & fs.column_set


def test_moi_muc_cua_group_deu_duoc_khai_bao(fs):
    """§5 / G-1 — mot muc bi bo quen o CA selected LAN intermediate_only se
    lam SQL dung thieu muc va bat bien one-hot vo mot cach am tham."""
    for attr in fs.source_attributes.values():
        assert not set(attr.outputs) - fs.column_set - fs.intermediate_only


def test_source_attribute_dung_du_muc_cua_group(fs):
    cat = fs.source_attributes["synthetic_category_515"]
    assert cat.levels == 515
    assert set(cat.outputs) == {"f79", "f80", "f81", "f82"}  # MOT bien, 4 encoding


def test_g1_duoc_chon_du_ca_group(fs):
    """Group DUY NHAT nam tron trong target => target mang san sum(g1)==1.

    He qua: 55 cot nhung it hon 55 bac tu do. Day la su that ve selection,
    ghi lai de khong ai doc "55 cot" thanh "55 tin hieu doc lap".
    """
    g1 = fs.source_attributes["synthetic_segment_g1"]
    assert set(g1.selected) == {"f42"}
    assert set(g1.outputs) == {"f40", "f41", "f42"}


def test_regime_gan_dung_cot(fs):
    assert fs.regime_of("f5") == "LN"
    assert fs.regime_of("f1") == "REC"
    assert fs.regime_of("f82") == "CAT"
    assert fs.regime_of("f23") == "PASS"


def test_dung_sai_chi_ap_dung_cho_regime_ln(fs):
    """§7 — LOG10 khop chinh xac 100%; LN chi 93% => phai co dung sai."""
    assert fs.tolerance_of("f5") == pytest.approx(1e-15)
    assert fs.tolerance_of("f1") is None


# ===========================================================================
# TEST-09 · scope duoc thuc thi (INVARIANT 1)
# ===========================================================================
def test_09_target_chua_83_cot_bi_tu_choi(fs):
    """Ai do 'tien tay' mo scope len 83 vi target vo tinh mang du du lieu."""
    v = _values(fs)
    v.update({f"f{i}": 0.0 for i in range(83)})   # nhet ca f0..f82
    with pytest.raises(ScopeViolation, match="DUNG 30 cot"):
        build_target(target_id="T", values=v, reference_ts=REF_TS)


def test_09_target_thieu_cot_bi_tu_choi(fs):
    v = _values(fs)
    del v["f82"]
    with pytest.raises(ScopeViolation):
        build_target(target_id="T", values=v, reference_ts=REF_TS)


# ===========================================================================
# I-1 / Gate E · leakage
# ===========================================================================
@pytest.mark.parametrize("bad", ["label", "is_treat"])
def test_label_va_is_treat_khong_bao_gio_vao_solver(fs, bad):
    v = _values(fs)
    v[bad] = 1.0
    with pytest.raises(LeakageViolation, match=bad):
        build_target(target_id="T", values=v, reference_ts=REF_TS)


def test_chi_dung_split_train(fs):
    """🚫 test set la tai san danh gia RCT duy nhat (uplift that +0.37pp)."""
    with pytest.raises(ValueError, match="split"):
        build_target(target_id="T", values=_values(fs), reference_ts=REF_TS, split="test")


def test_reference_ts_phai_co_timezone(fs):
    with pytest.raises(ValueError, match="timezone"):
        build_target(target_id="T", values=_values(fs), reference_ts=datetime(2026, 8, 1))


# ===========================================================================
# §6 · canonical hash
# ===========================================================================
def test_float_repr_round_trip():
    for x in [0.0, -0.0, 1.0, 1e-15, 8.169600000000001, 1.4771212547196624]:
        assert float(canonical.float_repr(x)) == x


def test_payload_khong_phu_thuoc_thu_tu_dict(fs):
    a = _values(fs)
    b = dict(reversed(list(a.items())))
    cols = fs.columns
    assert canonical.canonical_feature_payload(a, cols) == canonical.canonical_feature_payload(b, cols)


def test_hai_hash_long_nhau(fs):
    """§6.1a — payload doi thi CA HAI doi; identity doi thi CHI target_hash doi."""
    v = _values(fs)
    t1 = build_target(target_id="T", values=v, reference_ts=REF_TS)
    t2 = build_target(
        target_id="T", values=v,
        reference_ts=datetime(2026, 9, 1, tzinfo=timezone.utc),   # doi identity
    )
    assert t1.feature_payload_hash == t2.feature_payload_hash     # payload giu nguyen
    assert t1.target_hash != t2.target_hash                       # identity doi

    v2 = dict(v)
    v2["f82"] = 2.0
    t3 = build_target(target_id="T", values=v2, reference_ts=REF_TS)
    assert t3.feature_payload_hash != t1.feature_payload_hash
    assert t3.target_hash != t1.target_hash


def test_tamper_evidence(fs):
    """§6.3 — sua target => FAIL CUNG."""
    t = build_target(target_id="T", values=_values(fs), reference_ts=REF_TS)
    t.verify()   # sach

    tampered = ReconstructionTarget(
        target_id=t.target_id,
        selected_feature_set_id=t.selected_feature_set_id,
        feature_version=t.feature_version,
        reference_ts=t.reference_ts,
        split=t.split,
        values={**t.values, "f82": 999.0},   # sua len
        target_hash=t.target_hash,           # giu hash cu
    )
    with pytest.raises(TamperDetected, match="KHONG duoc sua target"):
        tampered.verify()


def test_target_bat_bien(fs):
    t = build_target(target_id="T", values=_values(fs), reference_ts=REF_TS)
    with pytest.raises(Exception):
        t.values["f82"] = 5.0        # type: ignore[index]
    with pytest.raises(Exception):
        t.target_id = "khac"         # type: ignore[misc]


# ===========================================================================
# TEST-05 · moi version deu phai vao fingerprint
# ===========================================================================
def _fp(**over):
    base = dict(
        target_hash_="h",
        selected_feature_set_id="fs_2026_08_v1",
        feature_spec_version="1",
        constraint_model_version="cm1",
        encoding_version="e1",
        objective_version="o1",
        selection_policy_version="sp1",
        solver_version="s1",
        semantic_branch="H1",
        reference_ts=REF_TS,
        generation_seed=7,
    )
    base.update(over)
    return canonical.reproducibility_fingerprint(**base)


@pytest.mark.parametrize(
    "field,new",
    [
        ("objective_version", "o2"),
        ("selection_policy_version", "sp2"),
        ("constraint_model_version", "cm2"),
        ("encoding_version", "e2"),
        ("solver_version", "s2"),
        ("generation_seed", 8),
        ("semantic_branch", "H2"),
        ("target_hash_", "h2"),
    ],
)
def test_05_doi_version_phai_doi_fingerprint(field, new):
    assert _fp() != _fp(**{field: new})


def test_05_khong_doi_gi_thi_fingerprint_giu_nguyen():
    assert _fp() == _fp()


def test_08_semantic_status_khong_nam_trong_fingerprint():
    """TEST-08 (phan hash) — status la metadata NHAN THUC, khong duoc doi output.

    `reproducibility_fingerprint()` khong nhan `semantic_status` -> bat bien nay
    duoc thuc thi boi CHU KY HAM, khong phai bang kiem tra runtime.
    """
    import inspect

    sig = inspect.signature(canonical.reproducibility_fingerprint)
    assert "semantic_status" not in sig.parameters


# ===========================================================================
# I-6 · semantic_status <-> semantic_branch (INVARIANT 2)
# ===========================================================================
def _cfg(**over):
    base = dict(
        semantic_branch="H1",
        semantic_status="UNIDENTIFIED",
        constraint_model_version="cm1",
        encoding_version="e1",
        objective_version="o1",
        selection_policy_version="sp1",
        solver_version="s1",
        generation_seed=7,
    )
    base.update(over)
    return SolverConfig(**base)


@pytest.mark.parametrize("branch", ["H1", "H2", None])
def test_unidentified_cho_phep_moi_branch(branch):
    """UNIDENTIFIED + branch=H1 la HOP LE — kich ban, khong phai su that."""
    assert _cfg(semantic_branch=branch).semantic_branch == branch


@pytest.mark.parametrize(
    "status,bad_branch", [("H1_SUPPORTED", "H2"), ("H2_SUPPORTED", "H1"), ("H1_SUPPORTED", None)]
)
def test_status_supported_ep_dung_branch(status, bad_branch):
    with pytest.raises(ValueError, match="bat buoc"):
        _cfg(semantic_status=status, semantic_branch=bad_branch)


def test_solver_config_khong_co_behaviour_model():
    """I-5 — thuc thi boi kieu du lieu, khong phai quy uoc."""
    assert "behaviour_model" not in SolverConfig.__dataclass_fields__


# ===========================================================================
# §8 · encoding — bay alphabet dung chung
# ===========================================================================
@pytest.fixture(scope="module")
def registry():
    # f37 va f38 CO Y dung chung alphabet — tai hien bay §8.3
    shared = [0.0, 0.076721, 0.118435, 0.140481]
    rows37 = [{"f37": v} for v in shared]
    rows38 = [{"f38": v} for v in shared + [0.263929, 0.518233]]
    return encoding.EncodingRegistry(
        {
            "synthetic_attr_64": encoding.fit_value_encoding("synthetic_attr_64", ["f37"], rows37),
            "synthetic_attr_241": encoding.fit_value_encoding("synthetic_attr_241", ["f38"], rows38),
            "synthetic_segment_g2": encoding.OneHotEncoding(
                "synthetic_segment_g2", tuple(f"f{i}" for i in range(43, 53))
            ),
        }
    )


def test_encoding_bijection_toan_domain(registry):
    """§8.1 — kiem MOI level, khong sample."""
    for attr in registry.attrs:
        registry.assert_bijective(attr)


def test_encoding_round_trip_moi_gia_tri_quan_sat(registry):
    rows = [{"f37": v} for v in (0.0, 0.076721, 0.118435, 0.140481)]
    registry.assert_round_trip("synthetic_attr_64", rows)


def test_83_alphabet_dung_chung_khong_gay_mo_ho(registry):
    """§8.3 — cung gia tri 0.118435, HAI thuoc tinh, HAI level khac nhau."""
    v = 0.118435
    lvl64 = registry.decode("synthetic_attr_64", {"f37": v})
    lvl241 = registry.decode("synthetic_attr_241", {"f38": v})
    # Tra ve dung gia tri cu cho tung thuoc tinh — khong lan
    assert registry.encode("synthetic_attr_64", lvl64) == {"f37": v}
    assert registry.encode("synthetic_attr_241", lvl241) == {"f38": v}


def test_khong_co_api_decode_khong_khoa(registry):
    """Chu ky ham la noi thuc thi §8.3, khong phai comment."""
    import inspect

    assert "attr" in inspect.signature(registry.decode).parameters
    assert "attr" in inspect.signature(registry.encode).parameters


def test_gia_tri_la_thi_fail_cung_khong_fallback(registry):
    with pytest.raises(encoding.EncodingError, match="KHONG fallback"):
        registry.decode("synthetic_attr_64", {"f37": 0.99999})


def test_one_hot_giu_du_10_muc_cua_group(registry):
    """G-1 — chi dung 3/10 muc se lam vo bat bien sum(f43..f52)==1."""
    g2 = registry.get("synthetic_segment_g2")
    assert g2.levels == 10
    vec = registry.encode("synthetic_segment_g2", 7)
    assert sum(vec.values()) == 1.0
    assert registry.decode("synthetic_segment_g2", vec) == 7


def test_one_hot_vo_bat_bien_thi_bao_loi(registry):
    bad = {f"f{i}": 0.0 for i in range(43, 53)}
    bad["f43"] = bad["f44"] = 1.0
    with pytest.raises(encoding.EncodingError, match="one-hot invariant vo"):
        registry.decode("synthetic_segment_g2", bad)


def test_fit_khong_phu_thuoc_thu_tu_dong():
    """Level id gan theo thu tu SAP XEP, khong theo thu tu gap dau tien."""
    vals = [0.5, 0.1, 0.9, 0.3]
    a = encoding.fit_value_encoding("x", ["c"], [{"c": v} for v in vals])
    b = encoding.fit_value_encoding("x", ["c"], [{"c": v} for v in reversed(vals)])
    for lvl in range(a.levels):
        assert a.encode(lvl) == b.encode(lvl)

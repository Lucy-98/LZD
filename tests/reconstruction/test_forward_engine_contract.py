"""Forward feature engine — contract tinh tren SQL.

RECONSTRUCTION_SPEC.md §12 (tautology prevention) TA-1 · TA-2 · TA-3 · TA-6

Day la mat xich bien Gate A tu "hop dong" thanh "chay duoc":

    Track A  ->  E*  ->  [ 4 dbt model ]  ->  joined 55 F'  ->  so voi target

Neu mat xich nay doc duoc target hoac doc duoc metadata cua solver, thi
`F' == F` tro thanh so dau ra cua mot ham voi CHINH NO.

Test o day KHONG chay dbt (can Postgres/DuckDB + du lieu). Chung kiem tinh
chat CAU TRUC cua SQL — thu ma neu sai thi moi ket qua sau do deu vo nghia.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

DBT = Path(__file__).resolve().parents[2] / "dbt" / "models"
STG = DBT / "staging" / "stg_events_v2.sql"

FEATURE_MODELS = {
    "feat_cfs_counter": DBT / "marts" / "feat_cfs_counter.sql",
    "feat_cfs_recency": DBT / "marts" / "feat_cfs_recency.sql",
    "feat_cfs_categorical": DBT / "marts" / "feat_cfs_categorical.sql",
    "feat_passthrough": DBT / "marts" / "feat_passthrough.sql",
    "feat_cfs_reconstructed_selected": (
        DBT / "marts" / "feat_cfs_reconstructed_selected.sql"
    ),
}

#: Metadata noi bo cua solver. Feature engine THAY duoc chung => vong tron.
SOLVER_INTERNALS = ("gen_reason", "sub_index", "occurrence", "candidate_slot_id")


def _sql(p: Path) -> str:
    return p.read_text(encoding="utf-8")


def _strip_comments(s: str) -> str:
    """Bo comment de khong bat nham chinh loi canh bao trong doc."""
    return "\n".join(l.split("--")[0] for l in s.splitlines())


@pytest.fixture(scope="module")
def models() -> dict[str, str]:
    return {n: _sql(p) for n, p in FEATURE_MODELS.items()}


def test_moi_model_ton_tai():
    for name, p in FEATURE_MODELS.items():
        assert p.is_file(), f"thieu model {name}"
    assert STG.is_file()


def test_joined_mart_doc_du_bon_component_va_khong_doc_target(models):
    body = models["feat_cfs_reconstructed_selected"]
    for upstream in (
        "feat_cfs_counter", "feat_cfs_recency",
        "feat_cfs_categorical", "feat_passthrough",
    ):
        assert f"ref('{upstream}')" in body
    assert "reconstruction_target" not in _strip_comments(body)


# ===========================================================================
# TA-6 · staging PHAI drop metadata cua solver
# ===========================================================================
def test_ta6_staging_khong_cho_gen_reason_di_qua():
    """★ Bay lon nhat.

    Neu feature engine dem theo `gen_reason`, no dang dem nhung event ma SOLVER
    da noi la thuoc ve f5 => Gate A khong kiem gi ca.
    """
    body = _strip_comments(_sql(STG))
    for bad in SOLVER_INTERNALS:
        assert bad not in body, (
            f"stg_events_v2 cho `{bad}` di qua — feature engine se thay metadata "
            "cua solver va Gate A tro thanh vong tron"
        )


def test_ta6_staging_chi_phoi_bay_giao_dien_nghiep_vu():
    body = _strip_comments(_sql(STG))
    for need in ("event_id", "event_type", "event_ts", "observation_ts"):
        assert need in body, f"staging thieu cot nghiep vu {need}"


@pytest.mark.parametrize("name", list(FEATURE_MODELS))
def test_ta6_feature_model_khong_cham_solver_internals(models, name):
    body = _strip_comments(models[name])
    for bad in SOLVER_INTERNALS:
        assert bad not in body, f"{name} doc `{bad}` — do la noi bo cua solver"


# ===========================================================================
# TA-3 · KHONG duoc doc payload cua target
# ===========================================================================
@pytest.mark.parametrize("name", list(FEATURE_MODELS))
def test_ta3_khong_doc_bang_target(models, name):
    """Chi duoc doc `reconstruction_boundary` (target_id + reference_ts),
    KHONG duoc doc `reconstruction_target` (co payload 36 gia tri)."""
    body = _strip_comments(models[name])
    assert "reconstruction_target" not in body, (
        f"{name} doc bang target => co the 'doc dap an'. "
        "Chi duoc doc source `reconstruction_boundary`."
    )
    assert "payload" not in body


def test_ta3_boundary_view_khong_phoi_bay_payload():
    sql = (Path(__file__).resolve().parents[2] / "sql" / "postgres"
           / "02_biz_reconstruction.sql").read_text(encoding="utf-8")
    view = sql.split("CREATE OR REPLACE VIEW biz.v_reconstruction_boundary AS")[1].split(";")[0]
    assert "payload" not in view, "boundary view phoi bay payload => Gate A vo nghia"
    for need in ("target_id", "reference_ts"):
        assert need in view


# ===========================================================================
# TA-2 · KHONG duoc co nhanh reconstruction_mode
# ===========================================================================
@pytest.mark.parametrize("name", list(FEATURE_MODELS))
def test_ta2_khong_co_nhanh_reconstruction_mode(models, name):
    body = _strip_comments(models[name]).lower()
    for bad in ("reconstruction_mode", "is_reconstruction", "if_recon"):
        assert bad not in body, f"{name} co nhanh dac biet cho reconstruction"


# ===========================================================================
# Bien PIT — §12.1
# ===========================================================================
@pytest.mark.parametrize("name", ["feat_cfs_counter", "feat_cfs_recency"])
def test_bien_pit_nghiem_ngat(models, name):
    """`<` chu KHONG phai `<=`. Dung `<=` lech mot event o bien."""
    body = _strip_comments(models[name])
    assert re.search(r"event_ts\s*<\s*b\.reference_ts", body), (
        f"{name} phai dung `event_ts < reference_ts` NGHIEM NGAT"
    )
    assert not re.search(r"event_ts\s*<=\s*b\.reference_ts", body)


@pytest.mark.parametrize("name", ["feat_cfs_counter", "feat_cfs_recency"])
def test_availability_semantics(models, name):
    """Event xay ra truoc reference_ts nhung quan sat sau => chua ton tai."""
    body = _strip_comments(models[name])
    assert re.search(r"observation_ts\s*<=\s*b\.reference_ts", body), (
        f"{name} thieu rang buoc availability (observation_ts)"
    )


# ===========================================================================
# Regime encode — §7
# ===========================================================================
def test_regime_log10_lam_tron_6_chu_so(models):
    body = _strip_comments(models["feat_cfs_counter"])
    assert "round(log10(n18), 6)" in body
    assert "round(log10(n30), 6)" in body


def test_regime_ln_KHONG_duoc_lam_tron(models):
    """★ Lam tron regime LN se pha round-trip 1e-15."""
    body = _strip_comments(models["feat_cfs_counter"])
    assert "ln(n5)" in body and "ln(n11)" in body
    assert "round(ln(" not in body, "regime LN bi lam tron => pha dung sai 1e-15"


def test_f30_co_hai_semantic_scenario_tuong_minh(models):
    """H1 dem active day; H2 dem lop event rieng; branch den tu config."""
    body = _strip_comments(models["feat_cfs_counter"])
    assert "count(distinct cast(event_ts as date))" in body
    assert "event_type = 'EVT_F30'" in body
    assert "f30_semantic_branch" in models["feat_cfs_counter"]


def test_khong_coalesce_che_loi(models):
    """Mien do duoc la [1,N]. Thay 0 la KHONG KHOP THAT — phai de NULL cho
    Gate A bat, khong duoc coalesce ve 1."""
    body = _strip_comments(models["feat_cfs_counter"])
    assert "coalesce" not in body.lower(), (
        "coalesce trong counter model se che mat truong hop n=0 (khong khop that)"
    )


# ===========================================================================
# Group-level reconstruction — §5 G-1
# ===========================================================================
def test_g2_dung_du_10_muc_du_chi_chon_3_cot(models):
    """Chi dung 3/10 muc lam vo bat bien sum(f43..f52)=1."""
    body = _strip_comments(models["feat_cfs_categorical"])
    for c in [f"f{i}" for i in range(43, 53)]:
        assert f"'{c}'" in body, f"group G2 thieu muc {c} — one-hot invariant se vo"


def test_categorical_tra_theo_KHOA_THUOC_TINH(models):
    """§8.3 — f37/f38 dung chung alphabet; tra theo value se MAP MO."""
    body = _strip_comments(models["feat_cfs_categorical"])
    assert "e.attr_name = a.attr_name" in body, (
        "encoding map phai join theo attr_name, khong theo value"
    )


def test_categorical_co_assert_onehot_invariant(models):
    body = _strip_comments(models["feat_cfs_categorical"])
    for g in ("_g1_onehot_sum", "_g2_onehot_sum", "_g3_onehot_sum",
              "_g4_onehot_sum", "_g6_onehot_sum"):
        assert g in body


def test_f79_f82_tu_MOT_thuoc_tinh(models):
    """card(tuple) = card(moi cot) = 515 => MOT bien latent, KHONG phai bon."""
    body = models["feat_cfs_categorical"]
    assert "synthetic_category_515" in body
    assert "MOT bien latent" in body or "MOT thuoc tinh" in body


# ===========================================================================
# Pass-through — §12 Gate A-T3 rieng
# ===========================================================================
def test_passthrough_co_du_cot_t3(models):
    """Danh sach lay tu ARTIFACT, khong chep tay — chep tay la cach B5 tai dien."""
    from lzd_pipeline.reconstruction.feature_set import load_feature_set

    body = models["feat_passthrough"]
    cols = load_feature_set().tiers["T3"]
    assert len(cols) == 24
    for c in cols:
        assert f"'{c}'" in body, f"passthrough thieu {c}"


def test_passthrough_ghi_ro_KHONG_phai_feature_engineering(models):
    """Neu khong ghi ro, nguoi doc se tuong T3 cung duoc reconstruct."""
    body = models["feat_passthrough"]
    assert "KHONG PHAI FEATURE ENGINEERING" in body
    assert "Gate A-T3" in body


def test_passthrough_khong_gop_cot_trung(models):
    """f23/f25 va f9/f16 CHUA giai quyet o muc semantic => giu nguyen ven."""
    body = models["feat_passthrough"]
    for c in ("f23", "f25", "f9", "f16"):
        assert f"'{c}'" in body

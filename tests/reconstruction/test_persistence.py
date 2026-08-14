"""Persistence layer — §9 + sql/postgres/02_biz_reconstruction.sql.

Rang buoc trung tam:

    DB schema KHONG duoc lam thay doi semantics cua `Provenance` da pass test.

Nen bo test nay kiem BA thu:
    1. round-trip Provenance <-> row, khong mat mat
    2. CONSTRAINT PARITY — luat trong dataclass va CHECK trong SQL la MOT
    3. lineage P-1 / P-2 / P-5 tren DO THI
    + capability boundary: live.py khong reach duoc persistence
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from lzd_pipeline.reconstruction import capability, persistence
from lzd_pipeline.reconstruction.persistence import (
    LineageViolation,
    provenance_from_row,
    provenance_to_row,
)
from lzd_pipeline.reconstruction.state import Provenance

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
RECON = SRC_ROOT / "lzd_pipeline" / "reconstruction"
SQL_FILE = Path(__file__).resolve().parents[2] / "sql" / "postgres" / "02_biz_reconstruction.sql"
TS = datetime(2026, 8, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def sql() -> str:
    return SQL_FILE.read_text(encoding="utf-8")


def _prov(**over) -> Provenance:
    base = dict(
        source_type="RECONSTRUCTED",
        generation_run_id="run-1",
        root_generation_id="run-1",
        parent_target_id="T-1",
        created_at=TS,
    )
    base.update(over)
    return Provenance(**base)


# ===========================================================================
# 1) Round-trip
# ===========================================================================
def test_round_trip_khong_mat_mat():
    p = _prov(
        parent_run_id="run-0",
        parent_entity_id="C1",
        parent_feature_version="v1",
        ancestor_model_ids=("m1", "m2"),
    )
    assert provenance_from_row(provenance_to_row(p)) == p


def test_ancestor_model_ids_thanh_list_cho_postgres():
    """TEXT[] cua Postgres — tuple khong serialize truc tiep duoc."""
    row = provenance_to_row(_prov(ancestor_model_ids=("m1",)))
    assert isinstance(row["ancestor_model_ids"], list)
    # nhung doc lai phai ve tuple (bat bien)
    assert isinstance(provenance_from_row(row).ancestor_model_ids, tuple)


def test_row_co_dung_cot_nhu_sql(sql):
    row = provenance_to_row(_prov())
    assert set(row) == set(persistence.PROVENANCE_COLUMNS)
    # va moi cot phai that su ton tai trong DDL
    ddl = sql.split("CREATE TABLE IF NOT EXISTS biz.provenance")[1].split(");")[0]
    for col in persistence.PROVENANCE_COLUMNS:
        assert re.search(rf"\b{col}\b", ddl), f"cot {col} khong co trong biz.provenance"


def test_doc_lai_van_kiem_p3_p4():
    """Neu CHECK trong DB bi drop tay, vi pham phai LO luc doc — khong am tham."""
    bad = provenance_to_row(_prov())
    bad["parent_target_id"] = None          # RECONSTRUCTED ma khong co target
    with pytest.raises(ValueError, match="P-3"):
        provenance_from_row(bad)


# ===========================================================================
# 2) ★ CONSTRAINT PARITY — dataclass va SQL ma hoa MOT luat
# ===========================================================================
def test_parity_p3_co_mat_o_ca_hai_lop(sql):
    # dataclass
    with pytest.raises(ValueError, match="P-3"):
        Provenance(
            source_type="RECONSTRUCTED", generation_run_id="r", root_generation_id="r"
        )
    # SQL
    assert "ck_p3" in sql
    body = sql.split("CONSTRAINT ck_p3 CHECK (")[1].split(")")[0]
    assert "RECONSTRUCTED" in body and "parent_target_id IS NOT NULL" in body


def test_parity_p4_co_mat_o_ca_hai_lop(sql):
    with pytest.raises(ValueError, match="P-4"):
        Provenance(source_type="SYNTHETIC", generation_run_id="r", root_generation_id="r")
    assert "ck_p4" in sql
    body = sql.split("CONSTRAINT ck_p4 CHECK (")[1].split(");")[0]
    # 🚫 P-4 KHONG duoc siet thanh "phai co model id" — rule-based la hop le
    assert "ancestor_model_ids" in body
    assert "behaviour_policy_version IS NOT NULL" in body
    assert " OR " in body, "P-4 phai la ML HOAC rule, khong phai chi ML"


def test_parity_i6_status_branch(sql):
    """I-6 lap lai trong SQL — cung luat voi SolverConfig."""
    assert "ck_status_branch" in sql
    body = sql.split("CONSTRAINT ck_status_branch CHECK (")[1].split(");")[0]
    for token in ("UNIDENTIFIED", "H1_SUPPORTED", "H2_SUPPORTED"):
        assert token in body


def test_sql_chan_label_va_is_treat(sql):
    """I-1 / Gate E o tang DB."""
    assert "ck_no_label" in sql
    body = sql.split("CONSTRAINT ck_no_label CHECK (")[1].split(")")[0]
    assert "label" in body and "is_treat" in body


def test_sql_ep_dung_scope_cua_artifact(sql):
    """INVARIANT 1 o tang DB — va con so phai LAY TU ARTIFACT.

    Truoc day test nay hard-code 36, nen DB va `fs_*.yaml` co the troi khoi nhau
    ma khong ai biet: payload 55 cot se bi CHECK 36 tu choi ngay khi writer
    Postgres duoc viet. Doc tu artifact thi moi lan doi scope, test nay ep SQL
    phai doi theo.
    """
    from lzd_pipeline.reconstruction.feature_set import load_feature_set

    n = load_feature_set().expected_column_count
    assert f"ck_scope_{n}" in sql
    body = sql.split(f"CONSTRAINT ck_scope_{n} CHECK (")[1].split(")")[0]
    assert f"= {n}" in body + f"= {n}"


def test_sql_passthrough_dung_du_cot_t3(sql):
    """`feat_passthrough.sql` select tung cot T3 theo ten — thieu mot cot trong
    DDL la loi runtime o production, khong phai loi lint."""
    import re

    from lzd_pipeline.reconstruction.feature_set import load_feature_set

    block = sql.split("CREATE TABLE IF NOT EXISTS biz.passthrough_source (")[1].split(");")[0]
    assert set(re.findall(r"\b(f\d+)\s+DOUBLE", block)) == set(load_feature_set().tiers["T3"])


def test_target_la_immutable_o_tang_db(sql):
    """§10 — trigger chan UPDATE/DELETE, khong chi la loi hua trong doc."""
    assert "forbid_target_mutation" in sql
    assert "BEFORE UPDATE OR DELETE ON biz.reconstruction_target" in sql


def test_customer_state_khong_co_fk_toi_target(sql):
    """★ INVARIANT 4 o tang schema.

    FK se bien `source_target_id` thanh lookup key => Track B join duoc sang
    target => nhin trom 36 feature.
    """
    block = sql.split("CREATE TABLE IF NOT EXISTS biz.customer_state")[1].split(");")[0]
    assert "source_target_id" in block
    assert "REFERENCES biz.reconstruction_target" not in block


def test_customer_state_khong_co_cot_feature(sql):
    block = sql.split("CREATE TABLE IF NOT EXISTS biz.customer_state")[1].split(");")[0]
    for bad in ("payload", "target_hash", "feature_payload_hash", "selected_feature_set_id"):
        assert bad not in block, f"customer_state khong duoc co cot {bad}"


def test_gate_b_o_tang_db(sql):
    block = sql.split("CREATE TABLE IF NOT EXISTS biz.customer_state")[1].split(");")[0]
    assert "ck_last_event_before" in block
    assert "last_event_ts < as_of_ts" in block


def test_objective_summary_bat_buoc_khi_solved(sql):
    """§3.2 — status=SOLVED ma khong co objective => khong kiem duoc solver."""
    assert "ck_objective_required" in sql


def test_fingerprint_co_du_moi_version_trong_ddl(sql):
    """§11 — thieu mot version la Gate G mat hieu luc."""
    block = sql.split("CREATE TABLE IF NOT EXISTS biz.generation_run")[1].split(");")[0]
    for col in (
        "selected_feature_set_id", "feature_spec_version", "constraint_model_version",
        "encoding_version", "objective_version", "selection_policy_version",
        "solver_version", "generation_seed", "reference_ts",
    ):
        assert col in block, f"fingerprint thieu {col}"


# ===========================================================================
# 3) Lineage — P-1 / P-2 / P-5
# ===========================================================================
def test_p1_bat_duoc_chu_trinh():
    with pytest.raises(LineageViolation, match="CHU TRINH"):
        persistence.assert_no_cycle({"a": "b", "b": "c", "c": "a"})


def test_p1_chap_nhan_cay_hop_le():
    persistence.assert_no_cycle({"root": None, "a": "root", "b": "a", "c": "root"})


def test_p1_bat_duoc_tu_tro_chinh_minh():
    with pytest.raises(LineageViolation):
        persistence.assert_no_cycle({"a": "a"})


def _row(run, parent, root, src="REAL", models=()):
    return {
        "generation_run_id": run, "parent_run_id": parent,
        "root_generation_id": root, "source_type": src,
        "ancestor_model_ids": list(models),
    }


def test_p2_bat_duoc_root_khai_sai():
    rows = [_row("r0", None, "r0"), _row("r1", "r0", "KHAC")]
    with pytest.raises(LineageViolation, match="P-2"):
        persistence.assert_single_root(rows)


def test_p2_chap_nhan_cay_dung():
    persistence.assert_single_root([_row("r0", None, "r0"), _row("r1", "r0", "r0")])


def test_p5_lineage_thuan_real_du_dieu_kien():
    rows = [_row("r0", None, "r0"), _row("r1", "r0", "r0")]
    assert persistence.real_lineage_roots(rows) == frozenset({"r0"})
    persistence.assert_production_train_eligible(rows)


def test_p5_bat_duoc_to_tien_synthetic_qua_NHIEU_DOI():
    """★ Day la ly do phai kiem tren DO THI, khong tren tung dong.

    r2 la REAL — kiem muc dong se cho no qua. Nhung to tien r0 la SYNTHETIC.
    """
    rows = [
        _row("r0", None, "r0", src="SYNTHETIC", models=("m1",)),
        _row("r1", "r0", "r0", src="REAL"),
        _row("r2", "r1", "r0", src="REAL"),
    ]
    assert all(r["source_type"] == "REAL" for r in rows[1:])   # muc dong: "sach"
    assert persistence.real_lineage_roots(rows) == frozenset()  # do thi: BAN
    with pytest.raises(LineageViolation, match="to tien khong phai REAL"):
        persistence.assert_production_train_eligible(rows)


def test_p5_bat_duoc_ancestor_model_ids_du_source_la_real():
    rows = [_row("r0", None, "r0", src="REAL", models=("m1",))]
    with pytest.raises(LineageViolation):
        persistence.assert_production_train_eligible(rows)


def test_sql_co_view_tuong_duong_p1_va_p5(sql):
    assert "v_lineage_cycles" in sql          # P-1
    assert "v_real_lineage_roots" in sql      # P-5
    body = sql.split("CREATE OR REPLACE VIEW biz.v_real_lineage_roots AS")[1].split(";")[0]
    assert "bool_and" in body, "P-5 phai kiem TOAN BO cay, khong phai tung dong"


# ===========================================================================
# 4) Capability boundary — persistence nam PHIA TRACK A
# ===========================================================================
def test_live_khong_reach_duoc_persistence():
    """Neu Track B cham duoc persistence, no query duoc biz.reconstruction_target."""
    capability.assert_cannot_reach(
        entry=RECON / "live.py",
        forbidden=("lzd_pipeline.reconstruction.persistence",),
        src_root=SRC_ROOT,
        why="persistence co the doc biz.reconstruction_target => INVARIANT 4.",
    )


def test_persistence_khong_can_reach_target():
    """Persistence chi map Provenance — khong can biet gi ve target contents."""
    capability.assert_cannot_reach(
        entry=RECON / "persistence.py",
        forbidden=(
            "lzd_pipeline.reconstruction.target",
            "lzd_pipeline.reconstruction.canonical",
        ),
        src_root=SRC_ROOT,
    )

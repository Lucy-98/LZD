"""TEST-10 · Track B khong the thay ReconstructionTarget.

    10a  API boundary        -> chu ky ham + cau truc dataclass
    10b  CAPABILITY boundary -> do thi import, BAC CAU

10b la tang quan trong hon: chu ky sach van co the bi ph� neu Track B import
duoc target repository roi giai `source_target_id`.
"""
from __future__ import annotations

import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from lzd_pipeline.reconstruction import capability
from lzd_pipeline.reconstruction.state import (
    CustomerState,
    HandoffRefused,
    Provenance,
    assert_state_has_no_target_channel,
)

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
RECON = SRC_ROOT / "lzd_pipeline" / "reconstruction"
TS = datetime(2026, 8, 1, tzinfo=timezone.utc)

#: Track B khong duoc reachable toi nhung module nay (INVARIANT 4)
TRACK_B_FORBIDDEN = (
    "lzd_pipeline.reconstruction.target",
    "lzd_pipeline.reconstruction.canonical",
    "lzd_pipeline.reconstruction.feature_set",
)


def _prov(**over):
    base = dict(
        source_type="RECONSTRUCTED",
        generation_run_id="run-1",
        root_generation_id="run-1",
        parent_target_id="T-1",
    )
    base.update(over)
    return Provenance(**base)


def _state(**over):
    base = dict(
        customer_id="C1",
        as_of_ts=TS,
        source_target_id="T-1",
        attributes={"synthetic_category_515": 12},
        counters={"n30": 5, "n5": 1},
        semantic_branch="H1",
        semantic_status="UNIDENTIFIED",
        provenance=_prov(),
    )
    base.update(over)
    return CustomerState(**base)


# ===========================================================================
# TEST-10a · API boundary
# ===========================================================================
def test_10a_state_khong_co_kenh_suy_nguoc_ra_target():
    assert_state_has_no_target_channel()


@pytest.mark.parametrize(
    "bad", ["values", "target_hash", "feature_payload_hash", "selected_feature_set_id"]
)
def test_10a_tung_truong_bi_cam_deu_vang_mat(bad):
    assert bad not in CustomerState.__dataclass_fields__


def test_10a_state_bat_bien():
    s = _state()
    with pytest.raises(Exception):
        s.counters["n30"] = 99      # type: ignore[index]
    with pytest.raises(Exception):
        s.customer_id = "khac"      # type: ignore[misc]


def test_10a_last_event_ts_phai_truoc_as_of_ts():
    """Gate B §12.1 — moi event Track A nam TRUOC reference_ts."""
    with pytest.raises(ValueError, match="TRUOC"):
        _state(last_event_ts=TS + timedelta(seconds=1))
    _state(last_event_ts=TS - timedelta(seconds=1))   # hop le


# ===========================================================================
# TEST-10b · CAPABILITY boundary — do thi import
# ===========================================================================
def test_10b_state_module_khong_reachable_toi_target():
    """`state.py` la mat tien cua Track B. No khong duoc cham target/canonical."""
    capability.assert_cannot_reach(
        entry=RECON / "state.py",
        forbidden=TRACK_B_FORBIDDEN,
        src_root=SRC_ROOT,
        why="state.py la dau vao cua Track B — phai mu voi target.",
    )


def test_10b_checker_bat_duoc_import_TRUC_TIEP(tmp_path):
    pkg = tmp_path / "lzd_pipeline" / "reconstruction"
    pkg.mkdir(parents=True)
    (tmp_path / "lzd_pipeline" / "__init__.py").write_text("")
    (pkg / "__init__.py").write_text("")
    (pkg / "target.py").write_text("X = 1\n")
    (pkg / "trackb.py").write_text(
        "from lzd_pipeline.reconstruction.target import X\n"
    )
    with pytest.raises(AssertionError, match="CAPABILITY BOUNDARY VI PHAM"):
        capability.assert_cannot_reach(
            entry=pkg / "trackb.py",
            forbidden=("lzd_pipeline.reconstruction.target",),
            src_root=tmp_path,
        )


def test_10b_checker_bat_duoc_import_BAC_CAU_qua_3_lop(tmp_path):
    """Day la ly do can do thi thay vi doc chu ky ham.

        trackb -> helper -> deep -> target
    """
    pkg = tmp_path / "lzd_pipeline" / "reconstruction"
    pkg.mkdir(parents=True)
    (tmp_path / "lzd_pipeline" / "__init__.py").write_text("")
    (pkg / "__init__.py").write_text("")
    (pkg / "target.py").write_text("SECRET = 36\n")
    (pkg / "deep.py").write_text(
        "from lzd_pipeline.reconstruction.target import SECRET\n"
    )
    (pkg / "helper.py").write_text("from lzd_pipeline.reconstruction import deep\n")
    (pkg / "trackb.py").write_text("from lzd_pipeline.reconstruction import helper\n")

    with pytest.raises(AssertionError) as exc:
        capability.assert_cannot_reach(
            entry=pkg / "trackb.py",
            forbidden=("lzd_pipeline.reconstruction.target",),
            src_root=tmp_path,
        )
    # Bao loi phai chi ra DUONG DI, khong chi noi "vi pham"
    assert "duong di" in str(exc.value)


def test_10b_checker_khong_bao_dong_gia(tmp_path):
    pkg = tmp_path / "lzd_pipeline" / "reconstruction"
    pkg.mkdir(parents=True)
    (tmp_path / "lzd_pipeline" / "__init__.py").write_text("")
    (pkg / "__init__.py").write_text("")
    (pkg / "target.py").write_text("X = 1\n")
    (pkg / "trackb.py").write_text(
        textwrap.dedent(
            """
            import math
            from datetime import datetime
            """
        )
    )
    capability.assert_cannot_reach(
        entry=pkg / "trackb.py",
        forbidden=("lzd_pipeline.reconstruction.target",),
        src_root=tmp_path,
    )


def test_10b_import_tuong_doi_cung_bi_bat(tmp_path):
    """`from .target import X` — de lot neu chi grep chuoi tuyet doi."""
    pkg = tmp_path / "lzd_pipeline" / "reconstruction"
    pkg.mkdir(parents=True)
    (tmp_path / "lzd_pipeline" / "__init__.py").write_text("")
    (pkg / "__init__.py").write_text("")
    (pkg / "target.py").write_text("X = 1\n")
    (pkg / "trackb.py").write_text("from .target import X\n")

    with pytest.raises(AssertionError, match="CAPABILITY BOUNDARY"):
        capability.assert_cannot_reach(
            entry=pkg / "trackb.py",
            forbidden=("lzd_pipeline.reconstruction.target",),
            src_root=tmp_path,
        )


def test_10b_import_tuong_doi_trong_init_phan_giai_khac_module(tmp_path):
    """`__init__.py` phan giai `from .x` KHAC voi module thuong.

        module  a.b.c        `from .target` -> a.b.target
        package a.b/__init__ `from .target` -> a.b.target   (GIU 'b')

    Lech mot cap o day => checker bo lot vi pham that.
    """
    pkg = tmp_path / "lzd_pipeline" / "reconstruction"
    pkg.mkdir(parents=True)
    (tmp_path / "lzd_pipeline" / "__init__.py").write_text("")
    (pkg / "target.py").write_text("X = 1\n")
    (pkg / "__init__.py").write_text("from .target import X\n")   # vi pham o INIT

    with pytest.raises(AssertionError, match="CAPABILITY BOUNDARY"):
        capability.assert_cannot_reach(
            entry=pkg / "__init__.py",
            forbidden=("lzd_pipeline.reconstruction.target",),
            src_root=tmp_path,
        )


def test_10b_import_tuong_doi_hai_cap(tmp_path):
    """`from ..other import Y` — level=2."""
    root = tmp_path / "lzd_pipeline"
    pkg = root / "reconstruction"
    pkg.mkdir(parents=True)
    (root / "__init__.py").write_text("")
    (root / "secret.py").write_text("Y = 1\n")
    (pkg / "__init__.py").write_text("")
    (pkg / "trackb.py").write_text("from ..secret import Y\n")

    with pytest.raises(AssertionError, match="lzd_pipeline.secret"):
        capability.assert_cannot_reach(
            entry=pkg / "trackb.py",
            forbidden=("lzd_pipeline.secret",),
            src_root=tmp_path,
        )


# ===========================================================================
# §9 · provenance invariant
# ===========================================================================
def test_p3_reconstructed_bat_buoc_co_parent_target_id():
    with pytest.raises(ValueError, match="P-3"):
        Provenance(
            source_type="RECONSTRUCTED", generation_run_id="r", root_generation_id="r"
        )


def test_p4_synthetic_chap_nhan_rule_based_khong_can_model_id():
    """Track B co the la rule-based simulator -> ancestor_model_ids rong LA CHINH DANG."""
    Provenance(
        source_type="SYNTHETIC",
        generation_run_id="r",
        root_generation_id="r0",
        behaviour_policy_version="policy_v1",
    )
    Provenance(
        source_type="SYNTHETIC",
        generation_run_id="r",
        root_generation_id="r0",
        ancestor_model_ids=("m1",),
    )
    with pytest.raises(ValueError, match="P-4"):
        Provenance(source_type="SYNTHETIC", generation_run_id="r", root_generation_id="r0")


def test_handoff_refused_ton_tai_de_engine_dung():
    """§3.4a — QUARANTINED khong duoc vao Track B. Engine se raise cai nay."""
    assert issubclass(HandoffRefused, ValueError)

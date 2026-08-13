"""ReconstructionTarget + SolverConfig — RECONSTRUCTION_SPEC.md §3.

Bat bien duoc thuc thi bang KIEU DU LIEU, khong phai bang quy uoc:

    I-1  khong co field label/is_treat        -> khong dinh nghia field + chan key
    I-2  DUNG 36 khoa khop feature set        -> validate luc dung
    I-3  split == "train"                     -> validate luc dung
    I-4  target BAT BIEN                      -> frozen=True
    I-5  solver khong nhan behaviour_model    -> chu ky ham (engine)
    I-6  status <-> branch                    -> validate luc dung
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Literal, Mapping

from lzd_pipeline.reconstruction import canonical
from lzd_pipeline.reconstruction.feature_set import SelectedFeatureSet, load_feature_set

SemanticBranchName = Literal["H1", "H2"]
SemanticStatus = Literal["H1_SUPPORTED", "H2_SUPPORTED", "UNIDENTIFIED"]

#: 🚫 I-1 — nhung khoa nay KHONG BAO GIO duoc phep di vao solver.
FORBIDDEN_KEYS = frozenset({"label", "is_treat"})


class ScopeViolation(ValueError):
    """INVARIANT 1 — ai do co mo scope ra ngoai 36 cot."""


class LeakageViolation(ValueError):
    """I-1 / Gate E — label hoac is_treat cham vao solver."""


class TamperDetected(ValueError):
    """§6.3 — target_hash khong khop => co nguoi sua target."""


def validate_scope(values: Mapping[str, float], fs: SelectedFeatureSet) -> None:
    """I-1 + I-2 + INVARIANT 1 — kiem TRUOC khi lam bat cu viec gi khac.

    Tach ra ham rieng vi ca `__post_init__` lan `build_target` deu phai goi.
    Neu `build_target` hash truoc roi moi validate, loi 'thieu cot' tu tang
    hashing se CHE MAT `ScopeViolation` — thong bao sai cho nguoi doc.
    """
    leaked = FORBIDDEN_KEYS & set(values)
    if leaked:
        raise LeakageViolation(f"label/is_treat KHONG duoc di vao solver: {sorted(leaked)}")

    got, want = set(values), fs.column_set
    if got != want:
        extra, missing = sorted(got - want), sorted(want - got)
        raise ScopeViolation(
            f"values phai co DUNG {len(want)} cot cua {fs.id}. "
            f"thua={extra[:8]}{'...' if len(extra) > 8 else ''} "
            f"thieu={missing[:8]}{'...' if len(missing) > 8 else ''}"
        )


@dataclass(frozen=True)
class ReconstructionTarget:
    """Target BAT BIEN. Solver khong bao gio duoc sua no.

    `values` chua DUNG 36 cot cua `selected_feature_set_id` — khong hon.
    Neu goi dung 83 cot, dataclass nay TU CHOI dung (INVARIANT 1, TEST-09).
    """

    target_id: str
    selected_feature_set_id: str
    feature_version: str
    reference_ts: datetime
    split: Literal["train"]
    values: Mapping[str, float]
    target_hash: str

    def __post_init__(self) -> None:
        fs = load_feature_set()

        if self.selected_feature_set_id != fs.id:
            raise ScopeViolation(
                f"feature set khong khop: target noi {self.selected_feature_set_id!r}, "
                f"artifact la {fs.id!r}"
            )

        # I-1 + I-2 + INVARIANT 1 — chan truoc moi thu khac
        validate_scope(self.values, fs)

        # I-3 — 🚫 test set la tai san danh gia RCT duy nhat
        if self.split != fs.split_allowed:
            raise ValueError(
                f"chi duoc dung split={fs.split_allowed!r}, nhan duoc {self.split!r}"
            )

        # reference_ts phai tuong minh (§9.3 PIPELINE_ARCHITECTURE) — khong now()
        if self.reference_ts.tzinfo is None:
            raise ValueError("reference_ts phai co timezone (tranh mo ho khi replay)")

        # Dong bang mapping de khong ai sua duoc qua tham chieu
        object.__setattr__(self, "values", MappingProxyType(dict(self.values)))

    # -- §6.3 tamper-evidence ----------------------------------------------
    def expected_hash(self, fs: SelectedFeatureSet | None = None) -> str:
        fs = fs or load_feature_set()
        return canonical.target_hash(
            values=self.values,
            columns=fs.columns,
            selected_feature_set_id=self.selected_feature_set_id,
            feature_version=self.feature_version,
            reference_ts=self.reference_ts,
        )

    def verify(self, fs: SelectedFeatureSet | None = None) -> None:
        """Goi MOI LAN doc target. Lech => FAIL CUNG, khong sua, khong bo qua."""
        actual = self.expected_hash(fs)
        if actual != self.target_hash:
            raise TamperDetected(
                f"target {self.target_id}: hash khong khop.\n"
                f"  luu   = {self.target_hash}\n"
                f"  tinh  = {actual}\n"
                "=> co nguoi sua target. KHONG duoc sua target de validation pass."
            )

    @property
    def feature_payload_hash(self) -> str:
        return canonical.feature_payload_hash(self.values, load_feature_set().columns)


def build_target(
    *,
    target_id: str,
    values: Mapping[str, float],
    reference_ts: datetime,
    feature_version: str = "v1",
    split: str = "train",
    fs: SelectedFeatureSet | None = None,
) -> ReconstructionTarget:
    """Dung target va tinh `target_hash` — dung cho fixture va cho ETL.

    Chi dung khi TAO target lan dau. Khi DOC lai target da luu, phai dung
    `ReconstructionTarget(...)` truc tiep roi goi `.verify()` — neu khong,
    tamper-evidence tro thanh vo nghia (hash duoc tinh lai tu chinh du lieu
    da bi sua).
    """
    fs = fs or load_feature_set()
    # Validate TRUOC khi hash — neu khong, loi tang hashing se che mat ScopeViolation
    validate_scope(values, fs)
    h = canonical.target_hash(
        values=values,
        columns=fs.columns,
        selected_feature_set_id=fs.id,
        feature_version=feature_version,
        reference_ts=reference_ts,
    )
    return ReconstructionTarget(
        target_id=target_id,
        selected_feature_set_id=fs.id,
        feature_version=feature_version,
        reference_ts=reference_ts,
        split=split,  # type: ignore[arg-type]
        values=values,
        target_hash=h,
    )


@dataclass(frozen=True)
class SolverConfig:
    """Cau hinh solver.

    `semantic_branch` = KICH BAN VAN HANH dang chay.
    `semantic_status` = TRANG THAI NHAN THUC.

    🚫 Hai truong KHAC LOAI. `branch == "H1"` KHONG BAO GIO co nghia
       "f30 la active_days" (INVARIANT 2).
    """

    semantic_branch: SemanticBranchName | None
    semantic_status: SemanticStatus
    constraint_model_version: str
    encoding_version: str
    objective_version: str
    selection_policy_version: str
    solver_version: str
    generation_seed: int
    max_repair_rounds: int = 8

    # KHONG co field behaviour_model — I-5 duoc thuc thi boi chinh kieu du lieu

    def __post_init__(self) -> None:
        if self.semantic_branch not in ("H1", "H2", None):
            raise ValueError(f"semantic_branch khong hop le: {self.semantic_branch!r}")
        # I-6
        required = {"H1_SUPPORTED": "H1", "H2_SUPPORTED": "H2"}
        if self.semantic_status in required:
            want = required[self.semantic_status]
            if self.semantic_branch != want:
                raise ValueError(
                    f"semantic_status={self.semantic_status} bat buoc "
                    f"semantic_branch={want}, nhan duoc {self.semantic_branch!r}"
                )
        elif self.semantic_status != "UNIDENTIFIED":
            raise ValueError(f"semantic_status khong hop le: {self.semantic_status!r}")
        # UNIDENTIFIED: branch ∈ {H1, H2, None} — deu hop le

        if self.max_repair_rounds < 0:
            raise ValueError("max_repair_rounds phai >= 0")

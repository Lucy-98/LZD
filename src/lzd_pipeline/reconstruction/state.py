"""CustomerState — thu DUY NHAT di qua ranh gioi Track A -> Track B.

RECONSTRUCTION_SPEC.md §3.4.

    Track A  CO QUYEN biet:       ReconstructionTarget
    Track B  KHONG CO QUYEN biet: ReconstructionTarget

🚫 Module nay CO Y khong import `reconstruction.target` va `reconstruction.canonical`.
   Do la mot phan cua capability boundary (INVARIANT 4) — neu import, Track B se
   reachable toi chung qua do thi import va TEST-10b se do.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Literal, Mapping

SemanticBranchName = Literal["H1", "H2"]
SemanticStatus = Literal["H1_SUPPORTED", "H2_SUPPORTED", "UNIDENTIFIED"]

#: Truong nao co mat o day => Track B suy nguoc ra target duoc => CAM (§3.4).
FORBIDDEN_STATE_FIELDS = frozenset(
    {"values", "target_hash", "feature_payload_hash", "selected_feature_set_id"}
)


class HandoffRefused(ValueError):
    """§3.4a — chi ban giao sau khi Gate A pass."""


@dataclass(frozen=True)
class Provenance:
    """§9.1 — lineage day du, chong contamination xuyen doi."""

    source_type: Literal["REAL", "RECONSTRUCTED", "SYNTHETIC"]
    generation_run_id: str
    root_generation_id: str
    parent_run_id: str | None = None
    parent_target_id: str | None = None
    parent_entity_id: str | None = None
    parent_feature_version: str | None = None
    ancestor_model_ids: tuple[str, ...] = ()
    behaviour_policy_version: str | None = None
    created_at: datetime | None = None

    def __post_init__(self) -> None:
        # P-3
        if self.source_type == "RECONSTRUCTED" and self.parent_target_id is None:
            raise ValueError("source_type=RECONSTRUCTED bat buoc co parent_target_id (P-3)")
        # P-4 — co che sinh phai truy vet duoc; ML hoac rule deu hop le
        if self.source_type == "SYNTHETIC" and not (
            self.ancestor_model_ids or self.behaviour_policy_version
        ):
            raise ValueError(
                "source_type=SYNTHETIC bat buoc co ancestor_model_ids HOAC "
                "behaviour_policy_version (P-4)"
            )


@dataclass(frozen=True)
class CustomerState:
    """Trang thai T0 DA DUOC Track A xac nhan. Dau vao DUY NHAT cua Track B.

    🚫 KHONG co field nao cho phep suy nguoc ra target — xem FORBIDDEN_STATE_FIELDS.

    `source_target_id` la OPAQUE LINEAGE IDENTIFIER (§3.4-1):
        ✅ luu de trace
        🚫 dung de DOC noi dung target
    Rang buoc thu hai khong the thuc thi bang kieu du lieu — no duoc thuc thi
    bang do thi import (INVARIANT 4 / TEST-10b).
    """

    customer_id: str
    as_of_ts: datetime
    source_target_id: str
    attributes: Mapping[str, int]      # 7 source attribute -> level_id
    counters: Mapping[str, int]        # n5, n11, n18, n30 ... da giai ma
    semantic_branch: SemanticBranchName | None
    semantic_status: SemanticStatus
    provenance: Provenance
    last_event_ts: datetime | None = None

    def __post_init__(self) -> None:
        if self.as_of_ts.tzinfo is None:
            raise ValueError("as_of_ts phai co timezone")
        if self.last_event_ts is not None and self.last_event_ts >= self.as_of_ts:
            raise ValueError(
                "last_event_ts phai < as_of_ts — moi event Track A nam TRUOC "
                "reference_ts (Gate B, §12.1)"
            )
        object.__setattr__(self, "attributes", MappingProxyType(dict(self.attributes)))
        object.__setattr__(self, "counters", MappingProxyType(dict(self.counters)))


def assert_state_has_no_target_channel(cls: type = CustomerState) -> None:
    """TEST-10a — kiem cau truc, khong kiem gia tri."""
    leaked = FORBIDDEN_STATE_FIELDS & set(cls.__dataclass_fields__)
    if leaked:
        raise AssertionError(
            f"{cls.__name__} co truong cho phep suy nguoc ra target: {sorted(leaked)}"
        )

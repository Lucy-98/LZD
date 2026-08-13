"""Ban giao Track A -> Track B.

RECONSTRUCTION_SPEC.md §3.4a.

    ReconstructionResult (SOLVED | REPAIRED)
            + Gate A / A-T3 / B / C PASS
                    |
                    v
            CustomerState(T0)   <- DA XAC NHAN
                    |
                    v
            live_generator(...)

    QUARANTINED  ->  HandoffRefused  ->  STOP

🚫 Module nay nam PHIA TRACK A. No duoc phep biet target/engine.
   `live.py` (Track B) TUYET DOI khong duoc reach toi day — neu khong,
   `source_target_id` tro thanh lookup key va INVARIANT 4 vo.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from lzd_pipeline.reconstruction.engine import SolveOutcome
from lzd_pipeline.reconstruction.semantics import (
    REASON_F5,
    REASON_F11,
    REASON_F18,
    REASON_F30,
    DecodedTarget,
)
from lzd_pipeline.reconstruction.state import (
    CustomerState,
    HandoffRefused,
    Provenance,
    SemanticBranchName,
    SemanticStatus,
)

#: Chi hai trang thai nay duoc ban giao (§3.4a).
HANDOFF_ALLOWED = frozenset({"SOLVED", "REPAIRED"})


@dataclass(frozen=True)
class GateReport:
    """Ket qua gate cua Track A.

    Gate A/A-T3 can FEATURE ENGINE THAT (dbt) — prototype khong chay duoc no,
    nen ket qua phai duoc TRUYEN VAO tuong minh. KHONG mac dinh True: mac dinh
    True se bien ban giao thanh khong dieu kien, dung thu §3.4a cam.
    """

    gate_a: bool
    gate_a_t3: bool
    gate_b: bool
    gate_c: bool

    @property
    def all_passed(self) -> bool:
        return self.gate_a and self.gate_a_t3 and self.gate_b and self.gate_c

    def failed(self) -> tuple[str, ...]:
        return tuple(
            name
            for name, ok in (
                ("A", self.gate_a), ("A-T3", self.gate_a_t3),
                ("B", self.gate_b), ("C", self.gate_c),
            )
            if not ok
        )


def to_customer_state(
    outcome: SolveOutcome,
    decoded: DecodedTarget,
    gates: GateReport,
    *,
    customer_id: str,
    target_id: str,
    reference_ts: datetime,
    attributes: dict[str, int],
    semantic_branch: SemanticBranchName | None,
    semantic_status: SemanticStatus,
    provenance: Provenance,
) -> CustomerState:
    """Dung `CustomerState(T0)` — thu DUY NHAT di qua ranh gioi.

    🚫 KHONG dua gia tri feature vao state. `CustomerState` khong co truong nao
       nhan chung (§3.4, FORBIDDEN_STATE_FIELDS) — day la rang buoc CAU TRUC.
    """
    if outcome.status not in HANDOFF_ALLOWED:
        raise HandoffRefused(
            f"khong ban giao duoc: status={outcome.status} "
            f"(reason={outcome.reason_code}). "
            "T0 chua duoc xac nhan => moi event tuong lai deu bam vao trang thai sai."
        )
    if not gates.all_passed:
        raise HandoffRefused(
            f"Gate chua pass: {gates.failed()}. §3.4a bat buoc Gate A/A-T3/B/C PASS "
            "truoc khi ban giao."
        )

    last = max((e.event_ts for e in outcome.events), default=None)

    return CustomerState(
        customer_id=customer_id,
        as_of_ts=reference_ts,
        source_target_id=target_id,      # OPAQUE lineage id, KHONG phai lookup key
        attributes=attributes,
        counters={
            REASON_F5: decoded.n5,
            REASON_F11: decoded.n11,
            REASON_F18: decoded.n18,
            REASON_F30: decoded.n30,
        },
        semantic_branch=semantic_branch,
        semantic_status=semantic_status,
        provenance=provenance,
        last_event_ts=last,
    )

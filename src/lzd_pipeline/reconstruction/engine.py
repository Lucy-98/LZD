"""ReconstructionEngine — P1 -> P2 -> P3 -> P3b -> P4.

RECONSTRUCTION_SPEC.md §4.4a.

    P1  FEASIBILITY   hard constraints + EXACT feature equality
    P2  OPTIMIZATION  lexicographic argmin  => OPTIMAL POOL
    P3  SELECTION     seeded PRNG chon MOT phan tu TRONG pool
    P3b MATERIALIZE   candidate_slot_id -> occurrence -> event_id
    P4  ORDERING      canonical sort — CHI sap thu tu

★ INVARIANT 3 (§4.4a-1): seed CHI tac dong SAU khi da co optimal pool.

    ✅ P1 -> P2 -> optimal pool -> P3 (seed) -> P3b -> P4
    🚫 seed -> random candidate -> check objective

Thu tu nay duoc thuc thi boi CHINH CAU TRUC HAM: `select_from_pool()` nhan
`pool` da la argmin, no khong co duong nao cham vao tap feasible day du.
"""
from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Iterable, Literal, Sequence

from lzd_pipeline.reconstruction.candidate import Candidate
from lzd_pipeline.reconstruction.semantics import DecodedTarget, SemanticBranch

TRACK_A = "A"

#: Namespace co dinh cho uuid5 — doi no se doi MOI event_id.
EVENT_NS = uuid.UUID("6f9b1c62-3f52-5a41-9d7e-000000000001")

#: ★ Anh xa gen_reason -> event_type CFS witness.
#
#  Day la RANH GIOI quan trong: `gen_reason` la noi bo cua solver va bi DROP o
#  `stg_events_v2`. Feature engine chi thay `event_type`. Hai ben khai bao DOC
#  LAP cung mot gia dinh:
#       solver:  "de dat n5, phat n5 event loai EVT_F5"
#       engine:  "f5 dem event loai EVT_F5 trong cua so W"
#  Lech nhau => Gate A do. Do la cho Gate A co gia tri.
#  `EVT_*` la namespace noi bo cua Track A CFS; business event v2 nhu ORDER_PAID
#  chi la semantic alias/future stream vocabulary.
EVENT_TYPE_OF = {
    "f5": "EVT_F5",
    "f11": "EVT_F11",
    "f18": "EVT_F18",
    "f19": "EVT_F19",              # counter LOG10 thu 7 — chi co o scope v2
    "f30": "EVT_F30",
    "recency": "EVT_ORDER_PAID",   # CFS witness cho f1/f2 recency
    "FREE": "EVT_SESSION_STARTED", # §4.1 — khong counter T1 nao dem
}


class Unsolvable(ValueError):
    """P1 khong tim duoc nghiem nao => QUARANTINE, KHONG sua target."""


# ===========================================================================
# P1 · FEASIBILITY
# ===========================================================================
def feasible_candidates(d: DecodedTarget, branch: SemanticBranch) -> list[Candidate]:
    """Prototype dung EXHAUSTIVE enumeration (§4.4c).

    Day la cach DUY NHAT de TEST-01 kiem duoc optimizer ma khong phai tin vao
    chinh solver dang test. Solver that se dung branch-and-bound / CP / MILP.
    """
    return [c for c in branch.candidates(d) if branch.is_feasible(c, d)]


# ===========================================================================
# P2 · OPTIMIZATION -> OPTIMAL POOL
# ===========================================================================
def optimal_pool(
    candidates: Iterable[Candidate], branch: SemanticBranch
) -> tuple[Candidate, ...]:
    """argmin lexicographic. Tra ve TOAN BO nghiem dat argmin (§4.4b).

    Sap theo `key()` de pool tat dinh — khong phu thuoc thu tu duyet dau vao.
    """
    items = list(candidates)
    if not items:
        return ()
    best = min(branch.objective(c) for c in items)
    pool = [c for c in items if branch.objective(c) == best]
    return tuple(sorted(pool, key=lambda c: c.key()))


# ===========================================================================
# P3 · SEEDED SELECTION — chi chay TREN pool
# ===========================================================================
def select_from_pool(
    pool: Sequence[Candidate], *, target_id: str, seed: int, track: str = TRACK_A
) -> Candidate:
    """Chon MOT phan tu trong optimal pool.

    ★ Ham nay KHONG nhan tap feasible day du — no khong co duong nao de chon
      mot nghiem suboptimal. Do la cach thuc thi INVARIANT 3 bang CAU TRUC.

    PRNG dan xuat theo (target_id, seed, track) => moi user doc lap => song
    song hoa khong doi ket qua (§4.5).
    """
    if not pool:
        raise Unsolvable("optimal pool rong — khong co nghiem nao thoa P1")
    digest = hashlib.sha256(f"{target_id}\x1f{seed}\x1f{track}".encode()).digest()
    idx = int.from_bytes(digest[:8], "big") % len(pool)
    return pool[idx]


# ===========================================================================
# P3b · MATERIALIZE — candidate_slot_id -> occurrence -> event_id
# ===========================================================================
@dataclass(frozen=True)
class Event:
    event_id: str
    gen_reason: str
    day_offset: int
    sub_index: int
    occurrence: int
    event_ts: datetime
    event_type: str

    @property
    def candidate_slot_id(self) -> tuple[str, int, int]:
        return (self.gen_reason, self.day_offset, self.sub_index)


def _intra_day_ts(reference_ts: datetime, day_offset: int, seed: int,
                  gen_reason: str, sub_index: int) -> datetime:
    """Gio-trong-ngay la TU DO (T-6) — khong feature selected nao phu thuoc.

    Ham THUAN cua (seed, reason, day, sub_index) => khong phu thuoc traversal.
    """
    event_date = reference_ts.date() - timedelta(days=day_offset)
    day_start = datetime.combine(event_date, time.min, tzinfo=reference_ts.tzinfo)
    h = hashlib.sha256(f"{seed}\x1f{gen_reason}\x1f{day_offset}\x1f{sub_index}".encode()).digest()
    upper = 86_400
    if day_offset == 0:
        # Cung ngay voi reference_ts nhung van phai nam nghiem ngat o qua khu.
        upper = int((reference_ts - day_start).total_seconds())
        if upper <= 0:
            # Legacy/unit fixtures may use midnight. A same-calendar-day event
            # is impossible there under the strict PIT boundary, so place
            # non-recency events in the immediately preceding day. Production
            # runtime config deliberately uses an end-of-day REFERENCE_TS.
            day_start -= timedelta(days=1)
            upper = 86_400
    secs = int.from_bytes(h[:4], "big") % upper
    return day_start + timedelta(seconds=secs)


def materialize(
    candidate: Candidate, *, target_id: str, seed: int, reference_ts: datetime,
    track: str = TRACK_A,
) -> list[Event]:
    """§4.3a-1 / §4.3a-2.

    `occurrence` = thu hang trong TAP event cung `gen_reason`, sap theo
    (day_offset, sub_index) — KHONG dung event_ts (tranh vong dinh nghia voi
    §4.3), KHONG dung thu tu loop.
    """
    # Buoc 1 — bung slot thanh candidate_slot_id, thu tu tu CAU TRUC
    slot_ids: list[tuple[str, int, int]] = []
    for s in sorted(candidate.slots):
        slot_ids.extend((s.gen_reason, s.day_offset, i) for i in range(s.count))

    # Buoc 2 — occurrence trong pham vi TUNG gen_reason
    by_reason: dict[str, list[tuple[str, int, int]]] = {}
    for sid in slot_ids:
        by_reason.setdefault(sid[0], []).append(sid)

    events: list[Event] = []
    for reason, ids in by_reason.items():
        for occ, (r, day, sub) in enumerate(sorted(ids, key=lambda x: (x[1], x[2]))):
            eid = uuid.uuid5(EVENT_NS, f"{target_id}:{track}:{r}:{occ}")
            events.append(
                Event(
                    event_id=str(eid),
                    gen_reason=r,
                    day_offset=day,
                    sub_index=sub,
                    occurrence=occ,
                    event_ts=_intra_day_ts(reference_ts, day, seed, r, sub),
                    event_type=EVENT_TYPE_OF[r],
                )
            )
    return events


# ===========================================================================
# P4 · CANONICAL ORDERING — CHI sap thu tu
# ===========================================================================
def canonical_sequence(events: Iterable[Event]) -> list[Event]:
    """§4.3 — `(event_ts, event_type, event_id)`.

    🚫 KHONG phai co che chon nghiem. Voi seed co dinh, lua chon DA duoc quyet
       dinh o P3; ham nay chi dam bao thu tu dau ra on dinh.
    """
    return sorted(events, key=lambda e: (e.event_ts, e.event_type, e.event_id))


# ===========================================================================
# Orchestration
# ===========================================================================
@dataclass(frozen=True)
class ObjectiveSummary:
    unexplained_events: int
    active_days: int
    sessions: int
    total_events: int
    objective_value: tuple[int, ...]


@dataclass(frozen=True)
class SolveOutcome:
    status: Literal["SOLVED", "QUARANTINED"]
    events: tuple[Event, ...]
    candidate: Candidate | None
    pool_size: int
    objective_summary: ObjectiveSummary | None
    reason_code: str | None = None


def solve(
    decoded: DecodedTarget,
    branch: SemanticBranch,
    *,
    target_id: str,
    seed: int,
    reference_ts: datetime,
) -> SolveOutcome:
    """P1 -> P2 -> P3 -> P3b -> P4, dung THU TU DO, khong dao duoc."""
    feasible = feasible_candidates(decoded, branch)          # P1
    if not feasible:
        return SolveOutcome("QUARANTINED", (), None, 0, None, "NO_FEASIBLE_CANDIDATE")

    pool = optimal_pool(feasible, branch)                    # P2
    chosen = select_from_pool(pool, target_id=target_id, seed=seed)   # P3
    events = materialize(                                    # P3b
        chosen, target_id=target_id, seed=seed, reference_ts=reference_ts
    )
    ordered = canonical_sequence(events)                     # P4

    return SolveOutcome(
        status="SOLVED",
        events=tuple(ordered),
        candidate=chosen,
        pool_size=len(pool),
        objective_summary=ObjectiveSummary(
            unexplained_events=chosen.unexplained_events,
            active_days=chosen.active_days,
            sessions=chosen.sessions,
            total_events=chosen.total_events,
            objective_value=branch.objective(chosen),
        ),
    )

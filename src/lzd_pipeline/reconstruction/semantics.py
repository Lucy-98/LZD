"""SemanticBranch — MOT engine, HAI strategy.

RECONSTRUCTION_SPEC.md §4.2b, §15.4.

    🚫 CAM:  solver_h1.py + solver_h2.py   -> duplication, hai nhanh se drift
    ✅ DUNG: ReconstructionEngine + SemanticBranch(H1 | H2)

Branch CHI cung cap: decode · constraints · objective · candidate generation.
Engine chung giu: P1 -> P2 -> P3 -> P3b -> P4 · fingerprint · provenance · gates.

★ PHAT HIEN §4.2a: `active_days` BI GHIM duoi H1 (feature equality ep
  active_days == n30 chinh xac) => KHONG the la thanh phan objective.
  Duoi H2 no TU DO => LA thanh phan objective.
  => CA HAI thanh phan cua objective deu phu thuoc nhanh.
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Iterator, Protocol, runtime_checkable

from lzd_pipeline.reconstruction.candidate import GEN_FREE, Candidate, Slot

#: gen_reason cua cac counter T1. `f30` chi la counter RIENG duoi nhanh H2.
REASON_F5 = "f5"
REASON_F11 = "f11"
REASON_F18 = "f18"
REASON_F30 = "f30"

#: Moc nghiep vu cho f1/f2. Event nay co timestamp CO DINH (§5.2) — khong co
#: bac tu do — nen no khong tham gia toi uu, chi tham gia feasibility.
REASON_RECENCY = "recency"

COUNTER_REASONS = (REASON_F5, REASON_F11, REASON_F18)


def _recency_slots(d: "DecodedTarget") -> tuple[Slot, ...]:
    """Event CFS `EVT_ORDER_PAID` tai dung d1 va d2.

    §7.2 E-3: `d1 == d2` => sinh MOT event, khong phai hai event trung timestamp.
    Do la truong hop BINH THUONG — do duoc o 97.42% dong.
    """
    days = {d.d1, d.d2}
    return tuple(Slot(REASON_RECENCY, day, 1) for day in sorted(days))


@dataclass(frozen=True)
class DecodedTarget:
    """Gia tri DA GIAI MA tu 36-feature target — thu solver thuc su lam viec.

    Chi chua counter T1 + recency. KHONG chua gia tri feature goc.
    """

    n5: int
    n11: int
    n18: int
    n30: int
    d1: int          # days_since_first_*  (SYNTHETIC_ASSUMPTION S-01)
    d2: int          # days_since_last_*   (S-02);  bat bien do duoc: d1 >= d2
    window_days: int = 30

    def __post_init__(self) -> None:
        if self.d1 < self.d2:
            raise ValueError(f"d1 >= d2 la bat bien do duoc tren 100% dong (d1={self.d1}, d2={self.d2})")
        for name in ("n5", "n11", "n18", "n30"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} phai >= 1 (mien do duoc: [1, ...])")

    @property
    def forced_days(self) -> frozenset[int]:
        """Ngay BI EP active: event cua d1/d2 co timestamp CO DINH (§5.2).

        Chi tinh ngay nam TRONG cua so — d >= window thi khong anh huong n30.
        """
        return frozenset(d for d in (self.d1, self.d2) if d < self.window_days)


@runtime_checkable
class SemanticBranch(Protocol):
    name: str

    def objective(self, c: Candidate) -> tuple[int, ...]:
        """Tuple lexicographic. Thu tu trong tuple LA MOT PHAN CUA CONTRACT."""
        ...

    def is_feasible(self, c: Candidate, d: DecodedTarget) -> bool:
        """PHA 1 — hard constraint + exact feature equality. Khong danh doi."""
        ...

    def candidates(self, d: DecodedTarget) -> Iterator[Candidate]:
        """Sinh khong gian tim kiem. Prototype: exhaustive (§4.4c)."""
        ...


def _place_counters(
    days: tuple[int, ...], counts: tuple[tuple[str, int], ...]
) -> Iterator[tuple[Slot, ...]]:
    """Moi cach chia `count` event cua tung reason vao `days`.

    Xep chong duoc: nhieu event cung reason cung ngay la hop le (§5.1).
    """
    per_reason: list[list[list[Slot]]] = []
    for reason, total in counts:
        options: list[list[Slot]] = []
        # phan hoach `total` thanh len(days) phan khong am
        for combo in itertools.product(range(total + 1), repeat=len(days)):
            if sum(combo) != total:
                continue
            options.append([Slot(reason, d, c) for d, c in zip(days, combo) if c])
        per_reason.append(options)

    for picks in itertools.product(*per_reason):
        yield tuple(s for group in picks for s in group)


@dataclass(frozen=True)
class H1Branch:
    """f30 = active_days_in_last_30d  (SYNTHETIC_ASSUMPTION S-03).

    - `active_days` BI GHIM boi feature equality => KHONG vao objective (§4.2a)
    - Duoc phep sinh FREE_EVENT de thoa n30 (PROPOSITION FE-1)
    - objective = (unexplained_events, sessions)
    """

    name: str = "H1"

    def objective(self, c: Candidate) -> tuple[int, ...]:
        return (c.unexplained_events, c.sessions)

    def is_feasible(self, c: Candidate, d: DecodedTarget) -> bool:
        if c.count_of(REASON_F5) != d.n5:
            return False
        if c.count_of(REASON_F11) != d.n11:
            return False
        if c.count_of(REASON_F18) != d.n18:
            return False
        if c.count_of(REASON_F30) != 0:          # H1 khong co counter rieng cho f30
            return False
        # f1/f2: dung so moc phan biet (1 neu d1==d2, nguoc lai 2)
        if c.count_of(REASON_RECENCY) != len({d.d1, d.d2}):
            return False
        if c.active_days_in_window(d.window_days) != d.n30:
            return False
        return d.forced_days <= c.day_offsets    # ngay cua d1/d2 phai active

    def candidates(self, d: DecodedTarget) -> Iterator[Candidate]:
        forced = sorted(d.forced_days)
        if len(forced) > d.n30:
            return                                # Q_FORCED_DAYS — vo nghiem
        free_slots = d.n30 - len(forced)
        pool = [x for x in range(d.window_days) if x not in d.forced_days]

        rec = _recency_slots(d)   # moc f1/f2 — timestamp co dinh, khong toi uu

        for extra in itertools.combinations(pool, free_slots):
            days = tuple(sorted((*forced, *extra)))
            for base in _place_counters(days, ((REASON_F5, d.n5), (REASON_F11, d.n11), (REASON_F18, d.n18))):
                slots = (*base, *rec)
                cand = Candidate.of(slots)
                # Ngay chua co event nao phai duoc phu bang FREE_EVENT
                gap = set(days) - cand.day_offsets
                if gap:
                    cand = Candidate.of((*slots, *(Slot(GEN_FREE, g, 1) for g in gap)))
                yield cand


@dataclass(frozen=True)
class H2Branch:
    """f30 dem LOP HANH DONG RIENG cua no, bi cap o 30.

    - KHONG co FREE_EVENT => `unexplained_events` khong ton tai (§4.2b)
    - `active_days` TU DO => LA thanh phan objective
    - objective = (active_days, sessions)
    """

    name: str = "H2"

    def objective(self, c: Candidate) -> tuple[int, ...]:
        return (c.active_days, c.sessions)

    def is_feasible(self, c: Candidate, d: DecodedTarget) -> bool:
        if c.unexplained_events != 0:            # H2 khong cho phep FREE_EVENT
            return False
        if c.count_of(REASON_F5) != d.n5:
            return False
        if c.count_of(REASON_F11) != d.n11:
            return False
        if c.count_of(REASON_F18) != d.n18:
            return False
        if c.count_of(REASON_F30) != d.n30:      # counter rieng, khong phai ngay
            return False
        if c.count_of(REASON_RECENCY) != len({d.d1, d.d2}):
            return False
        return d.forced_days <= c.day_offsets

    def candidates(self, d: DecodedTarget) -> Iterator[Candidate]:
        forced = sorted(d.forced_days)
        pool = [x for x in range(d.window_days) if x not in d.forced_days]
        counts = (
            (REASON_F5, d.n5), (REASON_F11, d.n11),
            (REASON_F18, d.n18), (REASON_F30, d.n30),
        )
        rec = _recency_slots(d)
        # active_days tu do => duyet moi so ngay tu |forced| toi window
        for k in range(max(len(forced), 1), d.window_days + 1):
            for extra in itertools.combinations(pool, k - len(forced)):
                days = tuple(sorted((*forced, *extra)))
                for base in _place_counters(days, counts):
                    cand = Candidate.of((*base, *rec))
                    if cand.day_offsets == set(days):   # khong bo trong ngay nao
                        yield cand


def build_branch(name: str | None) -> SemanticBranch:
    """`semantic_branch` la CAU HINH, khong phai semantic truth (INVARIANT 2)."""
    match name:
        case "H1":
            return H1Branch()
        case "H2":
            return H2Branch()
        case _:
            raise ValueError(
                f"semantic_branch phai la 'H1' hoac 'H2' de chay solver, nhan {name!r}. "
                "semantic_status=UNIDENTIFIED van hop le — nhung phai CHON mot kich ban."
            )

"""Candidate — CAU TRUC, khong phai danh sach event.

RECONSTRUCTION_SPEC.md §4.3a-1.

Goc re cua bug parallelism: neu candidate duoc bieu dien bang `list[Event]` thi
thu tu trong list la mot su that NGAU NHIEN cua implementation. Bo han cach do:

    Candidate = multiset of (gen_reason, day_offset, count)

Event duoc MATERIALIZE bang ham thuan (§4.3a-2), `candidate_slot_id = (r, d, i)`
duoc xac dinh HOAN TOAN boi cau truc — truoc va doc lap voi moi vong lap.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable

#: gen_reason cua event khong counter T1 nao dem — chi ton tai duoi nhanh H1.
GEN_FREE = "FREE"

#: 🟨 SYNTHETIC — so event toi da trong mot session. Tham so cua prototype,
#: chua co evidence. Doi no => doi objective => phai bump objective_version.
SESSION_CAPACITY = 5


@dataclass(frozen=True, order=True)
class Slot:
    """`count` event cung `gen_reason` roi vao cung `day_offset`.

    `order=True` de sap xep canonical — KHONG dung de chon nghiem (§4.3).
    """

    gen_reason: str
    day_offset: int
    count: int

    def __post_init__(self) -> None:
        if self.count < 1:
            raise ValueError(f"slot phai co count >= 1, nhan {self.count}")
        if self.day_offset < 0:
            raise ValueError("day_offset phai >= 0 (dem lui tu reference_ts)")


@dataclass(frozen=True)
class Candidate:
    """Bat bien. Moi metric deu dan xuat TAT DINH tu `slots`."""

    slots: tuple[Slot, ...]

    @classmethod
    def of(cls, slots: Iterable[Slot]) -> "Candidate":
        """Gop slot trung (gen_reason, day_offset) roi sap canonical.

        Gop la bat buoc: hai slot cung (r, d) la CUNG MOT trang thai nghiep vu,
        neu de rieng thi hai candidate bang nhau ve nghia lai khac nhau ve cau
        truc => optimal pool dem trung.
        """
        merged: dict[tuple[str, int], int] = {}
        for s in slots:
            merged[(s.gen_reason, s.day_offset)] = (
                merged.get((s.gen_reason, s.day_offset), 0) + s.count
            )
        return cls(tuple(sorted(Slot(r, d, c) for (r, d), c in merged.items())))

    # -- metric, dung lam thanh phan objective (§4.2b) ----------------------
    @property
    def active_days(self) -> int:
        """So NGAY PHAN BIET co event — khong phai so event (§7.2 T-4).

        ⚠️ Day la tong TOAN BO lich su. Voi rang buoc f30 phai dung
        `active_days_in_window()` — xem ham do.
        """
        return len({s.day_offset for s in self.slots})

    def active_days_in_window(self, window_days: int) -> int:
        """So ngay phan biet TRONG cua so — day moi la thu f30 dem.

        Phai co ham rieng vi event recency (f1/f2) co the nam NGOAI cua so:
        `d1` co mien [0,365] con cua so f30 chi 30 ngay. Dung `active_days`
        cho rang buoc f30 se dem ca ngay ngoai cua so => lech voi SQL
        (`filter (where event_ts >= reference_ts - 30 days)`) => Gate A do
        ma khong hieu vi sao.
        """
        return len({s.day_offset for s in self.slots if s.day_offset < window_days})

    @property
    def total_events(self) -> int:
        return sum(s.count for s in self.slots)

    @property
    def unexplained_events(self) -> int:
        """So FREE_EVENT. Luon 0 duoi nhanh H2."""
        return sum(s.count for s in self.slots if s.gen_reason == GEN_FREE)

    @property
    def sessions(self) -> int:
        """Moi ngay active tach thanh ceil(events/SESSION_CAPACITY) session."""
        per_day: dict[int, int] = {}
        for s in self.slots:
            per_day[s.day_offset] = per_day.get(s.day_offset, 0) + s.count
        return sum(math.ceil(n / SESSION_CAPACITY) for n in per_day.values())

    def count_of(self, gen_reason: str) -> int:
        return sum(s.count for s in self.slots if s.gen_reason == gen_reason)

    @property
    def day_offsets(self) -> frozenset[int]:
        return frozenset(s.day_offset for s in self.slots)

    def key(self) -> tuple:
        """Khoa canonical de so sanh / sap xep pool tat dinh."""
        return tuple((s.gen_reason, s.day_offset, s.count) for s in self.slots)

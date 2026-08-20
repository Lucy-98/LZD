"""Constructive H1 solver — dat argmin MA KHONG can duyet khong gian nghiem.

docs/RECONSTRUCTION_SPEC.md §4.4b · §4.4c · §4.4a-1

★ VI SAO MODULE NAY TON TAI
--------------------------------------------------------------------------
§4.4c cho phep hai chien luoc:

    prototype    exhaustive enumeration    (kiem duoc objective theo nghia den)
    solver that  B&B / CP / MILP           (co chung minh can)

Do tren du lieu that: khong gian ma `engine.feasible_candidates()` phai duyet
co median 10^9.5 candidate moi target, p90 10^18.6, max 10^126.8. Chi 20.0%
target co khong gian <= 10^6. => exhaustive enumeration CHI dung duoc cho
unit test voi `window_days <= 6`, khong bao gio cho 926,669 dong that.

Nhung bai toan nay KHONG can search. Ca hai thanh phan cua objective H1 deu
co dang DONG, nen mot phep dung O(n30) vua dat argmin vua CHUNG MINH duoc no
dat argmin. Do la nghia vu ma §4.4c dat ra:

    SPEC SEMANTICS          CandidatePool = TOAN BO nghiem dat argmin
    IMPLEMENTATION OBLIGATION   solver phai CHUNG MINH nghiem da chon thuoc
                                argmin — khong bat buoc liet ke het pool

★ CHUNG MINH
--------------------------------------------------------------------------
Ky hieu, voi cua so W = `window_days` va k = n30:

    F      = {d1,d2} ∩ [0,W)      ngay recency NAM TRONG cua so
    n_out  = |{d1,d2}| − |F|      ngay recency NAM NGOAI cua so
    C      = tong so event counter (n5+n11+n18[+n19])
    CAP    = SESSION_CAPACITY

Moi nghiem kha thi duoi H1 deu co: dung k ngay active trong cua so, moi ngay
>= 1 event; recency ghim tai d1/d2; counter dat tu do tren k ngay do; FREE
phu cho ngay con trong.

(1) unexplained_events
    |F| ngay da co recency. k − |F| ngay con lai phai duoc phu boi counter
    hoac FREE. Counter cung cap toi da C ngay phan biet.
        => free >= max(0, k − |F| − C)                      [can duoi]
    Phep dung dat dung dau thuc => THANH PHAN 1 CUA OBJECTIVE DAT MIN.

(2) sessions
    Vi objective la LEXICOGRAPHIC, sau khi (1) da min thi so event trong cua
    so la HANG SO:
        T_in = C + |F| + free_min
    Moi ngay recency ngoai cua so giu dung 1 event => dong gop dung n_out.
    Con lai la bai toan: chia T_in event vao k ngay, moi ngay >= 1, cuc tieu
        Σ ceil(c_d / CAP)
    Can duoi:  Σ ceil(c_d/CAP) >= k          (moi ngay >= 1 event => >= 1)
               Σ ceil(c_d/CAP) >= ceil(T_in/CAP)
        => sessions >= n_out + max(k, ceil(T_in/CAP))       [can duoi]

    Dat duoc:
      T_in <= CAP·k : cho moi ngay >= 1 va <= CAP  =>  Σ ceil = k
      T_in >  CAP·k : k−1 ngay dung CAP, ngay con lai CAP + (T_in − CAP·k)
                      =>  Σ ceil = (k−1) + 1 + ceil((T_in − CAP·k)/CAP)
                                 = ceil(T_in/CAP)
    Phep dung duoi day sinh dung phan bo do => THANH PHAN 2 DAT MIN.

★ SEED VAN DUNG CHO §4.4a-1
--------------------------------------------------------------------------
    Seed CHI duoc phep tac dong SAU khi da co optimal pool.

Ca hai thanh phan objective phu thuoc DUY NHAT vao (k, |F|, n_out, C) —
KHONG phu thuoc vao viec chon NHUNG ngay nao. Nen moi cach chon k − |F| ngay
tu [0,W) tru F deu cho CUNG gia tri objective, tuc la deu nam TRONG argmin.
=> seed dang chon mot phan tu TRONG pool, dung nhu §4.4a-1 yeu cau.

    🚫 CAM: seed -> random candidate -> check objective
    ✅ DUNG: objective da min BANG CAU TRUC -> seed chon trong lop tuong duong

Day cung la ly do §4.4 noi seed la load-bearing: `construct_h1_candidate` cu
luon lay ngay 0,1,2,... nen moi user co mot khoi hoat dong lien ke dung o mep
gan nhat cua cua so — phan bo temporal suy bien ma Gate F sinh ra de bat.
"""
from __future__ import annotations

import hashlib
import math
from typing import Sequence

from lzd_pipeline.reconstruction.candidate import GEN_FREE, SESSION_CAPACITY, Candidate, Slot
from lzd_pipeline.reconstruction.semantics import (
    REASON_RECENCY,
    DecodedTarget,
    H1Branch,
)

TRACK_A = "A"


class Infeasible(ValueError):
    """Khong ton tai nghiem => QUARANTINE. 🚫 KHONG sua target."""


# ===========================================================================
# Can duoi cua objective — dung cho CA phep dung LAN assertion kiem chung
# ===========================================================================
def h1_objective_bound(d: DecodedTarget) -> tuple[int, int]:
    """`(unexplained_min, sessions_min)` — can duoi CHUNG MINH DUOC cua H1.

    Ham nay la phat bieu doc lap cua chung minh o docstring module. `solve_h1`
    assert ket qua cua no bang dung tuple nay, nen neu phep dung lech khoi
    argmin thi loi no NGAY tai cho, khong am tham di vao manifest.
    """
    forced = d.forced_days
    if d.n30 is None:
        days = max(len(forced), 1)
        total = d.counter_capacity + len(forced)
        return (0, max(days, math.ceil(total / SESSION_CAPACITY)))
    k = d.n30
    if k < len(forced):
        raise Infeasible(
            f"Q_FORCED_DAYS: n30={k} < |forced_days|={len(forced)} "
            f"(d1={d.d1}, d2={d.d2}) — mau thuan trong chinh target"
        )
    if k > d.window_days:
        raise Infeasible(f"n30={k} > window_days={d.window_days} — khong du ngay phan biet")

    n_out = len({d.d1, d.d2}) - len(forced)
    free_min = max(0, k - len(forced) - d.counter_capacity)
    t_in = d.counter_capacity + len(forced) + free_min
    sessions_min = n_out + max(k, math.ceil(t_in / SESSION_CAPACITY))
    return (free_min, sessions_min)


# ===========================================================================
# P3 · chon ngay active — seeded, chay TREN lop tuong duong argmin
# ===========================================================================
def _seeded_days(
    pool: Sequence[int], need: int, *, target_id: str, seed: int, track: str
) -> list[int]:
    """Chon `need` ngay tu `pool`, tat dinh theo (target_id, seed, track).

    🚫 KHONG dung `random.Random.sample`: thuat toan cua no khong duoc bao dam
       on dinh giua cac ban Python, ma §4.5 doi cung input => cung event.
       Sap theo digest co khoa thi tat dinh tuyet doi va doc lap moi thu.
    """
    ranked = sorted(
        pool,
        key=lambda day: hashlib.sha256(
            f"{target_id}\x1f{seed}\x1f{track}\x1fday{day}".encode()
        ).digest(),
    )
    return sorted(ranked[:need])


# ===========================================================================
# Phep dung
# ===========================================================================
def solve_h1(
    d: DecodedTarget, *, target_id: str, seed: int, track: str = TRACK_A
) -> Candidate:
    """Dung MOT candidate H1 nam trong argmin. O(n30 + so counter).

    Raise `Infeasible` khi target tu mau thuan (Q_FORCED_DAYS / Q_RANGE).
    """
    free_min, sessions_min = h1_objective_bound(d)   # cung raise Infeasible

    window = d.window_days
    forced = sorted(d.forced_days)
    k = d.n30 if d.n30 is not None else max(len(forced), 1)
    pool = [x for x in range(window) if x not in d.forced_days]
    days = sorted([
        *forced,
        *_seeded_days(pool, k - len(forced), target_id=target_id, seed=seed, track=track),
    ])

    # -- ngan sach event moi ngay ------------------------------------------
    # Buoc 1: recency ghim cung. Buoc 2: moi ngay active phai co >= 1 event.
    budget = {day: 0 for day in days}
    for day in forced:
        budget[day] += 1
    for day in days:
        if budget[day] == 0:
            budget[day] = 1

    # Buoc 3: rai phan con lai, DON day len CAP roi moi sang ngay ke tiep.
    # 🚫 KHONG rai deu (round-robin): khi T_in > CAP·k, rai deu tao them
    #    session thua — do dung la 6.38% target that bi lech khoi argmin.
    remaining = (d.counter_capacity + len(forced) + free_min) - sum(budget.values())
    for day in days:
        if remaining <= 0:
            break
        take = min(SESSION_CAPACITY - budget[day], remaining)
        if take > 0:
            budget[day] += take
            remaining -= take
    if remaining > 0:
        # T_in > CAP·k: tran la khong tranh duoc, don het vao MOT ngay de
        # so session bang dung ceil(T_in/CAP).
        budget[days[0]] += remaining

    # -- gan event that vao ngan sach --------------------------------------
    # Recency dat tai d1/d2 ke ca khi NGOAI cua so (mien [0,365]).
    slots: list[Slot] = [Slot(REASON_RECENCY, day, 1) for day in sorted({d.d1, d.d2})]

    demand: list[list] = [[reason, count] for reason, count in d.counter_demand]
    if free_min:
        demand.append([GEN_FREE, free_min])

    cursor = 0
    for day in days:
        room = budget[day] - (1 if day in d.forced_days else 0)
        while room > 0:
            while cursor < len(demand) and demand[cursor][1] == 0:
                cursor += 1
            if cursor >= len(demand):        # bat bien noi bo, khong nen xay ra
                raise AssertionError(
                    f"het event de phu ngan sach: day={day}, room={room}, "
                    f"budget={budget}, demand={demand}"
                )
            take = min(room, demand[cursor][1])
            slots.append(Slot(demand[cursor][0], day, take))
            demand[cursor][1] -= take
            room -= take

    leftover = [(r, c) for r, c in demand if c]
    if leftover:
        raise AssertionError(f"con event chua duoc dat: {leftover}")

    candidate = Candidate.of(slots)

    # -- kiem chung: kha thi VA dat argmin ---------------------------------
    branch = H1Branch()
    if not branch.is_feasible(candidate, d):
        raise AssertionError(
            f"candidate dung ra khong thoa PHA 1 cho target {target_id}: {candidate.key()}"
        )
    got = branch.objective(candidate)
    if got != (free_min, sessions_min):
        raise AssertionError(
            f"candidate KHONG dat argmin cho target {target_id}: "
            f"objective={got}, can duoi chung minh duoc={(free_min, sessions_min)}"
        )
    return candidate

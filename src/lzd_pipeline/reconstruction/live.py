"""Track B — Live Synthetic Event Generator.

RECONSTRUCTION_SPEC.md §0, §3.4.

    Track A:  target  ->  E*     ->  F'     (mot lan, deterministic reconstruction)
    Track B:  State   ->  E_future        (lien tuc, behaviour simulation)

★ INVARIANT 4 — CAPABILITY BOUNDARY

    Module nay CHI import: `state` + stdlib.

    🚫 KHONG import (ke ca bac cau):
           reconstruction.target        reconstruction.canonical
           reconstruction.feature_set   reconstruction.handoff
           reconstruction.engine        reconstruction.semantics
           reconstruction.candidate

    Ly do: `CustomerState.source_target_id` la OPAQUE LINEAGE ID. Neu Track B
    co ĐƯỜNG NÀO toi target repository, no giai duoc id do va "nhin trom" 36
    feature => generator co the lai event tuong lai de chieu theo feature =>
    closed-loop test do CHINH NO, khong do he thong.

    Rang buoc nay duoc canh boi `capability.assert_cannot_reach` (TEST-10b),
    khong phai boi code review.

Track A la feature-space aware. Track B chi la state/behaviour aware.
"""
from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterator, Protocol, runtime_checkable

from lzd_pipeline.reconstruction.state import CustomerState, Provenance

TRACK_B = "B"

#: Namespace RIENG cho Track B — event_id cua hai track khong bao gio dung nhau.
FUTURE_EVENT_NS = uuid.UUID("6f9b1c62-3f52-5a41-9d7e-000000000002")


class TemporalViolation(ValueError):
    """Gate B §12.1 — event Track B phai nam TU `as_of_ts` tro di."""


@dataclass(frozen=True)
class FutureEvent:
    event_id: str
    customer_id: str
    event_type: str
    event_ts: datetime
    session_id: str
    provenance: Provenance
    source_type: str = "SYNTHETIC"
    track: str = TRACK_B

    def __post_init__(self) -> None:
        if self.provenance.source_type != self.source_type:
            raise ValueError("FutureEvent va provenance phai co cung source_type")

    @property
    def generation_run_id(self) -> str:
        return self.provenance.generation_run_id


@runtime_checkable
class BehaviourModel(Protocol):
    """Co the la ML model HOAC rule-based (§9.1 P-4).

    `policy_version` bat buoc: `source_type=SYNTHETIC` phai truy vet duoc co che
    sinh. Rule-based co `ancestor_model_ids` rong MOT CACH CHINH DANG.
    """

    policy_version: str

    def next_events(
        self, state: CustomerState, t_from: datetime, t_to: datetime, rng_seed: int
    ) -> Iterator[tuple[str, datetime]]:
        """Tra ve (event_type, event_ts). KHONG nhan target, khong nhan feature."""
        ...


@dataclass(frozen=True)
class RuleBasedBehaviour:
    """Behaviour model toi thieu cho prototype — thuan rule, khong ML.

    Cuong do phu thuoc `state.counters` (trang thai da quan sat), KHONG phu
    thuoc gia tri feature — Track B khong co quyen biet chung.
    """

    policy_version: str = "rule_v1"
    base_events_per_day: int = 2

    def next_events(
        self, state: CustomerState, t_from: datetime, t_to: datetime, rng_seed: int
    ) -> Iterator[tuple[str, datetime]]:
        span_days = max(int((t_to - t_from).total_seconds() // 86_400), 1)
        # Counter va T2 level deu thuoc CustomerState da xac nhan. Track B
        # khong can va khong duoc doc feature payload goc.
        attribute_signal = sum(
            (index + 1) * (level + 1)
            for index, (_, level) in enumerate(sorted(state.attributes.items()))
        )
        intensity = (
            self.base_events_per_day
            + min(state.counters.get("f5", 0), 5)
            + attribute_signal % 3
        )

        for day in range(span_days):
            for i in range(intensity):
                h = hashlib.sha256(
                    f"{state.customer_id}\x1f{rng_seed}\x1f{day}\x1f{i}".encode()
                ).digest()
                secs = int.from_bytes(h[:4], "big") % 86_400
                ts = t_from + timedelta(days=day, seconds=secs)
                if ts >= t_to:
                    continue
                kinds = ("PRODUCT_VIEWED", "ITEM_ADDED_TO_CART", "SESSION_STARTED")
                kind = kinds[(h[4] + attribute_signal) % len(kinds)]
                yield kind, ts


def live_generator(
    state: CustomerState,
    behaviour_model: BehaviourModel,
    t_from: datetime,
    t_to: datetime,
    *,
    rng_seed: int = 0,
) -> Iterator[FutureEvent]:
    """Sinh event TUONG LAI tu `CustomerState(T0)`.

    🚫 KHONG nhan `ReconstructionTarget` — TEST-10a kiem dieu nay bang chu ky.
    🚫 KHONG reverse feature. KHONG dung canonical target de dieu khien viec sinh.

    Bat bien thoi gian (Gate B §12.1):
        moi event Track B co  event_ts >= state.as_of_ts
    """
    if t_from < state.as_of_ts:
        raise TemporalViolation(
            f"t_from={t_from.isoformat()} nam TRUOC as_of_ts={state.as_of_ts.isoformat()} "
            "— do la cua so cua Track A, Track B khong duoc cham vao."
        )
    if t_to <= t_from:
        raise TemporalViolation("t_to phai lon hon t_from")

    run_id = str(uuid.uuid5(
        FUTURE_EVENT_NS,
        f"{state.provenance.generation_run_id}:{state.customer_id}:run:"
        f"{t_from.isoformat()}:{t_to.isoformat()}:"
        f"{behaviour_model.policy_version}:{rng_seed}",
    ))
    provenance = Provenance(
        source_type="SYNTHETIC",
        generation_run_id=run_id,
        root_generation_id=state.provenance.root_generation_id,
        parent_run_id=state.provenance.generation_run_id,
        parent_entity_id=state.customer_id,
        parent_feature_version=state.provenance.parent_feature_version,
        behaviour_policy_version=behaviour_model.policy_version,
        created_at=t_from,
    )

    for kind, ts in behaviour_model.next_events(state, t_from, t_to, rng_seed):
        if ts < state.as_of_ts:
            raise TemporalViolation(
                f"behaviour model sinh event tai {ts.isoformat()} < as_of_ts — "
                "vi pham Gate B."
            )
        eid = uuid.uuid5(
            FUTURE_EVENT_NS,
            f"{run_id}:{state.customer_id}:{TRACK_B}:{kind}:{ts.isoformat()}:{rng_seed}",
        )
        session = uuid.uuid5(
            FUTURE_EVENT_NS,
            f"{run_id}:{state.customer_id}:{ts.date().isoformat()}:{rng_seed}",
        )
        yield FutureEvent(
            event_id=str(eid),
            customer_id=state.customer_id,
            event_type=kind,
            event_ts=ts,
            session_id=str(session),
            provenance=provenance,
        )

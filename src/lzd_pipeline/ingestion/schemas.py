"""Schema cua event app -> Kafka.

Contract nay la ranh gioi giua team app va team data. Doi field = doi version
topic (app.user.events.v1 -> v2), khong sua tai cho.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

EVENT_TYPES = (
    "app_open",
    "page_view",
    "search",
    "add_to_cart",
    "checkout",
    "order",
    "voucher_view",
    "voucher_claim",
)

# Event nao dong gop vao feature realtime nao
EVENT_TO_COUNTER = {
    "page_view": "rt_page_view_1h",
    "add_to_cart": "rt_add_to_cart_1h",
    "order": "rt_order_1h",
}


@dataclass
class AppEvent:
    event_id: str
    user_id: str
    event_type: str
    event_ts: float          # epoch giay (thoi diem xay ra o client)
    session_id: str
    platform: str            # web | android | ios
    item_id: str | None = None
    category_id: str | None = None
    price: float = 0.0
    quantity: int = 0
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


REQUIRED_FIELDS = ("event_id", "user_id", "event_type", "event_ts")


def validate_event(payload: dict[str, Any]) -> tuple[bool, str]:
    """Kiem tra toi thieu truoc khi cho vao lake. Sai -> DLQ."""
    for f in REQUIRED_FIELDS:
        if payload.get(f) in (None, ""):
            return False, f"missing_field:{f}"
    if payload.get("event_type") not in EVENT_TYPES:
        return False, f"unknown_event_type:{payload.get('event_type')}"
    try:
        ts = float(payload["event_ts"])
    except (TypeError, ValueError):
        return False, "bad_event_ts"
    if ts <= 0:
        return False, "bad_event_ts"
    if not str(payload["user_id"]).startswith("U"):
        return False, "bad_user_id"
    return True, ""


# Schema cua file parquet ghi xuong data lake (raw layer)
LAKE_COLUMNS = [
    "event_id", "user_id", "event_type", "event_ts", "session_id",
    "platform", "item_id", "category_id", "price", "quantity",
    "schema_version", "ingested_at", "kafka_partition", "kafka_offset",
]

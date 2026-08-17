from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lzd_pipeline.demo.track_b_online import (
    DemoOnlineBehaviour,
    DemoUser,
    TRACK_B_TO_APP,
    expected_realtime,
    generate_track_b_app_events,
    sync_demo_batch,
    wait_realtime,
)
from lzd_pipeline.features.online_store import OnlineFeatureStore, rt_bucket_start
from lzd_pipeline.features.spec import load_feature_spec
from lzd_pipeline.ingestion.schemas import validate_event
from lzd_pipeline.reconstruction.state import CustomerState, Provenance


def _state(user_id: str = "U0000000") -> CustomerState:
    ref = datetime(2026, 8, 1, tzinfo=timezone.utc)
    provenance = Provenance(
        source_type="RECONSTRUCTED",
        generation_run_id="track-a-run",
        root_generation_id="track-a-run",
        parent_target_id="train_0",
    )
    return CustomerState(
        customer_id=user_id,
        as_of_ts=ref,
        source_target_id="train_0",
        attributes={"segment": 1},
        counters={"f5": 2},
        semantic_branch="H1",
        semantic_status="UNIDENTIFIED",
        provenance=provenance,
        last_event_ts=None,
    )


def test_track_b_demo_adapter_chi_publish_app_event_hop_le():
    anchor = datetime(2026, 8, 17, 8, 0, tzinfo=timezone.utc)
    events, lineage = generate_track_b_app_events(
        [DemoUser("train_0", "U0000000", 6, _state())],
        anchor=anchor,
        seed=42,
    )

    assert events
    assert len(events) == len(lineage)
    assert all(validate_event(event.to_dict()) == (True, "") for event in events)
    assert all(not event.event_type.startswith("EVT_") for event in events)
    assert {row["source_type"] for row in lineage} == {"SYNTHETIC"}
    assert {row["track"] for row in lineage} == {"B"}
    assert {row["behaviour_policy_version"] for row in lineage} == {
        DemoOnlineBehaviour.policy_version
    }
    assert {row["track_b_event_type"] for row in lineage} <= set(TRACK_B_TO_APP)
    assert all(_state().as_of_ts <= datetime.fromisoformat(row["event_ts"])
               for row in lineage)


def test_demo_sync_ghi_dung_55f_roi_moi_activate():
    fakeredis = pytest.importorskip("fakeredis")
    spec = load_feature_spec()
    redis = fakeredis.FakeRedis(decode_responses=True)
    store = OnlineFeatureStore(redis_client=redis, spec=spec)
    row = {"user_id": "U0000000", **{
        name: float(index) for index, name in enumerate(spec.batch_names)
    }}

    report = sync_demo_batch([row], version="demo-test", store=store)

    assert report["feature_columns"] == 55
    assert report["validated_values"] == 55
    assert store.get_active_version() == "demo-test"
    batch, _ = store.get_features("U0000000", version="demo-test")
    assert set(spec.batch_names) <= set(batch)
    assert batch["_feature_set_id"] == "fs_2026_08_v2"


def test_expected_realtime_dem_duoc_funnel_track_b():
    anchor = datetime(2026, 8, 17, 8, 0, tzinfo=timezone.utc)
    events, _ = generate_track_b_app_events(
        [DemoUser("train_0", "U0000000", 6, _state())],
        anchor=anchor,
        seed=42,
    )
    expected = expected_realtime(events)["U0000000"]
    assert expected["rt_events_1h"] == len(events)
    assert expected["rt_page_view_1h"] == 2
    assert expected["rt_add_to_cart_1h"] == 1
    assert expected["rt_order_1h"] == 1


def test_rt_synthetic_co_du_wait_send_va_suppress_scenario():
    anchor = datetime(2026, 8, 17, 8, 0, tzinfo=timezone.utc)
    cohort = [
        DemoUser(f"train_{i}", f"U{i:07d}", 6, _state(f"U{i:07d}"))
        for i in range(10)
    ]
    events, _ = generate_track_b_app_events(cohort, anchor=anchor, seed=42)
    realtime = expected_realtime(events)

    assert len(events) == 45
    # U5: cart, chua order -> policy co the SEND.
    assert realtime["U0000005"]["rt_add_to_cart_1h"] == 1
    assert realtime["U0000005"]["rt_order_1h"] == 0
    # U1: chi view -> policy WAIT_FOR_INTENT.
    assert realtime["U0000001"]["rt_page_view_1h"] == 3
    assert realtime["U0000001"]["rt_add_to_cart_1h"] == 0
    assert realtime["U0000001"]["rt_order_1h"] == 0
    # U3: cart + order -> policy SUPPRESS_ALREADY_PURCHASED.
    assert realtime["U0000003"]["rt_add_to_cart_1h"] == 1
    assert realtime["U0000003"]["rt_order_1h"] == 1


class _SparseRealtimeStore:
    def get_features(self, user_id):
        return {}, {"rt_events_1h": "4", "rt_page_view_1h": "2"}

    def aggregate_realtime(self, raw, now=None):
        return raw


def test_wait_realtime_coi_counter_khong_co_trong_redis_la_zero():
    anchor = datetime(2026, 8, 17, 8, 0, tzinfo=timezone.utc)
    actual = wait_realtime(
        {"U0000000": {
            "rt_events_1h": 4,
            "rt_page_view_1h": 2,
            "rt_add_to_cart_1h": 0,
            "rt_order_1h": 0,
            "rt_gmv_1h": 0,
        }},
        anchor=anchor,
        store=_SparseRealtimeStore(),  # type: ignore[arg-type]
        timeout=0.01,
    )

    assert actual["U0000000"]["rt_events_1h"] == "4"


class _RecordingStore:
    def __init__(self) -> None:
        self.calls = []
        self.bulk = None

    def incr_realtime_counters(self, user_id, counters, ttl=None, now=None):
        self.calls.append((user_id, counters, now))

    def update_realtime_bulk(self, updates, ttl=None):
        self.bulk = updates


def test_consumer_bucket_theo_event_time_khong_theo_consume_time():
    pytest.importorskip("prometheus_client")
    from lzd_pipeline.ingestion.stream_consumer import StreamConsumer

    consumer = StreamConsumer.__new__(StreamConsumer)
    consumer.store = _RecordingStore()
    rows = [
        {"user_id": "U0000000", "event_type": "page_view", "event_ts": 1_000.0,
         "price": 0, "quantity": 0},
        {"user_id": "U0000000", "event_type": "add_to_cart", "event_ts": 1_301.0,
         "price": 10, "quantity": 1},
    ]

    users = consumer._update_realtime(rows)

    assert users == 1
    assert [call[2] for call in consumer.store.calls] == [
        float(rt_bucket_start(1_000.0)),
        float(rt_bucket_start(1_301.0)),
    ]
    assert consumer.store.bulk == {"U0000000": {"rt_last_event_ts": 1301}}

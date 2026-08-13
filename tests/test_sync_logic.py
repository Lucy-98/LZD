"""Test logic sync khong can Redis that (dung fakeredis neu co, khong thi skip).

    pip install fakeredis pytest
    pytest tests/test_sync_logic.py -v
"""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pytest

SPEC_PATH = str(Path(__file__).resolve().parents[1] / "config" / "features" / "feature_spec.yml")
os.environ.setdefault("FEATURE_SPEC_PATH", SPEC_PATH)

from lzd_pipeline.features.spec import load_feature_spec  # noqa: E402
from lzd_pipeline.features.sync import dt_of, make_version  # noqa: E402

fakeredis = pytest.importorskip("fakeredis", reason="can `pip install fakeredis`")

from lzd_pipeline.features.online_store import OnlineFeatureStore  # noqa: E402


@pytest.fixture
def store():
    return OnlineFeatureStore(
        redis_client=fakeredis.FakeRedis(decode_responses=True),
        spec=load_feature_spec(SPEC_PATH),
    )


# ---------------------------------------------------------------- version
def test_version_is_deterministic():
    """Cung ngay logic -> cung version. Day la nen tang cua idempotency."""
    assert make_version("2026-08-05") == "v20260805"
    assert make_version(date(2026, 8, 5)) == "v20260805"
    assert make_version("2026-08-05") == make_version("2026-08-05T00:00:00+07:00")


def test_dt_of():
    assert dt_of("2026-08-05T01:30:00+07:00") == "2026-08-05"


# ------------------------------------------------------------ idempotency
def test_write_rows_is_idempotent(store):
    rows = [{"user_id": "U1", "f1": 1.5, "f2": 2.5}, {"user_id": "U2", "f1": 3.0}]
    assert store.write_rows("v1", rows) == 2
    first = store.r.hgetall("fs:v1:u:U1")

    # Ghi lai y het -> khong nhan doi, gia tri khong doi
    assert store.write_rows("v1", rows) == 2
    second = store.r.hgetall("fs:v1:u:U1")

    assert first["f1"] == second["f1"] == "1.5"
    assert first["_feature_set_id"] == "fs_2026_08_v1"
    assert store.count_keys("fs:v1:u:*") == 2


def test_shard_marker_prevents_rewrite(store):
    rows = [{"user_id": "U1", "f1": 1.0}]
    r1 = store.write_shard("v1", 0, rows)
    assert r1.rows_written == 1 and not r1.skipped

    r2 = store.write_shard("v1", 0, rows)
    assert r2.skipped is True and r2.rows_written == 0


# ------------------------------------------------------- atomic activation
def test_activate_version_swaps_pointer(store):
    store.write_rows("v1", [{"user_id": "U1", "f1": 1.0}])
    store.write_rows("v2", [{"user_id": "U1", "f1": 9.0}])

    store.activate_version("v1")
    assert store.get_active_version() == "v1"
    merged, _, hit = store.get_features_merged("U1")
    assert hit and merged["f1"] == 1.0

    previous = store.activate_version("v2")
    assert previous == "v1"
    merged, _, _ = store.get_features_merged("U1")
    assert merged["f1"] == 9.0

    # Rollback khong can ghi lai du lieu
    store.rollback_to("v1")
    merged, _, _ = store.get_features_merged("U1")
    assert merged["f1"] == 1.0


def test_data_written_is_invisible_before_activation(store):
    """Ghi vao version moi KHONG duoc anh huong traffic dang chay."""
    store.write_rows("v1", [{"user_id": "U1", "f1": 1.0}])
    store.activate_version("v1")

    store.write_rows("v2", [{"user_id": "U1", "f1": 99.0}])   # dang sync
    merged, _, _ = store.get_features_merged("U1")
    assert merged["f1"] == 1.0                                 # van la ban cu


# ------------------------------------------------------- realtime overlay
def test_realtime_overlay_wins(store):
    store.write_rows("v1", [{"user_id": "U1", "f1": 1.0, "rt_events_1h": 0}])
    store.activate_version("v1")
    store.incr_realtime_counters("U1", {"rt_events_1h": 7})

    merged, _, _ = store.get_features_merged("U1")
    assert merged["rt_events_1h"] == 7
    assert merged["f1"] == 1.0


def test_realtime_has_ttl(store):
    store.update_realtime("U1", {"rt_events_1h": 3})
    assert store.r.ttl("rt:u:U1") > 0


# ------------------------------------------- cua so truot 1 gio (regression)
# Loi cu: HINCRBY vao 1 field + EXPIRE lai key sau moi lan ghi -> user hoat
# dong lien tuc thi key khong bao gio het han va counter cong don vo han,
# trong khi dbt van tinh dung 1 gio => training/serving skew.
def test_realtime_counter_only_counts_last_hour(store):
    from lzd_pipeline.features.online_store import RT_WINDOW_SECONDS

    now = 1_780_000_000.0

    store.incr_realtime_counters("U1", {"rt_events_1h": 5}, now=now - 2 * RT_WINDOW_SECONDS)
    store.incr_realtime_counters("U1", {"rt_events_1h": 3}, now=now - 600)   # 10 phut truoc
    store.incr_realtime_counters("U1", {"rt_events_1h": 2}, now=now)

    window = store.aggregate_realtime(store.r.hgetall("rt:u:U1"), now=now)
    # Chi 3 + 2 = 5 nam trong cua so; 5 event cua 2 gio truoc phai bi loai
    assert window["rt_events_1h"] == 5


def test_realtime_window_slides_forward(store):
    from lzd_pipeline.features.online_store import RT_WINDOW_SECONDS

    now = 1_780_000_000.0
    store.incr_realtime_counters("U1", {"rt_events_1h": 4}, now=now)

    assert store.aggregate_realtime(store.r.hgetall("rt:u:U1"), now=now)["rt_events_1h"] == 4

    # Cung du lieu do, doc lai sau 2 gio -> phai rong, khong con dinh gi
    later = now + 2 * RT_WINDOW_SECONDS
    assert "rt_events_1h" not in store.aggregate_realtime(
        store.r.hgetall("rt:u:U1"), now=later
    )


def test_old_buckets_are_pruned_not_accumulated(store):
    """Hash khong duoc phinh vo han cho user hoat dong ca ngay."""
    from lzd_pipeline.features.online_store import RT_BUCKET_SECONDS

    now = 1_780_000_000.0
    for i in range(40):                       # 40 o 5 phut = hon 3 gio
        store.incr_realtime_counters(
            "U1", {"rt_events_1h": 1}, now=now + i * RT_BUCKET_SECONDS
        )
    fields = store.r.hkeys("rt:u:U1")
    # Cua so chi co 12 o -> khong duoc giu ca 40
    assert len(fields) <= 13, f"hash dang phinh: {len(fields)} field"


def test_absolute_fields_pass_through(store):
    """Field khong phai counter (rt_last_event_ts) giu nguyen gia tri."""
    store.update_realtime("U1", {"rt_last_event_ts": 1_780_000_000})
    window = store.aggregate_realtime(store.r.hgetall("rt:u:U1"), now=1_780_000_000.0)
    assert int(window["rt_last_event_ts"]) == 1_780_000_000


def test_lua_path_is_actually_exercised(store):
    """Chan viec test am tham roi vao duong du phong.

    Neu thieu `lupa`, fakeredis khong chay duoc EVAL -> moi test o tren van
    xanh nhung Lua script chua he duoc kiem tra. Test nay bat lo hong do.
    """
    pytest.importorskip("lupa", reason="can `pip install lupa` de chay Lua")
    store.write_rows("v1", [{"user_id": "U1", "f1": 1.0}])
    store.activate_version("v1")
    store.incr_realtime_counters("U1", {"rt_events_1h": 1})
    store.read_for_serving("U1")
    assert store._lua_ok, "da roi vao duong du phong - Lua khong duoc test"


def test_serving_read_returns_version_and_both_hashes(store):
    """read_for_serving lay ca version lan 2 hash trong 1 lan goi."""
    store.write_rows("v1", [{"user_id": "U1", "f1": 1.0}])
    store.activate_version("v1")
    store.incr_realtime_counters("U1", {"rt_events_1h": 2})

    version, batch_raw, rt_raw = store.read_for_serving("U1")
    assert version == "v1"
    assert batch_raw["f1"] == "1.0"
    assert rt_raw                       # co it nhat 1 o
    assert store.read_for_serving("KHONG_TON_TAI")[1] == {}


# --------------------------------------------------------------------- GC
def test_gc_keeps_recent_and_active(store):
    for v in ("v1", "v2", "v3", "v4"):
        store.write_rows(v, [{"user_id": "U1", "f1": 1.0}])
        store.set_version_status(v, status="ACTIVE")
    store.activate_version("v4")

    result = store.gc_old_versions(keep=2)
    remaining = set(store.list_versions())

    assert "v4" in remaining                       # active luon duoc giu
    assert len(remaining) <= 3
    assert all(store.count_keys(f"fs:{v}:u:*") == 0 for v in result["removed"])


def test_expire_version_sets_ttl_not_delete(store):
    store.write_rows("v1", [{"user_id": "U1", "f1": 1.0}])
    store.expire_version("v1", ttl=60)
    assert store.r.ttl("fs:v1:u:U1") > 0
    assert store.r.exists("fs:v1:u:U1")            # van con, chi la se tu het han

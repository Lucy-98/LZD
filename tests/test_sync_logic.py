"""Test khong can Docker - chay bang: pytest tests/ -v

Test cac tinh chat cot loi cua sync:
  - IDEMPOTENCY : ghi lai khong nhan doi, rerun shard khong ghi trung
  - CONSISTENCY : swap con tro atomic, version cu khong bi xoa khi dang phuc vu
  - FAULT TOLER.: shard da xong duoc danh dau va bo qua khi chay lai
  - OBSERVABILITY: version co status day du trong Redis meta
"""
from __future__ import annotations

import os
import time
from datetime import date
from pathlib import Path

import fakeredis
import pytest

SPEC_PATH = str(Path(__file__).resolve().parents[1] / "config" / "features" / "feature_spec.yml")
os.environ.setdefault("FEATURE_SPEC_PATH", SPEC_PATH)

from lzd_pipeline.features.online_store import OnlineFeatureStore  # noqa: E402
from lzd_pipeline.features.spec import load_feature_spec  # noqa: E402
from lzd_pipeline.features.sync import dt_of, make_version  # noqa: E402


@pytest.fixture
def store():
    # Fakeredis dung server rieng cho tung test de khong bi nhiem state
    server = fakeredis.FakeServer()
    r = fakeredis.FakeRedis(server=server, decode_responses=True)
    return OnlineFeatureStore(
        redis_client=r, spec=load_feature_spec(SPEC_PATH)
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
    rows = [
        {"user_id": "U1", "customer_value_score": 1.5, "days_since_first_signal": 2.5},
        {"user_id": "U2", "customer_value_score": 3.0},
    ]
    assert store.write_rows("v1", rows) == 2
    first = store.r.hgetall("fs:v1:u:U1")

    # Ghi lai y het -> khong nhan doi, gia tri khong doi
    assert store.write_rows("v1", rows) == 2
    second = store.r.hgetall("fs:v1:u:U1")

    assert first["customer_value_score"] == second["customer_value_score"] == "1.5"
    assert first["_feature_set_id"] == "fs_2026_08_v4"
    assert store.count_keys("fs:v1:u:*") == 2


def test_shard_marker_prevents_rewrite(store):
    rows = [{"user_id": "U1", "customer_value_score": 1.0}]
    r1 = store.write_shard("v1", 0, rows)
    assert r1.rows_written == 1 and not r1.skipped

    r2 = store.write_shard("v1", 0, rows)
    assert r2.skipped is True and r2.rows_written == 0


# ------------------------------------------------------- atomic activation
def test_activate_version_swaps_pointer(store):
    store.write_rows("v1", [{"user_id": "U1", "customer_value_score": 1.0}])
    store.write_rows("v2", [{"user_id": "U1", "customer_value_score": 9.0}])

    store.activate_version("v1")
    assert store.get_active_version() == "v1"
    merged, _, hit = store.get_features_merged("U1")
    assert hit and merged["customer_value_score"] == 1.0

    previous = store.activate_version("v2")
    assert previous == "v1"
    merged, _, _ = store.get_features_merged("U1")
    assert merged["customer_value_score"] == 9.0

    # Rollback khong can ghi lai du lieu
    store.rollback_to("v1")
    merged, _, _ = store.get_features_merged("U1")
    assert merged["customer_value_score"] == 1.0


def test_data_written_is_invisible_before_activation(store):
    """Ghi vao version moi KHONG duoc anh huong traffic dang chay."""
    store.write_rows("v1", [{"user_id": "U1", "customer_value_score": 1.0}])
    store.activate_version("v1")

    store.write_rows("v2", [{"user_id": "U1", "customer_value_score": 99.0}])   # dang sync
    merged, _, _ = store.get_features_merged("U1")
    assert merged["customer_value_score"] == 1.0                                 # van la ban cu


# ------------------------------------------------------- realtime overlay
def test_realtime_overlay_wins(store):
    store.write_rows("v1", [{"user_id": "U1", "customer_value_score": 1.0, "rt_events_1h": 0}])
    store.activate_version("v1")
    store.incr_realtime_counters("U1", {"rt_events_1h": 7})

    merged, _, _ = store.get_features_merged("U1")
    assert merged["rt_events_1h"] == 7
    assert merged["customer_value_score"] == 1.0


def test_realtime_has_ttl(store):
    store.update_realtime("U1", {"rt_events_1h": 3})
    assert store.r.ttl("rt:u:U1") > 0


# ------------------------------------------- cua so truot 1 gio (regression)
def test_realtime_counter_only_counts_last_hour(store):
    now = time.time()
    store.incr_realtime_counters("U1", {"rt_events_1h": 10}, now=now - 7200)
    store.incr_realtime_counters("U1", {"rt_events_1h": 3}, now=now)

    merged, _, _ = store.get_features_merged("U1", now=now)
    assert merged["rt_events_1h"] == 3


def test_realtime_bucket_cleans_stale_cells_on_write(store):
    now = time.time()
    store.incr_realtime_counters("U1", {"rt_events_1h": 5}, now=now - 7200)
    assert len(store.r.hkeys("rt:u:U1")) >= 1

    store.incr_realtime_counters("U1", {"rt_events_1h": 1}, now=now)
    fields = store.r.hkeys("rt:u:U1")
    for f in fields:
        if "|" in f:
            _, _, suffix = f.partition("|")
            assert int(suffix) >= now - 3600


# --------------------------------------------------------------- mget_raw
def test_mget_raw_tra_ve_dung_so_va_thu_tu(store):
    store.write_rows("v1", [
        {"user_id": "U1", "customer_value_score": 1.0},
        {"user_id": "U2", "customer_value_score": 2.0},
    ])
    store.activate_version("v1")
    store.incr_realtime_counters("U1", {"rt_events_1h": 5})

    version, raw = store.mget_raw(["U1", "U2", "U_MISS"])
    assert version == "v1"
    assert set(raw.keys()) == {"U1", "U2", "U_MISS"}

    b1, r1 = raw["U1"]
    assert b1["customer_value_score"] == "1.0"
    assert r1

    b2, r2 = raw["U2"]
    assert b2["customer_value_score"] == "2.0"
    assert r2 == {}

    b_miss, r_miss = raw["U_MISS"]
    assert b_miss == {}
    assert r_miss == {}


def test_mget_raw_tra_ve_none_khi_chua_co_active_version(store):
    version, raw = store.mget_raw(["U1", "U2"])
    assert version is None
    assert raw["U1"] == ({}, {})
    assert raw["U2"] == ({}, {})


# ---------------------------------------------------- Lua engine fallback
def test_lua_read_works_or_falls_back_cleanly(store):
    store.write_rows("v1", [{"user_id": "U1", "customer_value_score": 1.0}])
    store.activate_version("v1")
    store.read_for_serving("U1")
    assert store._lua_ok, "da roi vao duong du phong - Lua khong duoc test"


def test_serving_read_returns_version_and_both_hashes(store):
    """read_for_serving lay ca version lan 2 hash trong 1 lan goi."""
    store.write_rows("v1", [{"user_id": "U1", "customer_value_score": 1.0}])
    store.activate_version("v1")
    store.incr_realtime_counters("U1", {"rt_events_1h": 2})

    version, batch_raw, rt_raw = store.read_for_serving("U1")
    assert version == "v1"
    assert batch_raw["customer_value_score"] == "1.0"
    assert rt_raw                       # co it nhat 1 o
    assert store.read_for_serving("KHONG_TON_TAI")[1] == {}


# --------------------------------------------------------------------- GC
def test_gc_keeps_recent_and_active(store):
    for v in ("v1", "v2", "v3", "v4"):
        store.write_rows(v, [{"user_id": "U1", "customer_value_score": 1.0}])
        store.set_version_status(v, status="ACTIVE")
    store.activate_version("v4")

    result = store.gc_old_versions(keep=2)
    remaining = set(store.list_versions())

    assert "v4" in remaining                       # active luon duoc giu
    assert len(remaining) <= 3
    assert all(store.count_keys(f"fs:{v}:u:*") == 0 for v in result["removed"])


def test_expire_version_sets_ttl_not_delete(store):
    store.write_rows("v1", [{"user_id": "U1", "customer_value_score": 1.0}])
    store.expire_version("v1", ttl=60)
    assert store.r.ttl("fs:v1:u:U1") > 0
    assert store.r.exists("fs:v1:u:U1")            # van con, chi la se tu het han

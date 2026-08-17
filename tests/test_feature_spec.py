"""Test khong can Docker - chay bang: pytest tests/ -v

Kiem tra phan "hop dong" cua he thong: spec, key layout, merge logic,
sinh version. Day la nhung cho ma sai se gay offline/online serving skew.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

SPEC_PATH = str(Path(__file__).resolve().parents[1] / "config" / "features" / "feature_spec.yml")
os.environ.setdefault("FEATURE_SPEC_PATH", SPEC_PATH)

from lzd_pipeline.features.spec import load_feature_spec  # noqa: E402


@pytest.fixture(scope="module")
def spec():
    return load_feature_spec(SPEC_PATH)


def test_batch_features_are_the_selected_set(spec):
    """Redis batch contract chi duoc sync selected features, khong sync f0..f82."""
    names = spec.batch_names
    assert "f1" in names
    assert "f82" in names
    assert "f7" not in names       # f7 nam ngoai selected set o ca v1 lan v2
    assert "f33" not in names      # ban sao 100% cua f31 — da bi loai khi chon
    assert "label" not in names
    assert "is_treat" not in names
    assert len([n for n in names if n.startswith("f") and n[1:].isdigit()]) == 55
    assert spec.offline["serving_table"] == "marts.feat_user_selected_serving"
    assert spec.offline["selected_feature_set_id"] == "fs_2026_08_v2"


def test_batch_features_khop_chinh_xac_feature_set_artifact(spec):
    """Chong drift B5: spec sync va solver contract phai la MOT danh sach.

    Truoc day hai file nay doc lap nhau, nen mo scope o mot ben ma quen ben
    kia se lam Redis thieu cot dung luc serving — loi chi lo o production.
    """
    from lzd_pipeline.reconstruction.feature_set import load_feature_set

    fs = load_feature_set()
    f_names = {n for n in spec.batch_names if n.startswith("f") and n[1:].isdigit()}
    assert f_names == set(fs.columns)
    assert spec.offline["selected_feature_set_id"] == fs.id


def test_no_duplicate_feature_names(spec):
    names = spec.all_names
    assert len(names) == len(set(names))


def test_key_templates(spec):
    assert spec.batch_key("v20260805", "U0000123") == "fs:v20260805:u:U0000123"
    assert spec.realtime_key("U0000123") == "rt:u:U0000123"
    assert spec.meta_key("active_version") == "fs:meta:active_version"


def test_merge_realtime_overrides_batch(spec):
    """Overlay realtime phai thang feature batch cung ten."""
    batch = {"f1": "1.5", "rt_events_1h": "0"}
    realtime = {"rt_events_1h": "42"}
    merged, missing = spec.merge(batch, realtime)
    assert merged["f1"] == 1.5
    assert merged["rt_events_1h"] == 42
    assert missing > 0        # cac feature khac dung default


def test_merge_fills_defaults_when_cache_miss(spec):
    """Cache miss hoan toan -> tra ve du bo feature bang gia tri default."""
    merged, missing = spec.merge({}, {})
    assert len(merged) == len(spec.all_features)
    assert missing == len(spec.all_features)
    assert merged["f1"] == 0.0
    assert merged["rt_events_1h"] == 0


def test_cast_handles_garbage(spec):
    f = spec.by_name("f1")
    assert f.cast("abc") == f.default
    assert f.cast(None) == f.default
    assert f.cast("") == f.default
    assert f.cast("3.14") == pytest.approx(3.14)


def test_validate_columns_detects_missing(spec):
    missing = spec.validate_columns(["user_id", "f1"], scope="batch")
    assert "f2" in missing
    assert "f1" not in missing


def test_all_features_have_defaults(spec):
    """Moi feature phai co default - neu khong, cache miss se lam vo request."""
    for feature in spec.all_features:
        assert feature.default is not None, feature.name

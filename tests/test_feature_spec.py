"""Test khong can Docker - chay bang: pytest tests/ -v

Kiem tra phan "hop dong" cua he thong: spec, key layout, merge logic,
sinh version. Day la nhung cho ma sai se gay training/serving skew.
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
    """Redis batch contract sync serving features với tên nghiệp vụ rõ ràng."""
    names = spec.batch_names
    assert "price_sensitivity_segment" in names
    assert "customer_value_score" in names
    assert "days_since_first_signal" in names
    assert "order_cnt_7d" in names
    assert "label" not in names
    assert "is_treat" not in names
    assert spec.offline["serving_table"] == "marts.serving_features"
    assert spec.offline["selected_feature_set_id"] == "fs_2026_08_v3"


def test_feature_set_artifact_loads_correctly():
    """Artifact contract v3 tải đúng 30 features."""
    from lzd_pipeline.reconstruction.feature_set import load_feature_set

    v3_path = Path(__file__).resolve().parents[1] / "config" / "features" / "fs_2026_08_v3.yaml"
    fs = load_feature_set(v3_path)
    assert fs.id == "fs_2026_08_v3"
    assert len(fs.columns) == 30
    assert "f1" in fs.columns
    assert "f80" in fs.columns


def test_no_duplicate_feature_names(spec):
    names = spec.all_names
    assert len(names) == len(set(names))


def test_key_templates(spec):
    assert spec.batch_key("v20260805", "U0000123") == "fs:v20260805:u:U0000123"
    assert spec.realtime_key("U0000123") == "rt:u:U0000123"
    assert spec.meta_key("active_version") == "fs:meta:active_version"


def test_merge_realtime_overrides_batch(spec):
    """Overlay realtime phai thang feature batch cung ten."""
    batch = {"customer_value_score": "1.5", "rt_events_1h": "0"}
    realtime = {"rt_events_1h": "42"}
    merged, missing = spec.merge(batch, realtime)
    assert merged["customer_value_score"] == 1.5
    assert merged["rt_events_1h"] == 42
    assert missing > 0        # cac feature khac dung default


def test_merge_fills_defaults_when_cache_miss(spec):
    """Cache miss hoan toan -> tra ve du bo feature bang gia tri default."""
    merged, missing = spec.merge({}, {})
    assert len(merged) == len(spec.all_features)
    assert missing == len(spec.all_features)
    assert merged["customer_value_score"] == 0.0
    assert merged["rt_events_1h"] == 0
    assert merged["rt_page_view_5m"] == 0


def test_cast_handles_garbage(spec):
    f = spec.by_name("customer_value_score")
    assert f is not None
    assert f.cast("abc") == f.default
    assert f.cast(None) == f.default
    assert f.cast("") == f.default
    assert f.cast("3.14") == pytest.approx(3.14)


def test_validate_columns_detects_missing(spec):
    missing = spec.validate_columns(["user_id", "customer_value_score"], scope="batch")
    assert "days_since_first_signal" in missing
    assert "customer_value_score" not in missing


def test_all_features_have_defaults(spec):
    """Moi feature phai co default - neu khong, cache miss se lam vo request."""
    for feature in spec.all_features:
        assert feature.default is not None, feature.name

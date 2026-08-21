from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
os.environ.setdefault(
    "FEATURE_SPEC_PATH", str(ROOT / "config" / "features" / "feature_spec.yml")
)

from lzd_pipeline.features.compatibility import (  # noqa: E402
    REALTIME_SEMANTICS_VERSION,
    check_model_feature_compatibility,
    feature_schema_hash,
)
from lzd_pipeline.features.spec import load_feature_spec  # noqa: E402


def _status(spec):
    return {
        "feature_spec_version": spec.offline["selected_feature_set_id"],
        "schema_hash": feature_schema_hash(spec),
        "realtime_semantics_version": REALTIME_SEMANTICS_VERSION,
    }


def test_baseline_30f_is_compatible_with_active_batch_contract():
    spec = load_feature_spec()
    model_features = [f.name for f in spec.batch_features if f.group == "model_30f"]
    report = check_model_feature_compatibility(
        model_features=model_features,
        model_feature_spec_version="fs_2026_08_v4",
        model_realtime_semantics_version=REALTIME_SEMANTICS_VERSION,
        requires_realtime=False,
        spec=spec,
        active_status=_status(spec),
    )
    assert report.compatible
    assert not report.reasons


def test_missing_or_reordered_contract_metadata_fails_closed():
    spec = load_feature_spec()
    status = _status(spec)
    status["schema_hash"] = "tampered"
    report = check_model_feature_compatibility(
        model_features=["f0", "does_not_exist"],
        model_feature_spec_version="wrong",
        model_realtime_semantics_version="",
        requires_realtime=False,
        spec=spec,
        active_status=status,
    )
    assert not report.compatible
    assert len(report.reasons) == 3


def test_rt_model_requires_same_realtime_semantics():
    spec = load_feature_spec()
    report = check_model_feature_compatibility(
        model_features=["f0", "rt_events_1h"],
        model_feature_spec_version="fs_2026_08_v4",
        model_realtime_semantics_version="old_processing_time_v0",
        requires_realtime=True,
        spec=spec,
        active_status=_status(spec),
    )
    assert not report.compatible
    assert "model realtime semantics are incompatible" in report.reasons

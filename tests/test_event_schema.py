"""Test validate event - quyet dinh event nao vao lake, event nao vao DLQ."""
from __future__ import annotations

import time

from lzd_pipeline.ingestion.schemas import AppEvent, EVENT_TO_COUNTER, validate_event


def _valid_payload(**overrides):
    payload = AppEvent(
        event_id="e1",
        user_id="U0000001",
        event_type="page_view",
        event_ts=time.time(),
        session_id="s1",
        platform="web",
    ).to_dict()
    payload.update(overrides)
    return payload


def test_valid_event_passes():
    ok, reason = validate_event(_valid_payload())
    assert ok and reason == ""


def test_missing_user_id_goes_to_dlq():
    payload = _valid_payload()
    del payload["user_id"]
    ok, reason = validate_event(payload)
    assert not ok and reason == "missing_field:user_id"


def test_bad_timestamp_rejected():
    ok, reason = validate_event(_valid_payload(event_ts="hom-qua"))
    assert not ok and reason == "bad_event_ts"


def test_unknown_event_type_rejected():
    ok, reason = validate_event(_valid_payload(event_type="teleport"))
    assert not ok and reason.startswith("unknown_event_type")


def test_bad_user_id_format_rejected():
    ok, reason = validate_event(_valid_payload(user_id="12345"))
    assert not ok and reason == "bad_user_id"


def test_counter_mapping_matches_spec():
    """Moi counter realtime trong mapping phai co trong feature_spec.yml."""
    import os
    from pathlib import Path

    spec_path = str(Path(__file__).resolve().parents[1] / "config" / "features" / "feature_spec.yml")
    os.environ.setdefault("FEATURE_SPEC_PATH", spec_path)
    from lzd_pipeline.features.spec import load_feature_spec

    spec = load_feature_spec(spec_path)
    for field in EVENT_TO_COUNTER.values():
        assert field in spec.realtime_names, f"{field} thieu trong feature_spec.yml"

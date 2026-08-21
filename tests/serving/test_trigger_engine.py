from __future__ import annotations

import fakeredis
import pytest

from lzd_pipeline.features.online_store import OnlineFeatureStore
from lzd_pipeline.features.spec import load_feature_spec
from lzd_pipeline.serving.trigger_engine import (
    TriggerEngine,
    determine_voucher_code,
)


def test_determine_voucher_code():
    # Score >= threshold -> SEND_VOUCHER
    dec, code = determine_voucher_code(0.06, threshold=0.05)
    assert dec == "SEND_VOUCHER"
    assert code == "voucher_30"

    # Score < threshold -> NO_VOUCHER
    dec, code = determine_voucher_code(0.01, threshold=0.05)
    assert dec == "NO_VOUCHER"
    assert code == "no_voucher"

    # None -> NO_DECISION
    dec, code = determine_voucher_code(None)
    assert dec == "NO_DECISION"
    assert code == "no_voucher"


def test_trigger_engine_evaluate_user():
    server = fakeredis.FakeServer()
    r = fakeredis.FakeRedis(server=server, decode_responses=True)
    spec = load_feature_spec()
    store = OnlineFeatureStore(redis_client=r, spec=spec)

    # Seed batch features
    store.write_rows("v1", [
        {"user_id": "U123", "customer_value_score": 1.0, "price_sensitivity_segment": 2.0}
    ])
    store.activate_version("v1")

    # Seed realtime features
    store.update_realtime("U123", {"rt_events_1h": 5, "rt_page_view_5m": 3})

    engine = TriggerEngine(store=store)
    result = engine.evaluate_user("U123")

    assert result["user_id"] == "U123"
    assert result["feature_version"] == "v1"
    assert result["cache_hit"] is True
    assert "decision" in result
    assert "voucher_code" in result
    assert "latency_ms" in result

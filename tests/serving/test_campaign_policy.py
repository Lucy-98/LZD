from __future__ import annotations

import pytest

from lzd_pipeline.serving.campaign_policy import (
    NOT_SELECTED_BUDGET,
    SEND_VOUCHER,
    SKIP_NON_POSITIVE_UPLIFT,
    SUPPRESS_ALREADY_PURCHASED,
    WAIT_FOR_INTENT,
    apply_top_k_realtime_policy,
)


def _row(user_id: str, score: float) -> dict:
    return {
        "user_id": user_id,
        "uplift_score": score,
        "cache_hit": True,
        "features_supplied": 55,
        "realtime_applied": 0,
    }


def test_top_k_xep_hang_model_va_rt_chi_gate_thoi_diem():
    scored = [
        _row("U_SEND", 0.030),
        _row("U_WAIT", 0.020),
        _row("U_BOUGHT", 0.010),
        _row("U_BUDGET", 0.005),
        _row("U_NEG", -0.002),
    ]
    realtime = {
        "U_SEND": {"rt_add_to_cart_1h": 2},
        "U_WAIT": {"rt_page_view_1h": 3},
        "U_BOUGHT": {"rt_add_to_cart_1h": 1, "rt_order_1h": 1},
        "U_BUDGET": {"rt_add_to_cart_1h": 1},
        "U_NEG": {"rt_add_to_cart_1h": 9},
    }

    report = apply_top_k_realtime_policy(scored, realtime, budget=3)
    by_user = {row["user_id"]: row for row in report["results"]}

    assert by_user["U_SEND"]["campaign_action"] == SEND_VOUCHER
    assert by_user["U_WAIT"]["campaign_action"] == WAIT_FOR_INTENT
    assert by_user["U_BOUGHT"]["campaign_action"] == SUPPRESS_ALREADY_PURCHASED
    assert by_user["U_BUDGET"]["campaign_action"] == NOT_SELECTED_BUDGET
    assert by_user["U_NEG"]["campaign_action"] == SKIP_NON_POSITIVE_UPLIFT
    assert report["policy"]["effective_cutoff"] == pytest.approx(0.010)
    assert report["policy"]["model_score_uses_realtime"] is False
    assert all(row["realtime_applied_to_model"] == 0 for row in report["results"])


@pytest.mark.parametrize("budget", [0, 3])
def test_policy_tu_choi_budget_khong_hop_le(budget):
    rows = [_row("U1", 0.1), _row("U2", 0.05)]
    with pytest.raises(ValueError):
        apply_top_k_realtime_policy(rows, {}, budget=budget)


def test_policy_tu_choi_user_trung():
    rows = [_row("U1", 0.1), _row("U1", 0.05)]
    with pytest.raises(ValueError, match="duy nhat"):
        apply_top_k_realtime_policy(rows, {}, budget=1)

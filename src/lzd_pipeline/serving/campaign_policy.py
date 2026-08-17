"""Campaign policy cho demo: model chon USER, realtime chon THOI DIEM.

DRLearner artifact hien tai khong co ``rt_*`` trong 76 cot dau vao. Vi vay
policy nay co y tach hai viec:

* ``uplift_score`` xep hang user theo loi ich du doan cua voucher;
* ``rt_*`` chi gate thoi diem phat: co intent, da mua, hay con cho.

Day la policy synthetic/bounded cho demo, khong phai threshold production da
toi uu. Ham thuan Python de co the test ma khong can FastAPI/Redis.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Mapping, Sequence


SEND_VOUCHER = "SEND_VOUCHER"
WAIT_FOR_INTENT = "WAIT_FOR_INTENT"
SUPPRESS_ALREADY_PURCHASED = "SUPPRESS_ALREADY_PURCHASED"
NOT_SELECTED_BUDGET = "NOT_SELECTED_BUDGET"
SKIP_NON_POSITIVE_UPLIFT = "SKIP_NON_POSITIVE_UPLIFT"
NO_MODEL_SCORE = "NO_MODEL_SCORE"


def _number(values: Mapping[str, Any], name: str) -> float:
    try:
        return float(values.get(name) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _score_key(row: Mapping[str, Any]) -> tuple[bool, float]:
    raw = row.get("uplift_score")
    return (raw is not None, float(raw) if raw is not None else float("-inf"))


def apply_top_k_realtime_policy(
    scored_users: Sequence[Mapping[str, Any]],
    realtime_by_user: Mapping[str, Mapping[str, Any]],
    *,
    budget: int,
) -> dict[str, Any]:
    """Xep hang theo uplift roi gate top-K bang tin hieu realtime synthetic.

    Thu tu gate cho user da nam trong top-K:

    1. ``rt_order_1h > 0``: da mua, khong phat nua;
    2. ``rt_add_to_cart_1h > 0``: co intent, phat voucher;
    3. con lai: cho them intent.

    User score <= 0 khong duoc chon du con budget. ``budget`` la so slot xep
    hang, khong tu dong backfill user hang thap khi mot user top-K bi suppress;
    nhu vay demo nhin thay ro hai tang model-selection va timing-policy.
    """
    if budget < 1:
        raise ValueError("budget phai >= 1")
    if not scored_users:
        raise ValueError("can it nhat mot user")
    if budget > len(scored_users):
        raise ValueError("budget khong duoc lon hon so user")

    ids = [str(row["user_id"]) for row in scored_users]
    if len(ids) != len(set(ids)):
        raise ValueError("user_id trong campaign phai duy nhat")

    ranked = sorted(
        scored_users,
        key=_score_key,
        reverse=True,
    )
    results: list[dict[str, Any]] = []

    for rank, source in enumerate(ranked, start=1):
        user_id = str(source["user_id"])
        raw_score = source.get("uplift_score")
        score = None if raw_score is None else float(raw_score)
        realtime = dict(realtime_by_user.get(user_id, {}))
        selected = score is not None and score > 0 and rank <= budget

        if score is None:
            action = NO_MODEL_SCORE
            reason = "model khong tra duoc uplift score"
        elif score <= 0:
            action = SKIP_NON_POSITIVE_UPLIFT
            reason = "uplift khong duong"
        elif rank > budget:
            action = NOT_SELECTED_BUDGET
            reason = f"ngoai top-{budget} cua campaign"
        elif _number(realtime, "rt_order_1h") > 0:
            action = SUPPRESS_ALREADY_PURCHASED
            reason = "user da co order trong cua so 1h"
        elif _number(realtime, "rt_add_to_cart_1h") > 0:
            action = SEND_VOUCHER
            reason = "top-K va co add_to_cart intent"
        else:
            action = WAIT_FOR_INTENT
            reason = "top-K nhung chua co add_to_cart intent"

        results.append({
            "rank": rank,
            "user_id": user_id,
            "uplift_score": score,
            "uplift_percentage_point": (
                None if score is None else round(score * 100.0, 6)
            ),
            "selected_by_model": selected,
            "campaign_action": action,
            "reason_code": action,
            "reason": reason,
            "realtime": {
                name: realtime.get(name, 0)
                for name in (
                    "rt_events_1h", "rt_page_view_1h", "rt_add_to_cart_1h",
                    "rt_order_1h", "rt_gmv_1h", "rt_last_event_ts",
                )
            },
            "cache_hit": bool(source.get("cache_hit")),
            "features_supplied": int(source.get("features_supplied") or 0),
            "realtime_applied_to_model": int(source.get("realtime_applied") or 0),
        })

    selected_scores = [
        row["uplift_score"] for row in results if row["selected_by_model"]
    ]
    counts = Counter(row["campaign_action"] for row in results)
    return {
        "policy": {
            "type": "TOP_K_WITH_REALTIME_TIMING",
            "budget": budget,
            "positive_uplift_only": True,
            "effective_cutoff": min(selected_scores) if selected_scores else None,
            "synthetic_realtime": True,
            "model_score_uses_realtime": False,
        },
        "action_counts": dict(sorted(counts.items())),
        "results": results,
    }

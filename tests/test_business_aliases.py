from __future__ import annotations

from lzd_pipeline.features.business_aliases import load_business_aliases
from lzd_pipeline.reconstruction.feature_set import load_feature_set


def test_business_aliases_cover_every_selected_feature():
    fs = load_feature_set()
    aliases = load_business_aliases()

    assert aliases.feature_set_id == fs.id
    assert set(aliases.feature_aliases) == set(fs.columns)
    assert aliases.semantic_status == "SYNTHETIC_ASSUMPTION_NOT_LAZADA_FACT"


def test_business_aliases_do_not_claim_true_lazada_semantics():
    aliases = load_business_aliases()

    forbidden = {"FACT", "CONFIRMED", "LAZADA_FACT"}
    for item in aliases.feature_aliases.values():
        assert item.confidence.upper() not in forbidden
        assert item.alias.startswith("f") is False
        assert "lazada" not in item.alias.lower()


def test_track_a_event_aliases_make_witness_boundary_explicit():
    aliases = load_business_aliases()

    assert aliases.event_alias_of("EVT_ORDER_PAID") == "CFS_RECENCY_MARKER"
    assert "not" in aliases.event_note_of("EVT_ORDER_PAID").lower()
    assert aliases.event_alias_of("PRODUCT_VIEWED") == "PRODUCT_VIEWED"

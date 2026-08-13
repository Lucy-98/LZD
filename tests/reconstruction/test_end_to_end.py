"""Executable Track A -> feature SQL -> T0 -> Track B contract."""
from __future__ import annotations

from dataclasses import replace

import pytest

from lzd_pipeline.reconstruction.e2e import load_runtime_config, run_demo


@pytest.mark.parametrize("branch", ["H1", "H2"])
def test_both_semantic_scenarios_run_end_to_end(branch):
    config = replace(
        load_runtime_config(),
        semantic_status="UNIDENTIFIED",
        semantic_branch=branch,
    )
    result = run_demo(config)

    assert result.all_passed
    assert result.state.semantic_status == "UNIDENTIFIED"
    assert result.state.semantic_branch == branch
    assert result.gate_a.gate_a_pass
    assert result.gate_a.gate_a_t3_pass
    assert result.future_events
    assert all(event.event_ts >= result.state.as_of_ts for event in result.future_events)
    assert not (
        {event.event_id for event in result.outcome.events}
        & {event.event_id for event in result.future_events}
    )


def test_track_b_events_carry_synthetic_lineage():
    result = run_demo()
    event = result.future_events[0]

    assert event.provenance.source_type == "SYNTHETIC"
    assert event.provenance.parent_run_id == result.state.provenance.generation_run_id
    assert event.provenance.behaviour_policy_version == "rule_v1"
    assert event.generation_run_id


def test_track_b_run_id_depends_on_track_a_parent():
    config = load_runtime_config()
    h1 = run_demo(replace(config, semantic_branch="H1"))
    h2 = run_demo(replace(config, semantic_branch="H2"))
    assert h1.future_events[0].generation_run_id != h2.future_events[0].generation_run_id
    assert not (
        {event.event_id for event in h1.future_events}
        & {event.event_id for event in h2.future_events}
    )


def test_recency_round_trip_uses_same_calendar_day_semantics_as_dbt():
    result = run_demo()
    assert result.reconstructed["f1"] == result.target.values["f1"]
    assert result.reconstructed["f2"] == result.target.values["f2"]


def test_runtime_config_rejects_unknown_semantic_branch():
    with pytest.raises(ValueError, match="semantic_branch"):
        replace(load_runtime_config(), semantic_branch="H3")

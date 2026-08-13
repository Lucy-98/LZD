from __future__ import annotations

import csv
import json

from lzd_pipeline.reconstruction.e2e import load_runtime_config, run_demo
from lzd_pipeline.reconstruction.snapshot import write_snapshot


def test_snapshot_writer_emits_review_artifacts(tmp_path):
    config = load_runtime_config()
    result = run_demo(config)

    manifest = write_snapshot(result, config, tmp_path)

    expected = {
        "README.md",
        "feature_pass.svg",
        "features_expected_actual.csv",
        "manifest.json",
        "report.html",
        "summary.json",
        "timeline.svg",
        "track_a_events.csv",
        "track_b_events.csv",
    }
    assert expected == {p.name for p in tmp_path.iterdir()}
    assert manifest["selected_feature_count"] == 36
    assert manifest["summary"]["all_passed"] is True
    assert "manifest.json" not in manifest["files"]

    stored_manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert stored_manifest == manifest
    assert set(stored_manifest["files"]) == expected - {"manifest.json"}

    with (tmp_path / "features_expected_actual.csv").open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 36
    assert {row["ok"] for row in rows} == {"true"}
    assert {row["method"] for row in rows} == {"event_reconstructed", "pass_through"}
    assert "business_alias" in rows[0]
    assert "semantic_confidence" in rows[0]
    assert rows[0]["semantic_confidence"] == "SYNTHETIC_ASSUMPTION"

    with (tmp_path / "track_a_events.csv").open(encoding="utf-8") as fh:
        track_a = list(csv.DictReader(fh))
    assert track_a
    assert {row["event_family"] for row in track_a} == {"CFS_WITNESS"}
    assert "CFS_" in track_a[0]["event_alias"]

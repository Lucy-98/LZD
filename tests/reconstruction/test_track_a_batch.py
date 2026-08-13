from __future__ import annotations

import csv
from dataclasses import replace
from pathlib import Path

from lzd_pipeline.reconstruction.e2e import load_runtime_config
from lzd_pipeline.reconstruction.feature_set import load_feature_set
from lzd_pipeline.reconstruction.semantics import DecodedTarget, build_branch
from lzd_pipeline.reconstruction.track_a_batch import (
    construct_h1_candidate,
    materialize_track_a,
)


def test_h1_feasibility_counts_active_days_inside_window_only():
    decoded = DecodedTarget(n5=1, n11=1, n18=1, n30=5, d1=365, d2=365)
    candidate = construct_h1_candidate(decoded)

    assert candidate.active_days > decoded.n30
    assert candidate.active_days_in_window(decoded.window_days) == decoded.n30
    assert build_branch("H1").is_feasible(candidate, decoded)


def _fixture_row() -> dict[str, str]:
    row = {
        "data_id": "train_fixture_0",
        "label": "0",
        "is_treat": "0",
        **{f"f{i}": "0.0" for i in range(83)},
    }
    row.update({
        "f1": "365",
        "f2": "365",
        "f5": "0.0",
        "f11": "0.0",
        "f18": "0.0",
        "f30": "0.69897",
        "f37": "0.140481",
        "f38": "0.286151",
        "f79": "0.16917463",
        "f80": "0.04572446",
        "f81": "0.08180537",
        "f82": "0.076189525",
        "f40": "1.0",
        "f41": "0.0",
        "f42": "0.0",
        "f43": "1.0",
        **{f"f{i}": "0.0" for i in range(44, 53)},
        "f63": "0.0",
        "f64": "1.0",
        "f65": "0.0",
        "f68": "1.0",
        "f78": "0.0",
    })
    for index, column in enumerate(load_feature_set().tiers["T3"]):
        row[column] = str((index + 1) / 100)
    return row


def test_track_a_batch_materializes_real_csv_shape(tmp_path):
    path = tmp_path / "train.csv"
    row = _fixture_row()
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)

    config = replace(load_runtime_config(), semantic_branch="H1")
    manifest = materialize_track_a(
        input_path=path,
        output_dir=tmp_path / "out",
        limit=None,
        config=config,
        verify_limit=1,
    )

    assert manifest["processed_rows"] == 1
    assert manifest["solved_rows"] == 1
    assert manifest["quarantined_rows"] == 0
    assert manifest["raw_events"] == 6
    assert manifest["gate_sample"]["all_passed"] is True

    raw_path = Path(manifest["files"]["raw_events_v2"])
    with raw_path.open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert {row["source_type"] for row in rows} == {"RECONSTRUCTED"}
    assert "gen_reason" not in rows[0]

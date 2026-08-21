import csv
from dataclasses import replace
from pathlib import Path

import pytest

from lzd_pipeline.reconstruction.constructive import solve_h1
from lzd_pipeline.reconstruction.e2e import load_runtime_config
from lzd_pipeline.reconstruction.feature_set import load_feature_set
from lzd_pipeline.reconstruction.semantics import DecodedTarget, build_branch
from lzd_pipeline.reconstruction.track_a_batch import materialize_track_a


def test_h1_feasibility_counts_active_days_inside_window_only():
    decoded = DecodedTarget(n5=1, n11=1, n18=1, n19=1, n30=5, d1=365, d2=365)
    candidate = solve_h1(decoded, target_id="T", seed=42)

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
        "f19": "0.0",
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
        # g3 (f53..f62) — group MOI o scope v2, van phai one-hot hop le
        "f53": "1.0",
        **{f"f{i}": "0.0" for i in range(54, 63)},
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
    assert manifest["raw_events"] == 2
    assert manifest["gate_sample"]["all_passed"] is True

    raw_path = Path(manifest["files"]["raw_events_v2"])
    with raw_path.open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert {row["source_type"] for row in rows} == {"RECONSTRUCTED"}
    assert "gen_reason" not in rows[0]


def test_track_a_batch_fails_clearly_on_git_lfs_stub(tmp_path):
    stub_file = tmp_path / "full_trainset.csv"
    stub_file.write_text(
        "version https://git-lfs.github.com/spec/v1\n"
        "oid sha256:79cb6e5c2fb7ad0c35710a84ed82275baa167647b17bf3423f402602b02b5bf0\n"
        "size 657193635\n",
        encoding="utf-8",
    )
    config = replace(load_runtime_config(), semantic_branch="H1")
    with pytest.raises(RuntimeError, match="Git LFS pointer stub"):
        materialize_track_a(
            input_path=stub_file,
            output_dir=tmp_path / "out",
            limit=10,
            config=config,
            verify_limit=0,
        )


def test_track_a_batch_fails_on_missing_file(tmp_path):
    missing_file = tmp_path / "non_existent.csv"
    config = replace(load_runtime_config(), semantic_branch="H1")
    with pytest.raises(FileNotFoundError, match="Khong tim thay"):
        materialize_track_a(
            input_path=missing_file,
            output_dir=tmp_path / "out",
            limit=10,
            config=config,
            verify_limit=0,
        )


def test_track_a_batch_fails_on_missing_required_column(tmp_path):
    path = tmp_path / "bad_train.csv"
    row = _fixture_row()
    del row["f79"]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)

    config = replace(load_runtime_config(), semantic_branch="H1")
    with pytest.raises(KeyError, match="f79"):
        materialize_track_a(
            input_path=path,
            output_dir=tmp_path / "out",
            limit=10,
            config=config,
            verify_limit=0,
        )

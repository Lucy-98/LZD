"""Contract tests for the immutable DRLearner LightGBM 30F handoff."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from lzd_pipeline.reconstruction.feature_set import load_feature_set
from lzd_pipeline.serving.feature_contract import build_row, load_contract

try:
    import lightgbm  # noqa: F401
except (ImportError, OSError) as exc:
    pytest.skip(f"can lightgbm de nap booster: {exc}", allow_module_level=True)

ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = ROOT / "models" / "uplift_voucher_30f"


@pytest.fixture(scope="module")
def contract():
    return load_contract(MODEL_DIR / "feature_contract.json")


@pytest.fixture(scope="module")
def model():
    from lzd_pipeline.serving.model_loader import LightGBMUpliftModel

    return LightGBMUpliftModel(str(MODEL_DIR)).load()


def test_contract_matches_reconstruction_scope(contract):
    fs = load_feature_set()
    assert len(contract.order) == 30
    assert contract.order == (
        "f0", "f1", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10",
        "f13", "f16", "f17", "f20", "f21", "f22", "f23", "f25", "f26", "f27",
        "f28", "f29", "f35", "f38", "f42", "f52", "f60", "f68", "f80", "f82",
    )
    assert set(contract.order) == set(fs.columns)
    assert contract.redis_columns == contract.order
    assert not contract.derived_columns
    assert not contract.default_columns


def test_defaults_and_order_come_from_handoff(contract):
    vector = build_row(contract, {})
    assert len(vector) == 30
    assert vector[contract.slot_of("f1").index] == pytest.approx(172.0)
    assert vector[contract.slot_of("f27").index] == pytest.approx(30.0)
    assert vector[contract.slot_of("f82").index] == pytest.approx(0.6489617228507996)


def test_booster_and_manifest_are_valid(model, contract):
    manifest = json.loads((MODEL_DIR / "import_manifest.json").read_text())
    assert manifest["source_commit"] == "575fb1e0ea78ba0c9cb11876aac5984125308698"
    assert manifest["decision_threshold"] == pytest.approx(0.0070347543)
    assert manifest["sentinel"]["expected_uplift"] == pytest.approx(0.004436201035688247)
    assert model.booster.num_feature() == len(contract.order) == 30
    assert model.is_configured


def test_prediction_is_finite_and_deterministic(model, contract):
    row = {name: contract.defaults[name] for name in contract.order}
    first = model.predict_uplift([row])
    second = model.predict_uplift([row])
    assert first == second
    assert len(first) == 1
    assert isinstance(first[0], float)


def test_empty_batch_is_empty(model):
    assert model.predict_uplift([]) == []

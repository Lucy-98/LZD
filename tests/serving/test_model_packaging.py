"""Fitness tests cho kien truc model bat bien theo Docker image."""
from __future__ import annotations

import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_image_copy_du_artifact_notebook():
    dockerfile = (ROOT / "docker/python-service/Dockerfile").read_text(encoding="utf-8")
    dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8")

    for name in ("model_booster.txt", "feature_contract.json", "metadata.json"):
        artifact = ROOT / "models/uplift_voucher" / name
        assert artifact.is_file() and artifact.stat().st_size > 0
        assert f"models/uplift_voucher/{name}" in dockerfile
        assert f"!models/uplift_voucher/{name}" in dockerignore


def test_compose_khong_mount_model_va_khong_co_registry():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    services = compose["services"]

    assert "mlflow" not in services
    inference = services["inference-api"]
    assert all("models" not in str(volume) for volume in inference.get("volumes", []))
    contract = json.loads(
        (ROOT / "models/uplift_voucher/feature_contract.json").read_text(encoding="utf-8")
    )
    assert str(contract["phien_ban"]) in str(inference["image"])

    common_env = compose.get("x-common-env", {})
    assert not any(str(key).startswith("MLFLOW_") for key in common_env)
    assert "MODEL_STAGE" not in inference.get("environment", {})


def test_weekly_training_runtime_da_duoc_go_bo():
    assert not (ROOT / "airflow/dags/dag_30_train_uplift_model.py").exists()
    assert not list((ROOT / "src/lzd_pipeline/training").glob("*.py"))
    assert not (ROOT / "dbt/models/marts/training_dataset.sql").exists()
    assert not (ROOT / "docker/mlflow/Dockerfile").exists()

    app_source = (ROOT / "src/lzd_pipeline/serving/app.py").read_text(encoding="utf-8")
    assert "/admin/reload-model" not in app_source

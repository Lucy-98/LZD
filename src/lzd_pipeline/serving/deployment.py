"""Controlled deployment of a verified MLflow Production artifact."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import shutil
import urllib.request
import uuid
from pathlib import Path
from typing import Any

from lzd_pipeline.common.config import get_settings
from lzd_pipeline.features.compatibility import check_model_feature_compatibility
from lzd_pipeline.features.online_store import OnlineFeatureStore
from lzd_pipeline.features.spec import load_feature_spec
from lzd_pipeline.serving.model_loader import LightGBMUpliftModel


class DeploymentError(RuntimeError):
    pass


def _download_alias(model_name: str, alias: str, destination: Path) -> tuple[str, Path]:
    import mlflow
    from mlflow.tracking import MlflowClient

    mlflow.set_tracking_uri(get_settings().mlflow_tracking_uri)
    version = MlflowClient().get_model_version_by_alias(model_name, alias)
    downloaded = mlflow.artifacts.download_artifacts(
        artifact_uri=version.source, dst_path=str(destination)
    )
    return str(version.version), Path(downloaded)


def _notify_reload(url: str, token: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url, method="POST", data=b"{}",
        headers={"Content-Type": "application/json", "X-Model-Reload-Token": token},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read())


def deploy_production_alias(
    *, root: Path, model_name: str = "uplift_voucher", alias: str = "Production",
    reload_url: str | None = None, reload_token: str | None = None,
) -> dict[str, Any]:
    """Stage, verify and atomically activate the registry alias artifact."""
    root = root.resolve()
    versions = root / "versions"
    versions.mkdir(parents=True, exist_ok=True)
    lock_handle = (root / ".deploy.lock").open("a+")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        lock_handle.close()
        raise DeploymentError("another model deployment is already running") from exc
    staging_parent = root / f".staging-{uuid.uuid4().hex}"
    staging_parent.mkdir()
    active_link = root / "active"
    previous_target = active_link.resolve() if active_link.is_symlink() else None
    swapped = False
    try:
        registry_version, downloaded = _download_alias(model_name, alias, staging_parent)
        model = LightGBMUpliftModel(str(downloaded)).load()
        store = OnlineFeatureStore()
        active_feature_version = store.get_active_version()
        if not active_feature_version:
            raise DeploymentError("Redis has no active feature version")
        spec = load_feature_spec()
        compatibility = check_model_feature_compatibility(
            model_features=model.feature_order,
            model_feature_spec_version=model.feature_spec_version,
            model_realtime_semantics_version=model.realtime_semantics_version,
            requires_realtime=model.requires_realtime,
            spec=spec,
            active_status=store.get_version_status(active_feature_version),
        )
        if not compatibility.compatible:
            raise DeploymentError("; ".join(compatibility.reasons))

        final_dir = versions / model.version
        if final_dir.exists():
            LightGBMUpliftModel(str(final_dir)).load()
            shutil.rmtree(staging_parent)
        else:
            os.replace(downloaded, final_dir)
            shutil.rmtree(staging_parent)
        temporary_link = root / f".active-{uuid.uuid4().hex}"
        temporary_link.symlink_to(final_dir)
        os.replace(temporary_link, active_link)
        swapped = True

        reload_result = None
        if reload_url:
            if not reload_token:
                raise DeploymentError("reload token is required")
            reload_result = _notify_reload(reload_url, reload_token)
            if reload_result.get("model_version") != model.version:
                raise DeploymentError("API reloaded a different model version")
        result = {
            "deployed": True, "registry_version": registry_version,
            "model_version": model.version, "feature_version": active_feature_version,
            "artifact_dir": str(final_dir), "reload": reload_result,
            **compatibility.as_dict(),
        }
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        lock_handle.close()
        return result
    except Exception:
        if swapped and previous_target is not None:
            rollback_link = root / f".rollback-{uuid.uuid4().hex}"
            rollback_link.symlink_to(previous_target)
            os.replace(rollback_link, active_link)
            if reload_url and reload_token:
                try:
                    _notify_reload(reload_url, reload_token)
                except Exception:
                    pass
        elif swapped and active_link.is_symlink():
            active_link.unlink()
        if staging_parent.exists():
            shutil.rmtree(staging_parent)
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        lock_handle.close()
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=os.environ.get("MODEL_DEPLOY_ROOT", "models/serving"))
    parser.add_argument("--model-name", default=os.environ.get("MODEL_NAME", "uplift_voucher"))
    parser.add_argument("--alias", default=os.environ.get("MODEL_STAGE", "Production"))
    parser.add_argument("--reload-url", default=os.environ.get("MODEL_RELOAD_URL"))
    args = parser.parse_args()
    print(json.dumps(deploy_production_alias(
        root=Path(args.root), model_name=args.model_name, alias=args.alias,
        reload_url=args.reload_url, reload_token=os.environ.get("MODEL_RELOAD_TOKEN"),
    ), indent=2))


if __name__ == "__main__":
    main()

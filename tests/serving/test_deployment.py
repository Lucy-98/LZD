from __future__ import annotations

from types import SimpleNamespace

import pytest

from lzd_pipeline.serving import deployment


class _Model:
    version = "model-v2"
    feature_order = ["f0"]
    feature_spec_version = "fs_2026_08_v4"
    realtime_semantics_version = "event_time_5m_1h_dedup_v1"
    requires_realtime = False

    def load(self):
        return self


class _Store:
    def get_active_version(self):
        return "v20260821"

    def get_version_status(self, version):
        return {"status": "ACTIVE"}


class _Report:
    compatible = True
    reasons = ()

    def as_dict(self):
        return {"compatible": True, "reasons": []}


def _wire(monkeypatch):
    def download(_name, _alias, destination):
        artifact = destination / "artifact"
        artifact.mkdir()
        (artifact / "verified").write_text("yes")
        return "2", artifact

    monkeypatch.setattr(deployment, "_download_alias", download)
    monkeypatch.setattr(deployment, "LightGBMUpliftModel", lambda _path: _Model())
    monkeypatch.setattr(deployment, "OnlineFeatureStore", _Store)
    monkeypatch.setattr(deployment, "load_feature_spec", lambda: SimpleNamespace())
    monkeypatch.setattr(
        deployment, "check_model_feature_compatibility", lambda **_kwargs: _Report()
    )


def test_deployment_atomically_points_active_to_verified_version(tmp_path, monkeypatch):
    _wire(monkeypatch)
    result = deployment.deploy_production_alias(root=tmp_path)
    assert result["deployed"]
    assert (tmp_path / "active").is_symlink()
    assert (tmp_path / "active").resolve() == tmp_path / "versions" / "model-v2"
    assert (tmp_path / "active" / "verified").read_text() == "yes"


def test_reload_mismatch_rolls_back_previous_symlink(tmp_path, monkeypatch):
    _wire(monkeypatch)
    previous = tmp_path / "versions" / "model-v1"
    previous.mkdir(parents=True)
    (tmp_path / "active").symlink_to(previous)
    monkeypatch.setattr(deployment, "_notify_reload", lambda *_args: {
        "model_version": "unexpected"
    })
    with pytest.raises(deployment.DeploymentError, match="different model"):
        deployment.deploy_production_alias(
            root=tmp_path, reload_url="http://api/reload", reload_token="secret"
        )
    assert (tmp_path / "active").resolve() == previous

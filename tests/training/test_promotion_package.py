from __future__ import annotations

import json

from lzd_pipeline.training.promote import _deployment_package_gate


class _Client:
    def __init__(self, manifest, contract_path):
        self.manifest = manifest
        self.contract_path = contract_path

    def download_artifacts(self, _run_id, path):
        if path.endswith("import_manifest.json"):
            return str(self.manifest)
        return str(self.contract_path)


def test_promotion_rejects_missing_contract_checksum(tmp_path):
    contract = tmp_path / "feature_contract.json"
    contract.write_text("{}")
    manifest = tmp_path / "import_manifest.json"
    manifest.write_text(json.dumps({"sha256": {}}))
    passed, reason = _deployment_package_gate(_Client(manifest, contract), "run-1")
    assert not passed
    assert "checksum" in reason

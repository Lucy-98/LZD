"""Machine-readable compatibility contract shared by sync, serving and deploy."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Sequence

from lzd_pipeline.features.spec import FeatureSpec

REALTIME_SEMANTICS_VERSION = "event_time_5m_1h_dedup_v1"


def feature_schema_hash(spec: FeatureSpec) -> str:
    payload = {
        "feature_spec_version": spec.offline.get("selected_feature_set_id")
        or f"feature_spec_v{spec.version}",
        "batch": [(feature.name, feature.dtype) for feature in spec.batch_features],
        "realtime": [(feature.name, feature.dtype) for feature in spec.realtime_features],
        "realtime_semantics_version": REALTIME_SEMANTICS_VERSION,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CompatibilityReport:
    compatible: bool
    reasons: tuple[str, ...]
    feature_spec_version: str
    schema_hash: str
    realtime_semantics_version: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "compatible": self.compatible,
            "reasons": list(self.reasons),
            "feature_spec_version": self.feature_spec_version,
            "schema_hash": self.schema_hash,
            "realtime_semantics_version": self.realtime_semantics_version,
        }


def check_model_feature_compatibility(
    *,
    model_features: Sequence[str],
    model_feature_spec_version: str,
    model_realtime_semantics_version: str,
    requires_realtime: bool,
    spec: FeatureSpec,
    active_status: dict[str, str],
) -> CompatibilityReport:
    spec_version = str(
        spec.offline.get("selected_feature_set_id") or f"feature_spec_v{spec.version}"
    )
    schema_hash = feature_schema_hash(spec)
    reasons: list[str] = []
    available = set(spec.all_names)
    missing = [name for name in model_features if name not in available]
    if missing:
        reasons.append(f"model features missing from spec: {missing[:10]}")
    if model_feature_spec_version != spec_version:
        reasons.append(
            f"model feature spec {model_feature_spec_version!r} != active {spec_version!r}"
        )
    if active_status.get("feature_spec_version") != spec_version:
        reasons.append("active Redis version lacks matching feature_spec_version")
    if active_status.get("schema_hash") != schema_hash:
        reasons.append("active Redis version lacks matching schema_hash")
    if requires_realtime:
        if model_realtime_semantics_version != REALTIME_SEMANTICS_VERSION:
            reasons.append("model realtime semantics are incompatible")
        if active_status.get("realtime_semantics_version") != REALTIME_SEMANTICS_VERSION:
            reasons.append("active Redis realtime semantics are incompatible")
        rt_names = set(spec.realtime_names)
        if not any(name in rt_names for name in model_features):
            reasons.append("model requires realtime but declares no realtime feature")
    return CompatibilityReport(
        compatible=not reasons,
        reasons=tuple(reasons),
        feature_spec_version=spec_version,
        schema_hash=schema_hash,
        realtime_semantics_version=REALTIME_SEMANTICS_VERSION,
    )

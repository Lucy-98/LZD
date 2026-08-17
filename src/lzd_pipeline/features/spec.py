"""Doc & validate feature_spec.yml.

Spec la HOP DONG giua 3 ben:
  - dbt   : mart phai co dung cac cot nay
  - sync  : ghi dung cac field nay len Redis
  - API   : doc dung cac field nay, thieu thi dung default

Nho vay khong bi offline/online serving skew kieu "mart co f42, Redis lai thieu".
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Literal

import yaml

from lzd_pipeline.common.config import get_settings

DType = Literal["float", "int", "str"]


@dataclass(frozen=True)
class Feature:
    name: str
    dtype: DType
    default: Any
    group: str = "default"
    description: str = ""
    source: Literal["batch", "realtime"] = "batch"

    def cast(self, raw: Any) -> Any:
        """Ep kieu gia tri doc tu Redis (luon la str) ve dung dtype."""
        if raw is None or raw == "":
            return self.default
        try:
            if self.dtype == "float":
                return float(raw)
            if self.dtype == "int":
                return int(float(raw))
            return str(raw)
        except (TypeError, ValueError):
            return self.default


@dataclass(frozen=True)
class FeatureSpec:
    version: int
    entity: str
    entity_key: str
    batch_features: tuple[Feature, ...]
    realtime_features: tuple[Feature, ...]
    offline: dict[str, Any]
    online: dict[str, Any]
    quality: dict[str, Any]

    # ------------------------------------------------------------- helpers
    @property
    def all_features(self) -> tuple[Feature, ...]:
        return self.batch_features + self.realtime_features

    @property
    def batch_names(self) -> list[str]:
        return [f.name for f in self.batch_features]

    @property
    def realtime_names(self) -> list[str]:
        return [f.name for f in self.realtime_features]

    @property
    def all_names(self) -> list[str]:
        return [f.name for f in self.all_features]

    def by_name(self, name: str) -> Feature | None:
        for f in self.all_features:
            if f.name == name:
                return f
        return None

    def defaults(self) -> dict[str, Any]:
        return {f.name: f.default for f in self.all_features}

    # ------------------------------------------------------------ key gen
    def batch_key(self, version: str, user_id: str) -> str:
        return self.online["batch_key_template"].format(version=version, user_id=user_id)

    def realtime_key(self, user_id: str) -> str:
        return self.online["realtime_key_template"].format(user_id=user_id)

    @property
    def meta_prefix(self) -> str:
        return self.online.get("meta_prefix", "fs:meta")

    def meta_key(self, *parts: str) -> str:
        return ":".join([self.meta_prefix, *parts])

    # --------------------------------------------------------- validation
    def validate_columns(self, columns: list[str], scope: str = "batch") -> list[str]:
        """Tra ve danh sach cot BI THIEU so voi spec (rong = OK)."""
        expected = self.batch_names if scope == "batch" else self.all_names
        present = set(columns)
        return [c for c in expected if c not in present]

    def merge(
        self, batch_values: dict[str, Any], realtime_values: dict[str, Any] | None = None
    ) -> tuple[dict[str, Any], int]:
        """Gop feature batch + realtime overlay theo dung thu tu uu tien.

        Realtime GHI DE batch (du lieu moi hon thang).
        Tra ve (feature_dict day du, so feature phai dung default).
        """
        realtime_values = realtime_values or {}
        out: dict[str, Any] = {}
        missing = 0
        for f in self.batch_features:
            raw = batch_values.get(f.name)
            if raw is None:
                missing += 1
            out[f.name] = f.cast(raw)
        for f in self.realtime_features:
            raw = realtime_values.get(f.name)
            if raw is None:
                missing += 1
            out[f.name] = f.cast(raw)
        return out, missing


# ===========================================================================
def _expand_entry(entry: dict[str, Any], source: str) -> list[Feature]:
    """1 entry trong yml co the la 1 feature hoac 1 range (f0..f82)."""
    if "range" in entry:
        r = entry["range"]
        return [
            Feature(
                name=f"{r['prefix']}{i}",
                dtype=r.get("dtype", "float"),
                default=r.get("default", 0.0),
                group=r.get("group", "default"),
                description=r.get("description", ""),
                source=source,  # type: ignore[arg-type]
            )
            for i in range(int(r["start"]), int(r["end"]) + 1)
        ]
    return [
        Feature(
            name=entry["name"],
            dtype=entry.get("dtype", "float"),
            default=entry.get("default", 0.0),
            group=entry.get("group", "default"),
            description=entry.get("description", ""),
            source=source,  # type: ignore[arg-type]
        )
    ]


@lru_cache(maxsize=4)
def load_feature_spec(path: str | None = None) -> FeatureSpec:
    path = path or get_settings().feature_store.spec_path
    with open(path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    batch: list[Feature] = []
    for entry in raw.get("batch_features", []):
        batch.extend(_expand_entry(entry, "batch"))

    realtime: list[Feature] = []
    for entry in raw.get("realtime_features", []):
        realtime.extend(_expand_entry(entry, "realtime"))

    names = [f.name for f in batch + realtime]
    duplicates = {n for n in names if names.count(n) > 1}
    if duplicates:
        raise ValueError(f"feature_spec.yml co ten feature trung nhau: {sorted(duplicates)}")

    return FeatureSpec(
        version=int(raw.get("version", 1)),
        entity=raw.get("entity", "user"),
        entity_key=raw.get("entity_key", "user_id"),
        batch_features=tuple(batch),
        realtime_features=tuple(realtime),
        offline=raw.get("offline", {}),
        online=raw.get("online", {}),
        quality=raw.get("quality", {}),
    )


if __name__ == "__main__":  # kiem tra nhanh: python -m lzd_pipeline.features.spec
    spec = load_feature_spec()
    print(f"spec v{spec.version} | entity={spec.entity}")
    print(f"  batch    : {len(spec.batch_features)} feature")
    print(f"  realtime : {len(spec.realtime_features)} feature")
    print(f"  tong     : {len(spec.all_features)}")
    print(f"  vd key   : {spec.batch_key('v20260805', 'U000123')}")

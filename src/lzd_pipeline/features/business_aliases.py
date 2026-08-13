"""Load synthetic business aliases for selected LZD features.

The aliases are a presentation layer. They make reports readable for the
voucher-uplift story, but they do not change dbt/Redis/model-facing feature
names and they do not claim the true Lazada/DESCN semantic of any ``f*`` column.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

DEFAULT_PATH = Path(__file__).resolve().parents[3] / "config" / "features" / "business_aliases.yml"


@dataclass(frozen=True)
class BusinessAlias:
    feature: str
    tier: str
    alias: str
    display_name: str
    business_role: str
    source_family: str
    confidence: str
    reconstruction_note: str


@dataclass(frozen=True)
class EventAlias:
    event_type: str
    alias: str
    note: str


@dataclass(frozen=True)
class BusinessAliasSpec:
    version: str
    feature_set_id: str
    semantic_status: str
    feature_aliases: dict[str, BusinessAlias]
    event_aliases: dict[str, EventAlias]

    def for_feature(self, feature: str) -> BusinessAlias | None:
        return self.feature_aliases.get(feature)

    def alias_of(self, feature: str) -> str:
        item = self.for_feature(feature)
        return item.alias if item else feature

    def display_name_of(self, feature: str) -> str:
        item = self.for_feature(feature)
        return item.display_name if item else feature

    def event_alias_of(self, event_type: str) -> str:
        item = self.event_aliases.get(event_type)
        return item.alias if item else event_type

    def event_note_of(self, event_type: str) -> str:
        item = self.event_aliases.get(event_type)
        return item.note if item else ""


def _as_alias(feature: str, raw: dict[str, Any]) -> BusinessAlias:
    return BusinessAlias(
        feature=feature,
        tier=str(raw["tier"]),
        alias=str(raw["alias"]),
        display_name=str(raw.get("display_name") or raw["alias"]),
        business_role=str(raw.get("business_role") or ""),
        source_family=str(raw.get("source_family") or ""),
        confidence=str(raw.get("confidence") or "SYNTHETIC_ASSUMPTION"),
        reconstruction_note=str(raw.get("reconstruction_note") or ""),
    )


def _as_event_alias(event_type: str, raw: dict[str, Any]) -> EventAlias:
    return EventAlias(
        event_type=event_type,
        alias=str(raw["alias"]),
        note=str(raw.get("note") or ""),
    )


@lru_cache(maxsize=4)
def load_business_aliases(path: str | Path = DEFAULT_PATH) -> BusinessAliasSpec:
    path = Path(path)
    with path.open(encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    features = {
        str(name): _as_alias(str(name), spec)
        for name, spec in raw.get("feature_aliases", {}).items()
    }
    events = {
        str(name): _as_event_alias(str(name), spec)
        for name, spec in raw.get("track_a_event_aliases", {}).items()
    }

    aliases = [item.alias for item in features.values()]
    duplicates = {alias for alias in aliases if aliases.count(alias) > 1}
    if duplicates:
        raise ValueError(f"business_aliases.yml has duplicate aliases: {sorted(duplicates)}")

    forbidden_confidence = {
        f.feature: f.confidence
        for f in features.values()
        if f.confidence.upper() in {"FACT", "CONFIRMED", "LAZADA_FACT"}
    }
    if forbidden_confidence:
        raise ValueError(
            "Business aliases must not claim true Lazada semantics: "
            f"{forbidden_confidence}"
        )

    return BusinessAliasSpec(
        version=str(raw["version"]),
        feature_set_id=str(raw["feature_set_id"]),
        semantic_status=str(raw.get("semantic_status") or "SYNTHETIC_ASSUMPTION"),
        feature_aliases=features,
        event_aliases=events,
    )

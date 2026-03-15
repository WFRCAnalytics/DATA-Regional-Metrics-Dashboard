from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class GeographyAreaConfig:
    geog_name: str
    query_fields: list[str] = field(default_factory=list)
    query: str = "1=1"

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> GeographyAreaConfig:
        return cls(
            geog_name=raw["geog_name"],
            query_fields=list(raw.get("query_fields", [])),
            query=raw.get("query", "1=1"),
        )


@dataclass(slots=True)
class MetricConfig:
    key: str
    name: str
    family: str
    item_id: str
    index: int = 0
    aggregation: str = "sum"
    query: str = "1=1"
    geog_fields: list[str] = field(default_factory=list)
    geog_areas: list[GeographyAreaConfig] = field(default_factory=list)
    key_field_pattern: str = ""
    out_field_pattern: str = ""
    weighted_field_pattern: str | None = None
    weighted_field_prefix: str | None = None

    @classmethod
    def from_dict(cls, key: str, raw: dict[str, Any]) -> MetricConfig:
        geog_areas = [GeographyAreaConfig.from_dict(item) for item in raw.get("geog_areas", [])]
        return cls(
            key=key,
            name=raw["name"],
            family=raw["family"],
            item_id=raw["item_id"],
            index=int(raw.get("index", 0)),
            aggregation=str(raw.get("aggregation", "sum")),
            query=str(raw.get("query", "1=1")),
            geog_fields=list(raw.get("geog_fields", [])),
            geog_areas=geog_areas,
            key_field_pattern=str(raw.get("key_field_pattern", "")),
            out_field_pattern=str(raw.get("out_field_pattern", "")),
            weighted_field_pattern=raw.get("weighted_field_pattern"),
            weighted_field_prefix=raw.get("weighted_field_prefix"),
        )


@dataclass(slots=True)
class BoundariesConfig:
    item_id: str
    index: int
    join_field: str
    drop_columns: list[str]


@dataclass(slots=True)
class OutputItemConfig:
    title: str
    service_name: str
    tags: list[str]
    existing_item_id: str = ""


@dataclass(slots=True)
class PublishConfig:
    boundaries: BoundariesConfig
    output_item: OutputItemConfig


@dataclass(slots=True)
class ArcGISSettings:
    url: str
    profile: str | None = None
    username: str | None = None
    password: str | None = None


@dataclass(slots=True)
class ProjectConfig:
    root: Path
    metrics: dict[str, MetricConfig]
    geography_groups: dict[str, list[GeographyAreaConfig]]
    publish: PublishConfig
    logging: dict[str, Any]

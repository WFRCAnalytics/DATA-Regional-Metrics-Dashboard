from __future__ import annotations

import os
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - dependency resolution happens outside the sandbox.
    load_dotenv = None

try:
    import yaml
except ImportError:  # pragma: no cover - dependency resolution happens outside the sandbox.
    yaml = None

from .models import (
    ArcGISSettings,
    BoundariesConfig,
    GeographyAreaConfig,
    MetricConfig,
    OutputItemConfig,
    ProjectConfig,
    PublishConfig,
)


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_environment(root: Path | None = None) -> None:
    if load_dotenv is None:
        return
    repo_root = root or project_root()
    load_dotenv(repo_root / ".env", override=False)


def config_dir(root: Path | None = None) -> Path:
    load_environment(root)
    repo_root = root or project_root()
    override = os.getenv("REGIONAL_METRICS_CONFIG_DIR")
    return Path(override) if override else repo_root / "config"


def load_yaml(path: Path) -> dict[str, Any]:
    if yaml is None:
        raise RuntimeError("PyYAML is required to load configuration files.")
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def load_arcgis_settings() -> ArcGISSettings:
    load_environment()
    return ArcGISSettings(
        url=os.getenv("ARCGIS_URL", "https://wfrc.maps.arcgis.com"),
        profile=os.getenv("ARCGIS_PROFILE") or None,
        username=os.getenv("ARCGIS_USERNAME") or None,
        password=os.getenv("ARCGIS_PASSWORD") or None,
    )


def publish_enabled() -> bool:
    load_environment()
    return os.getenv("ARCGIS_PUBLISH_ENABLED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _load_geography_groups(raw: dict[str, Any]) -> dict[str, list[GeographyAreaConfig]]:
    groups: dict[str, list[GeographyAreaConfig]] = {}
    for key, items in raw.get("groups", {}).items():
        groups[key] = [GeographyAreaConfig.from_dict(item) for item in items]
    return groups


def load_project_config(root: Path | None = None) -> ProjectConfig:
    repo_root = root or project_root()
    cfg_dir = config_dir(repo_root)

    datasets_doc = load_yaml(cfg_dir / "datasets.yml")
    geographies_doc = load_yaml(cfg_dir / "geographies.yml")
    publish_doc = load_yaml(cfg_dir / "publish.yml")
    logging_doc = load_yaml(cfg_dir / "logging.yml")

    geography_groups = _load_geography_groups(geographies_doc)
    metrics: dict[str, MetricConfig] = {}

    for key, raw_metric in datasets_doc.get("metrics", {}).items():
        metric_payload = dict(raw_metric)
        geography_group = metric_payload.pop("geography_group", None)
        if geography_group and "geog_areas" not in metric_payload:
            metric_payload["geog_areas"] = [
                {
                    "geog_name": area.geog_name,
                    "query_fields": area.query_fields,
                    "query": area.query,
                }
                for area in geography_groups[geography_group]
            ]
        metrics[key] = MetricConfig.from_dict(key, metric_payload)

    boundaries = publish_doc["boundaries"]
    output_item = publish_doc["output_item"]

    publish = PublishConfig(
        boundaries=BoundariesConfig(
            item_id=boundaries["item_id"],
            index=int(boundaries.get("index", 0)),
            join_field=boundaries["join_field"],
            drop_columns=list(boundaries.get("drop_columns", [])),
        ),
        output_item=OutputItemConfig(
            title=output_item["title"],
            service_name=output_item["service_name"],
            tags=list(output_item.get("tags", [])),
            existing_item_id=output_item.get("existing_item_id", ""),
        ),
    )

    return ProjectConfig(
        root=repo_root,
        metrics=metrics,
        geography_groups=geography_groups,
        publish=publish,
        logging=logging_doc,
    )

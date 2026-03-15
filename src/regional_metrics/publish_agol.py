from __future__ import annotations

from dataclasses import dataclass

try:
    from arcgis.features import FeatureSet
except ImportError:  # pragma: no cover - dependency resolution happens outside the sandbox.
    FeatureSet = None

from .validation import validate_columns


@dataclass(slots=True)
class PublishPlan:
    title: str
    existing_item_id: str
    tags: list[str]
    row_count: int
    column_count: int
    join_field: str
    geometry_column: str
    mode: str


def validate_publish_frame(frame, join_field: str = "geoname") -> None:
    validate_columns(frame, [join_field])
    geometry_name = getattr(frame, "geometry", None)
    if geometry_name is None and "SHAPE" not in frame.columns:
        raise ValueError("Publish frames must include geometry.")


def build_publish_plan(
    frame,
    title: str,
    tags: list[str],
    existing_item_id: str = "",
    join_field: str = "geoname",
) -> PublishPlan:
    validate_publish_frame(frame, join_field=join_field)
    geometry_name = getattr(frame, "geometry", None)
    geometry_column = geometry_name.name if geometry_name is not None else "SHAPE"
    mode = "overwrite" if existing_item_id else "create"

    return PublishPlan(
        title=title,
        existing_item_id=existing_item_id,
        tags=tags,
        row_count=len(frame.index),
        column_count=len(frame.columns),
        join_field=join_field,
        geometry_column=geometry_column,
        mode=mode,
    )


def publish_geodataframe(frame, gis, title: str, tags: list[str], existing_item_id: str = ""):
    if FeatureSet is None:
        raise RuntimeError("arcgis is required for hosted layer publishing.")

    validate_publish_frame(frame)
    sedf = FeatureSet.from_geodataframe(frame)
    tag_string = ", ".join(tags)

    if existing_item_id:
        item = gis.content.get(existing_item_id)
        return sedf.spatial.to_featurelayer(
            title,
            gis=gis,
            tags=tag_string,
            overwrite=True,
            service={"featureServiceId": item.id, "layer": 0},
            sanitize_columns=False,
        )

    return sedf.spatial.to_featurelayer(
        title,
        gis=gis,
        tags=tag_string,
        sanitize_columns=False,
    )

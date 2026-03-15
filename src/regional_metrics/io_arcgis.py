from __future__ import annotations

from typing import Any

import pandas as pd

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover - dependency resolution happens outside the sandbox.
    gpd = None

try:
    from arcgis.gis import GIS
except ImportError:  # pragma: no cover - dependency resolution happens outside the sandbox.
    GIS = None

try:
    from shapely.geometry import LineString, MultiLineString, MultiPoint, Point, Polygon
except ImportError:  # pragma: no cover - dependency resolution happens outside the sandbox.
    LineString = MultiLineString = MultiPoint = Point = Polygon = None

from .models import ArcGISSettings


def connect_gis(settings: ArcGISSettings):
    if GIS is None:
        raise RuntimeError("arcgis is required for ArcGIS service access.")
    if settings.profile:
        return GIS(settings.url, profile=settings.profile)
    if settings.username and settings.password:
        return GIS(settings.url, settings.username, settings.password)
    raise RuntimeError(
        "ArcGIS credentials are missing. Set ARCGIS_PROFILE or ARCGIS_USERNAME/ARCGIS_PASSWORD."
    )


def get_item_layer(gis, item_id: str, index: int = 0):
    item = gis.content.get(item_id)
    layers = item.layers or item.tables
    if not layers:
        raise ValueError(f"Item {item_id} does not expose layers or tables.")
    return layers[index]


def feature_layer_to_frame(layer, where: str = "1=1"):
    feature_set = layer.query(where=where, out_fields="*", return_geometry=True)
    records: list[dict[str, Any]] = []
    geometries: list[Any] = []

    for feature in feature_set.features:
        records.append(dict(feature.attributes))
        geometries.append(_arcgis_geometry_to_shapely(feature.geometry))

    frame = pd.DataFrame(records)
    if gpd is not None and any(geometry is not None for geometry in geometries):
        return gpd.GeoDataFrame(frame, geometry=geometries, crs=_crs_from_feature_set(feature_set))
    return frame


def _crs_from_feature_set(feature_set) -> str | None:
    spatial_reference = getattr(feature_set, "spatial_reference", None) or {}
    wkid = spatial_reference.get("latestWkid") or spatial_reference.get("wkid")
    return f"EPSG:{wkid}" if wkid else None


def _arcgis_geometry_to_shapely(geometry):
    if not geometry or Point is None:
        return None
    if "x" in geometry and "y" in geometry:
        return Point(geometry["x"], geometry["y"])
    if "points" in geometry:
        return MultiPoint(geometry["points"])
    if "paths" in geometry:
        paths = geometry["paths"]
        if len(paths) == 1:
            return LineString(paths[0])
        return MultiLineString(paths)
    if "rings" in geometry:
        rings = geometry["rings"]
        shell = rings[0]
        holes = rings[1:] if len(rings) > 1 else None
        return Polygon(shell=shell, holes=holes)
    return None

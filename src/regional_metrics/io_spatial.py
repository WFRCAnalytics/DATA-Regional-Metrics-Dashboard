from __future__ import annotations

from pathlib import Path

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover - dependency resolution happens outside the sandbox.
    gpd = None


def require_geopandas():
    if gpd is None:
        raise RuntimeError("geopandas is required for local spatial I/O.")
    return gpd


def read_spatial(path: str | Path):
    spatial = require_geopandas()
    source = Path(path)
    if source.suffix == ".parquet":
        return spatial.read_parquet(source)
    return spatial.read_file(source)


def write_spatial(frame, path: str | Path) -> Path:
    spatial = require_geopandas()
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.suffix == ".parquet":
        frame.to_parquet(destination, index=False)
        return destination

    driver = "GPKG" if destination.suffix == ".gpkg" else None
    if not isinstance(frame, spatial.GeoDataFrame):
        raise TypeError("Spatial outputs require a GeoDataFrame.")
    frame.to_file(destination, driver=driver)
    return destination


def ensure_analysis_crs(frame, epsg: int = 26912):
    spatial = require_geopandas()
    if not isinstance(frame, spatial.GeoDataFrame):
        return frame
    if frame.crs is None:
        return frame.set_crs(epsg=epsg)
    if frame.crs.to_epsg() == epsg:
        return frame
    return frame.to_crs(epsg=epsg)

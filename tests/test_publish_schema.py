import pandas as pd
import pytest

try:
    import geopandas as gpd
    from shapely.geometry import Point
except ImportError:  # pragma: no cover - test guard for optional spatial deps.
    gpd = None
    Point = None

from regional_metrics.publish_agol import build_publish_plan, validate_publish_frame

pytestmark = pytest.mark.skipif(gpd is None, reason="geopandas is not installed")


def test_validate_publish_frame_requires_geometry():
    frame = pd.DataFrame({"geoname": ["Alpha"]})
    with pytest.raises(ValueError):
        validate_publish_frame(frame)


def test_build_publish_plan_for_geodataframe():
    frame = gpd.GeoDataFrame(
        {"geoname": ["Alpha"], "value": [1], "geometry": [Point(0, 0)]},
        geometry="geometry",
        crs="EPSG:4326",
    )

    plan = build_publish_plan(
        frame,
        title="WFRC_PerformanceMetrics",
        tags=["wfrc", "metrics"],
        existing_item_id="abc123",
    )

    assert plan.title == "WFRC_PerformanceMetrics"
    assert plan.mode == "overwrite"
    assert plan.row_count == 1
    assert plan.geometry_column == "geometry"

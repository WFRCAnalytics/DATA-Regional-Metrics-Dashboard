import pandas as pd
import pytest

try:
    import geopandas as gpd
    from shapely.geometry import Point, Polygon
except ImportError:  # pragma: no cover - test guard for optional spatial deps.
    gpd = None
    Point = Polygon = None

from regional_metrics.metrics_ht import build_city_cost_layer, summarize_ht_by_geography

pytestmark = pytest.mark.skipif(gpd is None, reason="geopandas is not installed")


def test_build_city_cost_layer():
    city_areas = gpd.GeoDataFrame(
        {"CITY_NAME": ["Alpha"], "geometry": [Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])]},
        geometry="geometry",
        crs="EPSG:26912",
    )
    costs = pd.DataFrame({"City Area": ["Alpha"], "HPLUST2019": [45.0]})
    result = build_city_cost_layer(city_areas, costs)
    assert "HPLUST2019" in result.columns
    assert result.iloc[0]["CITYAREA"] == "Alpha"


def test_summarize_ht_by_geography():
    geography = gpd.GeoDataFrame(
        {"GEONAME": ["Alpha"], "geometry": [Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])]},
        geometry="geometry",
        crs="EPSG:26912",
    )
    block_groups = gpd.GeoDataFrame(
        {
            "h_ami": [10.0, 20.0],
            "t_ami": [5.0, 15.0],
            "geometry": [Point(0.5, 0.5), Point(1.5, 1.5)],
        },
        geometry="geometry",
        crs="EPSG:26912",
    )
    result = summarize_ht_by_geography(geography, block_groups, ["h_ami", "t_ami"])
    assert result.iloc[0]["h_ami"] == 15.0
    assert result.iloc[0]["t_ami"] == 10.0

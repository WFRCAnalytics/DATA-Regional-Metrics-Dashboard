from __future__ import annotations

from pathlib import Path

import pandas as pd

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover - dependency resolution happens outside the sandbox.
    gpd = None

from .metrics_projections import aggregate_metric_table
from .models import MetricConfig


def aggregate_ht_metric(metric: MetricConfig, source_df: pd.DataFrame) -> pd.DataFrame:
    return aggregate_metric_table(metric, source_df)


def build_city_cost_layer(city_areas, cost_table: pd.DataFrame, city_name_field: str = "CITY_NAME"):
    _require_geopandas()

    working = city_areas.copy()
    costs = cost_table.copy()
    costs.rename(columns={"City Area": "CITYAREA"}, inplace=True)
    working = working.rename(columns={city_name_field: "CITYAREA"})

    merged = working[["CITYAREA", working.geometry.name]].merge(costs, on="CITYAREA", how="left")
    return gpd.GeoDataFrame(merged, geometry=working.geometry.name, crs=city_areas.crs)


def summarize_ht_by_geography(
    geography,
    block_groups,
    value_fields: list[str],
    geoname_field: str = "GEONAME",
):
    _require_geopandas()

    joined = gpd.sjoin(
        block_groups,
        geography[[geoname_field, geography.geometry.name]],
        how="inner",
        predicate="intersects",
    )
    summary = joined.groupby(geoname_field)[value_fields].median().reset_index()
    geography_subset = geography[[geoname_field, geography.geometry.name]]
    return geography_subset.merge(summary, on=geoname_field, how="left")


def load_ht_cost_table(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(path)


def _require_geopandas():
    if gpd is None:
        raise RuntimeError("geopandas is required for housing and transportation workflows.")

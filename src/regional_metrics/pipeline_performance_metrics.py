from __future__ import annotations

import pandas as pd

from . import (
    metrics_access,
    metrics_acs,
    metrics_centers,
    metrics_ht,
    metrics_jobs,
    metrics_projections,
)
from .io_arcgis import feature_layer_to_frame, get_item_layer
from .models import MetricConfig, ProjectConfig

FAMILY_DISPATCH = {
    "weighted_jobs": metrics_jobs.metric_jobs_by,
    "projections": metrics_projections.aggregate_metric_table,
    "access": metrics_access.aggregate_access_metric,
    "centers": metrics_centers.aggregate_centers_metric,
    "ht": metrics_ht.aggregate_ht_metric,
    "acs": metrics_acs.aggregate_acs_metric,
}


def build_performance_metrics(
    config: ProjectConfig,
    gis,
    selected_metrics: list[str] | None = None,
):
    metric_keys = selected_metrics or list(config.metrics.keys())
    output_df = pd.DataFrame()

    for metric_key in metric_keys:
        metric = config.metrics[metric_key]
        source_df = fetch_metric_frame(gis, metric)
        metric_df = dispatch_metric(metric, source_df)
        output_df = merge_metric_dataframes(output_df, metric_df)

    boundaries = config.publish.boundaries
    boundaries_layer = get_item_layer(gis, boundaries.item_id, boundaries.index)
    boundaries_df = feature_layer_to_frame(boundaries_layer)
    merged_df = boundaries_df.merge(
        output_df,
        how="inner",
        left_on=boundaries.join_field,
        right_on="geoname",
    )
    drop_columns = [column for column in boundaries.drop_columns if column in merged_df.columns]
    if drop_columns:
        merged_df = merged_df.drop(columns=drop_columns)
    return merged_df


def fetch_metric_frame(gis, metric: MetricConfig):
    layer = get_item_layer(gis, metric.item_id, metric.index)
    return feature_layer_to_frame(layer, where=metric.query)


def dispatch_metric(metric: MetricConfig, source_df: pd.DataFrame) -> pd.DataFrame:
    if metric.family not in FAMILY_DISPATCH:
        raise KeyError(f"Unsupported metric family: {metric.family}")
    return FAMILY_DISPATCH[metric.family](metric, source_df)


def merge_metric_dataframes(output_df: pd.DataFrame, metric_df: pd.DataFrame) -> pd.DataFrame:
    if output_df.empty:
        return metric_df
    return output_df.merge(metric_df, how="outer", on="geoname")

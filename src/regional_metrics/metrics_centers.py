from __future__ import annotations

import pandas as pd

from .metrics_projections import aggregate_metric_table
from .models import MetricConfig


def aggregate_centers_metric(metric: MetricConfig, source_df: pd.DataFrame) -> pd.DataFrame:
    return aggregate_metric_table(metric, source_df)

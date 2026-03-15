import pandas as pd

from regional_metrics.metrics_jobs import metric_jobs_by
from regional_metrics.models import GeographyAreaConfig, MetricConfig


def test_metric_jobs_by_weighting():
    metric = MetricConfig(
        key="jobs_by_auto",
        name="Jobs By Auto",
        family="weighted_jobs",
        item_id="test",
        geog_fields=["CITYAREA"],
        geog_areas=[
            GeographyAreaConfig(
                geog_name="Region",
                query_fields=["CO_FIPS"],
                query="CO_FIPS == 1",
            )
        ],
        key_field_pattern=r"^JOBAUTO_[0-9]{2}$",
        weighted_field_pattern=r"^HH_[0-9]{2}$",
        weighted_field_prefix="HH_",
        out_field_pattern="weighted_ato_jobauto_",
    )
    frame = pd.DataFrame(
        {
            "CITYAREA": ["Alpha", "Alpha"],
            "CO_FIPS": [1, 1],
            "JOBAUTO_19": [100.0, 200.0],
            "JOBAUTO_20": [200.0, 100.0],
            "HH_19": [1.0, 3.0],
            "HH_20": [2.0, 2.0],
        }
    )

    result = metric_jobs_by(metric, frame)
    alpha = result[result["geoname"] == "Alpha"].iloc[0]
    assert alpha["weighted_ato_jobauto_CY"] == 175.0
    assert alpha["weighted_ato_jobauto_FY1"] == 150.0

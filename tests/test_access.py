import pandas as pd

from regional_metrics.metrics_access import aggregate_access_metric
from regional_metrics.models import GeographyAreaConfig, MetricConfig


def test_aggregate_access_metric():
    metric = MetricConfig(
        key="households_with_access_to_transit",
        name="Households with Access to Transit",
        family="access",
        item_id="test",
        geog_fields=["CITYAREA"],
        geog_areas=[
            GeographyAreaConfig(
                geog_name="Region",
                query_fields=["CO_FIPS"],
                query="CO_FIPS == 1",
            )
        ],
        key_field_pattern=r"^HH_[0-9]{4}$",
        out_field_pattern="hh_20min_walk_transit_",
    )
    frame = pd.DataFrame(
        {
            "CITYAREA": ["Alpha", "Alpha", "Beta"],
            "CO_FIPS": [1, 1, 2],
            "HH_2019": [10, 20, 30],
            "HH_2020": [20, 30, 40],
        }
    )

    result = aggregate_access_metric(metric, frame)
    alpha = result[result["geoname"] == "Alpha"].iloc[0]
    region = result[result["geoname"] == "Region"].iloc[0]
    assert alpha["hh_20min_walk_transit_2019"] == 30
    assert region["hh_20min_walk_transit_2020"] == 50

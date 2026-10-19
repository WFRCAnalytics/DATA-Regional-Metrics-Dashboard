from pathlib import Path

import pytest

from regional_metrics.models import HTIncomeConfig


@pytest.fixture
def ht_config(tmp_path: Path) -> HTIncomeConfig:
    return HTIncomeConfig.from_dict(
        {
            "state": "UT",
            "state_fips": "49",
            "county_fips": ["011", "035"],
            "acs_dataset": "acs/acs5",
            "acs_vars": {
                "median_income": "B19013_001E",
                "population": "B02001_001E",
                "households": "B11001_001E",
            },
            "acs_start_year": 2019,
            "bg_reference_year": 2020,
            "city_key": "CITYAREA",
            "year_prefixes": ["HCOST", "TCOST", "HPLUST"],
            "county_name_to_fips": {"Davis": "49011", "Salt Lake": "49035"},
            "county_tdm_rows": {"Davis County": "49011"},
            "workshop_area_lookup": {"North Davis County": "Davis County North"},
            "paths": {},
        },
        tmp_path,
    )

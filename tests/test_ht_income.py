import numpy as np
import pandas as pd
import pytest

try:
    import geopandas as gpd
    from shapely.geometry import Polygon
except ImportError:  # pragma: no cover - test guard for optional spatial deps.
    gpd = None

from regional_metrics.ht_income import (
    aggregate_city_income,
    assign_block_groups_to_cities,
    build_export_frame,
    build_hybrid_medinc,
    compute_ht_income_share,
    derive_years,
    load_ht_costs,
)
from regional_metrics.settings import load_ht_income_config
from regional_metrics.validation import compare_to_baseline

needs_geopandas = pytest.mark.skipif(gpd is None, reason="geopandas is not installed")


def _square(x0, y0, size=10):
    return Polygon([(x0, y0), (x0 + size, y0), (x0 + size, y0 + size), (x0, y0 + size)])


def test_load_ht_costs_normalizes_sentinels_and_maps_county(ht_config, tmp_path):
    path = tmp_path / "costs.csv"
    path.write_text(
        "City Area,County,HCOST2019,TCOST2019,HPLUST2019\n"
        'Alpha,Davis,"1,200",300,-1\n'
        "Beta,Salt Lake,900,-1,0.4\n",
        encoding="utf-8",
    )
    costs = load_ht_costs(path, ht_config)
    assert "City Area" not in costs.columns
    assert costs["CITYAREA"].tolist() == ["Alpha", "Beta"]
    assert costs["HCOST2019"].tolist() == [1200, 900]  # thousands separator parsed
    assert costs["TCOST2019"].tolist() == [300, 0]  # -1 sentinel -> 0
    assert costs["county_geoid"].tolist() == ["49011", "49035"]


def test_derive_years_spans_acs_start_to_latest_cost_year(ht_config):
    costs = pd.DataFrame(columns=["CITYAREA", "HCOST2020", "TCOST2021", "HPLUST2022", "OTHER2030"])
    target_years, acs_years = derive_years(costs, ht_config)
    assert target_years == [2020, 2021, 2022]
    assert acs_years == [2019, 2020, 2021, 2022]


@needs_geopandas
def test_assign_block_groups_uses_centroid_and_first_city_on_overlap(ht_config):
    cities = gpd.GeoDataFrame(
        {
            "CITY_NAME": ["West", "East", "Overlap"],
            "SUBAREA": ["North Davis County", None, None],
            "CO_NAME": ["davis", "davis", "davis"],
            "geometry": [_square(0, 0), _square(10, 0), _square(0, 0)],
        },
        crs="EPSG:26912",
    )
    block_groups = gpd.GeoDataFrame(
        {
            "GEOID": ["bg_west", "bg_east", "bg_straddle", "bg_outside"],
            "geometry": [
                _square(1, 1, 4),  # centroid in West (also in Overlap)
                _square(12, 1, 4),  # centroid in East
                _square(7, 1, 8),  # centroid (11, 5) in East even though it spans both
                _square(100, 100, 4),  # centroid in no city
            ],
        },
        crs="EPSG:26912",
    )

    lookup = assign_block_groups_to_cities(block_groups, cities, ht_config).set_index("GEOID")

    assert lookup.loc["bg_west", "CITYAREA"] == "West"  # first city in layer order wins
    assert lookup.loc["bg_east", "CITYAREA"] == "East"
    assert lookup.loc["bg_straddle", "CITYAREA"] == "East"
    assert pd.isna(lookup.loc["bg_outside", "CITYAREA"])
    assert lookup["CITYAREA"].index.is_unique
    assert lookup.loc["bg_west", "SUBAREA"] == "Davis County North"  # workshop rename
    assert lookup.loc["bg_west", "CO_NAME"] == "DAVIS"


@needs_geopandas
def test_assign_block_groups_reprojects_to_the_city_crs(ht_config):
    cities = gpd.GeoDataFrame(
        {
            "CITY_NAME": ["Alpha"],
            "SUBAREA": ["x"],
            "CO_NAME": ["davis"],
            "geometry": [_square(400000, 4500000, 10000)],
        },
        crs="EPSG:26912",
    )
    block_groups = gpd.GeoDataFrame(
        {"GEOID": ["bg1"], "geometry": [_square(400000, 4500000, 10000)]}, crs="EPSG:26912"
    ).to_crs("EPSG:4269")
    lookup = assign_block_groups_to_cities(block_groups, cities, ht_config)
    assert lookup["CITYAREA"].tolist() == ["Alpha"]


def _bg_income_panel(rows):
    return pd.DataFrame(
        rows, columns=["GEOID", "year", "median_income", "population", "households"]
    )


def test_aggregate_city_income_is_household_weighted_and_skips_unusable_rows(ht_config):
    panel = _bg_income_panel(
        [
            ("a", 2020, 100.0, 10.0, 1.0),
            ("b", 2020, 200.0, 30.0, 3.0),  # weighted mean = (100 + 600) / 4 = 175
            ("c", 2020, np.nan, 99.0, 5.0),  # no income -> excluded from every sum
            ("d", 2020, 300.0, 50.0, 0.0),  # zero households -> excluded
            ("e", 2020, 400.0, 70.0, 7.0),  # not assigned to a city
        ]
    )
    lookup = pd.DataFrame({"GEOID": ["a", "b", "c", "d"], "CITYAREA": ["Alpha"] * 4})
    wide = aggregate_city_income(panel, lookup, [2020], ht_config).set_index("CITYAREA")
    assert wide.loc["Alpha", "MEDINC2020"] == 175.0
    assert wide.loc["Alpha", "TOTHH2020"] == 4.0
    assert wide.loc["Alpha", "TOTPOP2020"] == 40.0


def test_aggregate_city_income_orders_columns_year_by_year(ht_config):
    panel = _bg_income_panel([("a", 2019, 1.0, 1.0, 1.0), ("a", 2020, 1.0, 1.0, 1.0)])
    lookup = pd.DataFrame({"GEOID": ["a"], "CITYAREA": ["Alpha"]})
    wide = aggregate_city_income(panel, lookup, [2019, 2020], ht_config)
    assert list(wide.columns) == [
        "CITYAREA",
        "TOTPOP2019",
        "TOTHH2019",
        "MEDINC2019",
        "TOTPOP2020",
        "TOTHH2020",
        "MEDINC2020",
    ]


def _county_panel(ht_config, income_by_year):
    return pd.DataFrame(
        [
            {ht_config.county_key: fips, "year": year, "median_income": income}
            for fips, by_year in income_by_year.items()
            for year, income in by_year.items()
        ]
    )


def test_hybrid_medinc_priority_is_place_then_county_tdm_row_then_block_groups(ht_config):
    city_income = pd.DataFrame(
        {
            "CITYAREA": ["Alpha", "Davis County", "Gamma", "Delta"],
            "MEDINC2020": [150.0, 150.0, 150.0, np.nan],
        }
    )
    place_panel = pd.DataFrame(
        {
            "place_name": ["Alpha", "Davis County"],
            "year": [2020, 2020],
            "place_medinc": [999.0, 888.0],
        }
    )
    county_panel = _county_panel(ht_config, {"49011": {2020: 500.0}})

    hybrid, sources = build_hybrid_medinc(city_income, place_panel, county_panel, [2020], ht_config)

    medinc = hybrid.set_index("CITYAREA")["MEDINC2020_CITY"]
    assert medinc["Alpha"] == 999.0  # place beats block groups
    assert medinc["Davis County"] == 888.0  # place beats county too
    assert medinc["Gamma"] == 150.0  # no place, not a county TDM row -> block groups
    assert np.isnan(medinc["Delta"])
    assert sources.set_index("CITYAREA")["medinc_source"].to_dict() == {
        "Alpha": "acs_place",
        "Davis County": "acs_place",
        "Gamma": "bg_weighted",
        "Delta": "none",
    }
    assert not any(c.startswith(("place_medinc_", "county_medinc_")) for c in hybrid.columns)


def test_hybrid_medinc_uses_county_for_whole_county_tdm_rows_without_a_place(ht_config):
    city_income = pd.DataFrame({"CITYAREA": ["Davis County"], "MEDINC2020": [150.0]})
    place_panel = pd.DataFrame(columns=["place_name", "year", "place_medinc"])
    place_panel["year"] = place_panel["year"].astype(int)
    county_panel = _county_panel(ht_config, {"49011": {2020: 500.0}})
    hybrid, sources = build_hybrid_medinc(city_income, place_panel, county_panel, [2020], ht_config)
    assert hybrid["MEDINC2020_CITY"].tolist() == [500.0]
    assert sources["medinc_source"].tolist() == ["acs_county"]


def test_ht_income_share_is_normalized_by_county_income(ht_config):
    costs = pd.DataFrame(
        {
            "CITYAREA": ["Alpha", "Beta", "Gamma"],
            "county_geoid": ["49011", "49035", "49049"],  # Gamma's county has no income
            "HCOST2020": [1000.0, 800.0, 500.0],
            "TCOST2020": [500.0, 400.0, 100.0],
            "HPLUST2020": [9.0, 9.0, 9.0],  # stale value from the input CSV is overwritten
        }
    )
    hybrid = pd.DataFrame(
        {"CITYAREA": ["Alpha", "Beta", "Gamma"], "MEDINC2020_CITY": [1.0, 2.0, 3.0]}
    )
    county_panel = _county_panel(
        ht_config, {"49011": {2020: 90000.0}, "49035": {2020: 72000.0}, "49049": {2020: np.nan}}
    )

    result = compute_ht_income_share(costs, hybrid, county_panel, [2020], ht_config).set_index(
        "CITYAREA"
    )

    assert result["HTCOST2020"].tolist() == [1500.0, 1200.0, 600.0]
    # HPLUST = HTCOST * 12 / county median income, NOT the city value
    assert result.loc["Alpha", "HPLUST2020"] == pytest.approx(1500 * 12 / 90000)
    assert result.loc["Beta", "HPLUST2020"] == pytest.approx(1200 * 12 / 72000)
    assert np.isnan(result.loc["Gamma", "HPLUST2020"])  # NaN, never 0
    assert result["MEDINC2020_CNTY"].tolist()[:2] == [90000.0, 72000.0]


def test_ht_income_share_ignores_previously_derived_columns(ht_config):
    costs = pd.DataFrame(
        {
            "CITYAREA": ["Alpha"],
            "county_geoid": ["49011"],
            "HCOST2020": [100.0],
            "TCOST2020": [50.0],
            "MEDINC2020_CITY": [1.0],  # stale; must not collide with the merge
            "HTCOST2020": [1.0],
        }
    )
    hybrid = pd.DataFrame({"CITYAREA": ["Alpha"], "MEDINC2020_CITY": [70000.0]})
    county_panel = _county_panel(ht_config, {"49011": {2020: 60000.0}})
    result = compute_ht_income_share(costs, hybrid, county_panel, [2020], ht_config)
    assert result["MEDINC2020_CITY"].tolist() == [70000.0]
    assert result["HTCOST2020"].tolist() == [150.0]


@needs_geopandas
def test_build_export_frame_schema_fill_and_order(ht_config):
    city_areas = gpd.GeoDataFrame(
        {
            "CITY_NAME": ["Alpha", "GeometryOnly"],
            "SUBAREA": ["North Davis County", None],
            "CO_NAME": ["DAVIS", "DAVIS"],
            "geometry": [_square(0, 0), _square(20, 0)],
        },
        crs="EPSG:26912",
    )
    ht_income = pd.DataFrame(
        {
            "CITYAREA": ["Alpha"],
            "COUNTY_NAME": ["Davis"],
            "HCOST2020": [100.0],
            "TCOST2020": [np.nan],
            "HTCOST2020": [np.nan],
            "HPLUST2020": [np.nan],
            "TOTPOP2019": [5.0],
            "TOTPOP2020": [6.0],
            "TOTHH2020": [2.0],
            "MEDINC2019_CITY": [10.0],
            "MEDINC2019_CNTY": [11.0],
            "MEDINC2020_CITY": [12.0],
            "MEDINC2020_CNTY": [13.0],
            "bg_count_2020": [3],
        }
    )
    export = build_export_frame(city_areas, ht_income, [2020], [2019, 2020], ht_config)

    assert list(export.columns[:7]) == [
        "CITYAREA",
        "SUBAREA",
        "CO_NAME",
        "geometry",
        "HCOST2020",
        "TCOST2020",
        "HTCOST2020",
    ]
    assert "bg_count_2020" not in export.columns
    assert export.columns.get_loc("MEDINC2019_CITY") < export.columns.get_loc("MEDINC2020_CITY")
    assert export.columns.get_loc("COUNTY_NAME") > export.columns.get_loc("MEDINC2020_CNTY")
    assert len(export) == 2 and export.crs.to_epsg() == 26912  # geometry-only city kept

    alpha = export.set_index("CITYAREA").loc["Alpha"]
    assert alpha["SUBAREA"] == "Davis County North"
    assert alpha["TCOST2020"] == 0 and alpha["HPLUST2020"] == 0  # legacy 0-fill toggle
    only = export.set_index("CITYAREA").loc["GeometryOnly"]
    assert only["HCOST2020"] == 0 and only["TOTPOP2020"] == 0 and only["COUNTY_NAME"] == ""


@needs_geopandas
def test_build_export_frame_can_preserve_nulls(ht_config):
    city_areas = gpd.GeoDataFrame(
        {
            "CITY_NAME": ["Alpha"],
            "SUBAREA": ["x"],
            "CO_NAME": ["DAVIS"],
            "geometry": [_square(0, 0)],
        },
        crs="EPSG:26912",
    )
    ht_income = pd.DataFrame(
        {
            "CITYAREA": ["Alpha"],
            "HCOST2020": [1.0],
            "TCOST2020": [1.0],
            "HTCOST2020": [2.0],
            "HPLUST2020": [np.nan],
        }
    )
    export = build_export_frame(
        city_areas, ht_income, [2020], [2020], ht_config, fill_missing=False
    )
    assert np.isnan(export.loc[0, "HPLUST2020"])


def _baseline():
    return pd.DataFrame(
        {
            "CITYAREA": ["A", "B"],
            "SUBAREA": ["NA", ""],
            "TOTPOP2020": [100.0, 200.0],
            "HPLUST2020": [0.25, 0.5],
        }
    )


def test_compare_to_baseline_passes_within_tolerance_and_flags_the_rest():
    candidate = _baseline()
    candidate["TOTPOP2020"] = [101.0, 200.0]
    candidate["HPLUST2020"] = [0.25, 0.5 + 1e-12]

    report = compare_to_baseline(_baseline(), candidate, key="CITYAREA", tolerances={"TOTPOP": 2.0})
    assert report.passed and report.column_order_matches

    candidate["HPLUST2020"] = [0.25, 0.51]
    report = compare_to_baseline(_baseline(), candidate, key="CITYAREA", tolerances={"TOTPOP": 2.0})
    assert not report.passed
    assert report.failing_columns["column"].tolist() == ["HPLUST2020"]


def test_compare_to_baseline_reports_schema_and_key_drift():
    candidate = _baseline().drop(columns=["SUBAREA"]).assign(EXTRA=1)
    candidate = candidate[candidate["CITYAREA"] != "B"]
    report = compare_to_baseline(_baseline(), candidate, key="CITYAREA")
    assert report.missing_columns == ["SUBAREA"]
    assert report.extra_columns == ["EXTRA"]
    assert report.missing_keys == ["B"]
    assert not report.passed


def test_compare_to_baseline_treats_legacy_zero_text_as_blank_and_nan_as_a_mismatch():
    candidate = _baseline()
    candidate["SUBAREA"] = ["NA", "0"]
    assert compare_to_baseline(_baseline(), candidate, key="CITYAREA").passed

    candidate["HPLUST2020"] = [np.nan, 0.5]
    report = compare_to_baseline(_baseline(), candidate, key="CITYAREA")
    assert report.failing_columns["column"].tolist() == ["HPLUST2020"]


def test_load_ht_income_config_resolves_paths_against_repo_root():
    config = load_ht_income_config()
    assert config.county_fips == ["003", "011", "035", "049", "057"]
    assert config.acs_vars["median_income"] == "B19013_001E"
    assert config.county_tdm_rows == {"Davis County": "49011", "Weber County": "49057"}
    assert config.paths["ht_costs_csv"].is_absolute()
    assert config.parity_tolerances == {"TOTPOP": 2.0, "TOTHH": 2.0}

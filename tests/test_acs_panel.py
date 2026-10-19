import numpy as np
import pandas as pd
import pytest

from regional_metrics.acs_panel import (
    apply_income_fallback,
    build_filled_panel,
    build_place_panel,
    fetch_acs_level,
    fill_acs_panel,
    place_name_lookup,
)

PAIRS = [("median_income", "income_source")]


def _panel(values, geoid="g1", start=2019):
    years = range(start, start + len(values))
    return pd.DataFrame(
        {
            "gid": geoid,
            "year": list(years),
            "median_income": values,
            "income_source": ["acs" if v == v else np.nan for v in values],
        }
    )


def test_fill_interpolates_internal_gap_and_tags_source():
    filled = fill_acs_panel(_panel([100.0, np.nan, 300.0]), "gid", PAIRS)
    assert filled["median_income"].tolist() == [100.0, 200.0, 300.0]
    assert filled["income_source"].tolist() == ["acs", "interpolated", "acs"]


def test_fill_extrapolates_edges_with_ols_trend():
    filled = fill_acs_panel(_panel([np.nan, 200.0, 300.0, 400.0, np.nan]), "gid", PAIRS)
    assert filled["median_income"].tolist() == [100.0, 200.0, 300.0, 400.0, 500.0]
    assert filled["income_source"].tolist() == [
        "extrapolated",
        "acs",
        "acs",
        "acs",
        "extrapolated",
    ]


def test_fill_leaves_geographies_with_fewer_than_two_observations_null():
    filled = fill_acs_panel(_panel([np.nan, 250.0, np.nan]), "gid", PAIRS)
    assert filled["median_income"].isna().tolist() == [True, False, True]
    assert filled["income_source"].isna().tolist() == [True, False, True]


def test_fill_drops_non_positive_extrapolations():
    # The 40 -> 20 trend reaches 0 in the third year; that must be NaN, not a real zero.
    filled = fill_acs_panel(_panel([40.0, 20.0, np.nan]), "gid", PAIRS)
    assert filled["median_income"].iloc[:2].tolist() == [40.0, 20.0]
    assert np.isnan(filled["median_income"].iloc[2])


def test_fill_treats_each_geography_independently():
    panel = pd.concat(
        [_panel([100.0, np.nan, 300.0], "a"), _panel([10.0, np.nan, 30.0], "b")],
        ignore_index=True,
    )
    filled = fill_acs_panel(panel, "gid", PAIRS)
    middle = filled[filled["year"] == 2020].set_index("gid")["median_income"]
    assert middle.to_dict() == {"a": 200.0, "b": 20.0}


def test_fill_year_gap_uses_year_distance_not_row_position():
    panel = pd.DataFrame(
        {
            "gid": "g1",
            "year": [2019, 2020, 2022],
            "median_income": [100.0, np.nan, 400.0],
            "income_source": ["acs", np.nan, "acs"],
        }
    )
    filled = fill_acs_panel(panel, "gid", PAIRS).sort_values("year")
    assert filled["median_income"].tolist() == [100.0, 200.0, 400.0]


def _bg_panel(config, bg_id, incomes):
    rows = [
        {
            config.bg_key: bg_id,
            "year": year,
            "median_income": income,
            "population": 10.0,
            "households": 4.0,
            "income_source": "acs" if income == income else np.nan,
            "population_source": "acs",
            "households_source": "acs",
        }
        for year, income in incomes.items()
    ]
    return pd.DataFrame(rows)


def test_income_fallback_prefers_tract_then_county(ht_config):
    bg = pd.concat(
        [
            _bg_panel(ht_config, "490110001001", {2019: np.nan, 2020: np.nan, 2021: 90.0}),
            _bg_panel(ht_config, "490110002001", {2019: np.nan}),
        ],
        ignore_index=True,
    )
    tract = pd.DataFrame(
        {
            ht_config.tract_key: ["49011000100", "49011000100"],
            "year": [2019, 2021],
            "median_income": [70.0, 75.0],
        }
    )
    county = pd.DataFrame(
        {
            ht_config.county_key: ["49011"] * 3,
            "year": [2019, 2020, 2021],
            "median_income": [50.0, 55.0, 60.0],
        }
    )

    filled, summary = apply_income_fallback(bg, tract, county, ht_config)

    key = ht_config.bg_key
    first = filled[filled[key] == "490110001001"].set_index("year")
    assert first.loc[2019, ["median_income", "income_source"]].tolist() == [70.0, "tract_fallback"]
    assert first.loc[2020, ["median_income", "income_source"]].tolist() == [55.0, "county_fallback"]
    assert first.loc[2021, ["median_income", "income_source"]].tolist() == [90.0, "acs"]
    other = filled[filled[key] == "490110002001"].iloc[0]
    assert other["median_income"] == 50.0 and other["income_source"] == "county_fallback"
    counts = summary.set_index("year").loc[2019, ["filled_by_tract", "filled_by_county"]]
    assert counts.tolist() == [1, 1]
    # population and households are never touched by the fallback
    assert filled["population"].eq(10.0).all()


def _fake_fetcher(failing_years=()):
    def fetch(dataset, variables, year, params, return_geoid, guess_dtypes):
        if year in failing_years:
            raise RuntimeError("unsupported geography hierarchy")
        level = params["for"].split(":")[0]
        county = params["in"].split("county:")[-1] if "county:" in params["in"] else None
        geoids = {
            "block group": [f"49{county}000100{n}" for n in (1, 2)] if county else [],
            "tract": [f"49{county}000100"] if county else [],
            "county": ["49011", "49035", "49049"],
            "place": ["4900001", "4900002"],
        }[level]
        frame = pd.DataFrame({"GEOID": geoids})
        for code in variables:
            base = 1000.0 if code == "B19013_001E" else 5.0
            frame[code] = [base + year] * len(geoids)
        if level == "block group" and county == "011":
            frame.loc[0, "B19013_001E"] = -666666666  # Census suppression sentinel
        return frame

    return fetch


def test_fetch_level_cleans_suppressed_values_and_filters_counties(ht_config):
    fetcher = _fake_fetcher()
    bg = fetch_acs_level("block group", 2020, ht_config, "KEY", fetcher)
    assert len(bg) == 4  # 2 counties x 2 block groups
    assert bg["median_income"].isna().sum() == 1
    county = fetch_acs_level("county", 2020, ht_config, "KEY", fetcher)
    assert sorted(county["geoid"]) == ["49011", "49035"]  # Utah County (049) filtered out


def test_build_filled_panel_survives_failed_years_and_expands_reference(ht_config):
    fetcher = _fake_fetcher(failing_years={2019})
    reference = ["490110001001", "490110001002", "490350001001", "490350001002"]
    panel, status = build_filled_panel(
        "block group", [2019, 2020, 2021], ht_config, "KEY", "GEOID", reference, fetcher
    )
    assert status.set_index("year")["status"].to_dict() == {
        2019: "missing",
        2020: "fetched",
        2021: "fetched",
    }
    assert len(panel) == 12  # every reference geography has a row for every year
    row = panel[(panel["GEOID"] == "490350001001") & (panel["year"] == 2019)].iloc[0]
    assert row["income_source"] == "extrapolated"
    assert row["median_income"] == 3019.0  # OLS through 2020 -> 3020 and 2021 -> 3021


def test_place_name_lookup_prefers_incorporated_over_cdp():
    places = pd.DataFrame(
        {
            "GEOID": ["1", "2", "3"],
            "NAME": ["Enterprise", "Enterprise", "Alpine"],
            "NAMELSAD": ["Enterprise CDP", "Enterprise city", "Alpine city"],
        }
    )
    assert place_name_lookup(places) == {"2": "Enterprise", "3": "Alpine"}


def test_build_place_panel_maps_names_and_fills(ht_config):
    places = pd.DataFrame(
        {
            "GEOID": ["4900001", "4900002"],
            "NAME": ["Alpine", "Bountiful"],
            "NAMELSAD": ["Alpine city", "Bountiful city"],
        }
    )
    panel, _ = build_place_panel([2020, 2021], ht_config, "KEY", places, _fake_fetcher())
    assert set(panel[ht_config.place_key]) == {"Alpine", "Bountiful"}
    alpine = panel[panel[ht_config.place_key] == "Alpine"].sort_values("year")
    assert alpine["place_medinc"].tolist() == [3020.0, 3021.0]


def test_fetch_level_rejects_unknown_level(ht_config):
    with pytest.raises(ValueError):
        fetch_acs_level("zip", 2020, ht_config, "KEY", _fake_fetcher())

"""City-level housing + transportation income share (HPLUST) from ACS median income.

HPLUST{YYYY} = (HTCOST{YYYY} * 12) / MEDINC{YYYY}_CNTY, where HTCOST is the monthly
H + T cost and MEDINC_CNTY is the ACS county median household income (B19013_001E).
"""

from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd

from .io_spatial import ensure_analysis_crs, require_geopandas
from .models import HTIncomeConfig

DERIVED_PREFIXES = ("MEDINC", "TOTPOP", "TOTHH", "bg_count_", "HTCOST")
STAT_FILL_PREFIXES = ("MEDINC", "TOTPOP", "TOTHH")
OUTPUT_PREFIX_ORDER = ["HCOST", "TCOST", "HTCOST", "HPLUST", "TOTPOP", "TOTHH"]


def load_ht_costs(path, config: HTIncomeConfig) -> pd.DataFrame:
    """Read the composite H+T cost CSV and attach the join keys (city, county GEOID)."""
    costs = pd.read_csv(path, thousands=",")
    costs = costs.replace(-1, 0)  # -1 = no data for that city-year

    if config.city_key not in costs.columns and "City Area" in costs.columns:
        costs[config.city_key] = costs["City Area"]
    costs = costs.drop(columns=["City Area"], errors="ignore")
    costs = costs.rename(columns={"County": "COUNTY_NAME"})
    costs[config.county_key] = costs["COUNTY_NAME"].map(config.county_name_to_fips)
    return costs


def derive_years(costs: pd.DataFrame, config: HTIncomeConfig) -> tuple[list[int], list[int]]:
    """Return (target_years, acs_years).

    target_years come from the cost-column suffixes; acs_years run from the earliest ACS
    vintage through the latest H+T year.
    """
    if config.target_years_override:
        target_years = sorted(config.target_years_override)
    else:
        target_years = sorted(
            {
                int(column[-4:])
                for column in costs.columns
                if column[-4:].isdigit()
                and any(column.startswith(prefix) for prefix in config.year_prefixes)
            }
        )
    acs_years = list(range(config.acs_start_year, max(target_years) + 1))
    return target_years, acs_years


def check_city_names(costs: pd.DataFrame, city_areas, config: HTIncomeConfig) -> dict[str, list]:
    """City names present in only one of the cost CSV / city layer."""
    csv_names = set(costs[config.city_key].dropna())
    layer_names = set(city_areas[config.city_key].dropna())
    return {
        "in_csv_not_layer": sorted(csv_names - layer_names),
        "in_layer_not_csv": sorted(layer_names - csv_names),
    }


def prepare_city_areas(city_areas, config: HTIncomeConfig):
    """Add the CITYAREA join key (from CITY_NAME) to the city boundary layer."""
    prepared = city_areas.copy()
    if config.city_key not in prepared.columns and "CITY_NAME" in prepared.columns:
        prepared[config.city_key] = prepared["CITY_NAME"]
    return prepared


def assign_block_groups_to_cities(block_groups, city_areas, config: HTIncomeConfig) -> pd.DataFrame:
    """Assign each block group to one city by the city polygon containing its centroid.

    Replaces ``arcpy.analysis.SpatialJoin(..., "HAVE_THEIR_CENTER_IN")``. Block groups are
    reprojected to the city CRS first. Where city polygons overlap, the first city in layer
    order wins (a one-to-one join).
    """
    gpd = require_geopandas()
    cities = ensure_analysis_crs(prepare_city_areas(city_areas, config))
    bgs = block_groups.to_crs(cities.crs)

    centroids = gpd.GeoDataFrame(
        {config.bg_key: bgs[config.bg_key].astype(str).to_numpy()},
        geometry=bgs.geometry.centroid.to_numpy(),
        crs=bgs.crs,
    )
    city_columns = [config.city_key, "SUBAREA", "CO_NAME"]
    joined = gpd.sjoin(
        centroids,
        cities[[*city_columns, cities.geometry.name]],
        how="left",
        predicate="within",
    )
    joined = joined.sort_values("index_right", na_position="last", kind="stable")
    lookup = joined.drop_duplicates(config.bg_key, keep="first")
    lookup = pd.DataFrame(lookup[[config.bg_key, *city_columns]]).sort_index()

    lookup["CO_NAME"] = lookup["CO_NAME"].str.upper()
    lookup["SUBAREA"] = lookup["SUBAREA"].replace(config.workshop_area_lookup)
    return lookup.reset_index(drop=True)


def aggregate_city_income(
    bg_panel: pd.DataFrame,
    bg_city_lookup: pd.DataFrame,
    acs_years: Iterable[int],
    config: HTIncomeConfig,
) -> pd.DataFrame:
    """Household-weighted city income plus summed population / households, wide by year.

    Only block groups assigned to a city with income and households > 0 contribute.
    """
    panel = bg_panel.copy()
    panel[config.bg_key] = panel[config.bg_key].astype(str)
    panel = panel.merge(
        bg_city_lookup[[config.bg_key, config.city_key]], on=config.bg_key, how="left"
    )
    panel = panel[
        panel[config.city_key].notna()
        & panel["median_income"].notna()
        & panel["households"].notna()
        & (panel["households"] > 0)
        & panel["year"].isin(list(acs_years))
    ].copy()
    panel["weighted_income"] = panel["median_income"] * panel["households"]

    grouped = (
        panel.groupby([config.city_key, "year"])
        .agg(
            TOTPOP=("population", "sum"),
            TOTHH=("households", "sum"),
            MEDINC=("weighted_income", "sum"),
        )
        .reset_index()
    )
    grouped["MEDINC"] = (grouped["MEDINC"] / grouped["TOTHH"]).round(0)

    wide = grouped.pivot(
        index=config.city_key, columns="year", values=["TOTPOP", "TOTHH", "MEDINC"]
    )
    wide.columns = [f"{name}{year}" for name, year in wide.columns]
    # Legacy column order: TOTPOP, TOTHH, MEDINC for one year, then the next year.
    years = sorted({int(column[-4:]) for column in wide.columns})
    ordered = [f"{name}{year}" for year in years for name in ("TOTPOP", "TOTHH", "MEDINC")]
    return wide[[c for c in ordered if c in wide.columns]].reset_index()


def county_income_wide(
    county_panel: pd.DataFrame, config: HTIncomeConfig, prefix: str
) -> pd.DataFrame:
    wide = county_panel.pivot(index=config.county_key, columns="year", values="median_income")
    wide.columns = [f"{prefix}{year}" for year in wide.columns]
    return wide.reset_index()


def build_hybrid_medinc(
    city_income: pd.DataFrame,
    place_panel: pd.DataFrame,
    county_panel: pd.DataFrame,
    acs_years: Iterable[int],
    config: HTIncomeConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """City median income by source: ACS Place > ACS County (county TDM rows) > BG-weighted.

    Returns the frame with ``MEDINC{yr}_CITY`` columns plus a table of which source each
    city resolved to for the latest ACS year.
    """
    acs_years = list(acs_years)
    hybrid = city_income.copy()

    place_wide = place_panel.pivot_table(
        index=config.place_key, columns="year", values="place_medinc", aggfunc="mean"
    )
    place_wide.columns = [f"place_medinc_{year}" for year in place_wide.columns]
    place_wide = place_wide.reset_index()
    hybrid = hybrid.merge(
        place_wide, left_on=config.city_key, right_on=config.place_key, how="left"
    )

    county_wide = county_income_wide(county_panel, config, "county_medinc_")
    county_rows = []
    for tdm_name, fips in config.county_tdm_rows.items():
        match = county_wide[county_wide[config.county_key] == fips]
        if len(match) != 1:
            continue
        entry = {config.city_key: tdm_name}
        for year in acs_years:
            entry[f"county_medinc_{year}"] = match.get(
                f"county_medinc_{year}", pd.Series([np.nan])
            ).iloc[0]
        county_rows.append(entry)
    county_tdm = pd.DataFrame(
        county_rows, columns=[config.city_key] + [f"county_medinc_{y}" for y in acs_years]
    )
    hybrid = hybrid.merge(county_tdm, on=config.city_key, how="left")

    latest = acs_years[-1]
    source = pd.Series("none", index=hybrid.index, dtype=object)
    for year in acs_years:
        medinc_col = f"MEDINC{year}"
        place = hybrid.get(f"place_medinc_{year}", pd.Series(np.nan, index=hybrid.index))
        county = hybrid.get(f"county_medinc_{year}", pd.Series(np.nan, index=hybrid.index))
        base = hybrid[medinc_col] if medinc_col in hybrid else pd.Series(np.nan, index=hybrid.index)

        use_place = place.notna() & (place > 0)
        use_county = ~use_place & county.notna() & (county > 0)
        year_source = np.where(
            use_place,
            "acs_place",
            np.where(
                use_county, "acs_county", np.where(base.notna() & (base > 0), "bg_weighted", "none")
            ),
        )
        hybrid[medinc_col] = np.where(use_place, place, np.where(use_county, county, base))
        if year == latest:
            source = pd.Series(year_source, index=hybrid.index)

    sources = pd.DataFrame({config.city_key: hybrid[config.city_key], "medinc_source": source})
    temp_columns = [c for c in hybrid.columns if c.startswith(("place_medinc_", "county_medinc_"))]
    hybrid = hybrid.drop(columns=[*temp_columns, config.place_key], errors="ignore")
    hybrid = hybrid.rename(columns={f"MEDINC{year}": f"MEDINC{year}_CITY" for year in acs_years})
    return hybrid, sources


def compute_ht_income_share(
    costs: pd.DataFrame,
    hybrid_medinc: pd.DataFrame,
    county_panel: pd.DataFrame,
    target_years: Iterable[int],
    config: HTIncomeConfig,
) -> pd.DataFrame:
    """Merge city + county income into the cost table and compute HTCOST and HPLUST.

    HPLUST stays NaN (not 0) where county income is missing or zero.
    """
    result = costs.drop(columns=[c for c in costs.columns if c.startswith(DERIVED_PREFIXES)])
    result = result.merge(hybrid_medinc, on=config.city_key, how="left")
    result = result.merge(
        county_income_wide(county_panel, config, "MEDINC_CNTY_"), on=config.county_key, how="left"
    )
    result = result.rename(
        columns={
            c: f"MEDINC{c.removeprefix('MEDINC_CNTY_')}_CNTY"
            for c in result.columns
            if c.startswith("MEDINC_CNTY_")
        }
    )

    for year in target_years:
        income = result[f"MEDINC{year}_CNTY"]
        h_cost, t_cost = result[f"HCOST{year}"], result[f"TCOST{year}"]
        ht_cost = np.where(h_cost.notna() & t_cost.notna(), h_cost + t_cost, np.nan)
        result[f"HTCOST{year}"] = ht_cost
        result[f"HPLUST{year}"] = np.where(
            income.notna() & (income > 0), (ht_cost * 12) / income, np.nan
        )
    return result


def build_export_frame(
    city_areas,
    ht_income: pd.DataFrame,
    target_years: Iterable[int],
    acs_years: Iterable[int],
    config: HTIncomeConfig,
    fill_missing: bool = True,
):
    """City geometry + H+T income columns in the legacy output schema and column order."""
    gpd = require_geopandas()
    target_years, acs_years = list(target_years), list(acs_years)
    cities = prepare_city_areas(city_areas, config)
    geometry_name = cities.geometry.name
    base = cities[[config.city_key, "SUBAREA", "CO_NAME", geometry_name]]
    export = base.merge(ht_income, on=config.city_key, how="left")
    export["SUBAREA"] = export["SUBAREA"].replace(config.workshop_area_lookup)
    export = export.drop(columns=[c for c in export.columns if c.startswith("bg_count_")])

    cost_columns = [
        f"{prefix}{year}" for year in target_years for prefix in ("HCOST", "TCOST", "HTCOST")
    ]
    cost_columns = [c for c in cost_columns if c in export.columns]
    export[cost_columns] = export[cost_columns].fillna(0)

    stat_columns = [c for c in export.columns if c.startswith(STAT_FILL_PREFIXES)]
    export[stat_columns] = export[stat_columns].fillna(0)

    ordered = [config.city_key, "SUBAREA", "CO_NAME", geometry_name]
    for prefix in OUTPUT_PREFIX_ORDER:
        ordered += [f"{prefix}{y}" for y in target_years if f"{prefix}{y}" in export.columns]
    for year in acs_years:
        ordered += [c for c in (f"MEDINC{year}_CITY", f"MEDINC{year}_CNTY") if c in export.columns]
    remaining = [c for c in export.columns if c not in ordered]
    export = export[ordered + remaining]

    if fill_missing:
        # Legacy toggle: dashboard expects 0, not null, in every non-geometry column.
        # Text columns get "" so the GeoPackage keeps a single column type.
        text_columns = [
            c for c in export.columns if c != geometry_name and export[c].dtype == object
        ]
        numeric_columns = [
            c for c in export.columns if c != geometry_name and c not in text_columns
        ]
        export[numeric_columns] = export[numeric_columns].fillna(0)
        export[text_columns] = export[text_columns].fillna("")

    return gpd.GeoDataFrame(export, geometry=geometry_name, crs=cities.crs)

"""ACS income / population / household panels with gap filling and a geography fallback.

A *panel* is a long table with one row per (geography, year). The gap-filling and
fallback rules reproduce the main-branch H+T income notebook so outputs stay in parity.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from pathlib import Path

import numpy as np
import pandas as pd

from .models import HTIncomeConfig

VALUE_COLUMNS = ("median_income", "population", "households")
VALUE_SOURCE_PAIRS: list[tuple[str, str]] = [
    ("median_income", "income_source"),
    ("population", "population_source"),
    ("households", "households_source"),
]

Fetcher = Callable[..., pd.DataFrame]


def default_fetcher() -> Fetcher:
    from pygris.data import get_census

    return get_census


def _fill_values(years: np.ndarray, values: np.ndarray):
    """Fill one geography's series; returns (filled, interpolated_mask, extrapolated_mask)."""
    observed = ~np.isnan(values)
    none = np.zeros(len(values), dtype=bool)
    if observed.sum() < 2:
        return values, none, none

    years_obs = years[observed]
    values_obs = values[observed]
    first_year, last_year = years_obs[0], years_obs[-1]

    filled = values.copy()
    inside = ~observed & (years > first_year) & (years < last_year)
    filled[inside] = np.interp(years[inside], years_obs, values_obs)

    slope, intercept = np.polyfit(years_obs, values_obs, 1)
    edge = np.isnan(filled) & ((years < first_year) | (years > last_year))
    filled[edge] = slope * years[edge] + intercept
    return filled, inside, edge


def fill_acs_panel(
    panel: pd.DataFrame,
    geoid_col: str,
    value_source_pairs: Iterable[tuple[str, str]] = VALUE_SOURCE_PAIRS,
    year_col: str = "year",
) -> pd.DataFrame:
    """Fill gaps in a long ACS panel, one geography at a time.

    * Internal gaps are linearly interpolated between the flanking observed years.
    * Edge gaps are extrapolated with an OLS trend fitted to all observed years.
    * Geographies with fewer than two observed years stay NaN.
    * Filled rows get ``interpolated`` / ``extrapolated`` in their source column.
    * Values are rounded to whole numbers and non-positive values become NaN.
    """
    pairs = list(value_source_pairs)
    result = panel.sort_values([geoid_col, year_col]).reset_index(drop=True)
    years = result[year_col].to_numpy(dtype=float)

    for value_col, source_col in pairs:
        values = result[value_col].to_numpy(dtype=float).copy()
        sources = result[source_col].astype(object).to_numpy().copy()

        for rows in result.groupby(geoid_col, sort=False).indices.values():
            filled, inside, edge = _fill_values(years[rows], values[rows])
            values[rows] = filled
            group_sources = sources[rows]
            group_sources[inside] = "interpolated"
            group_sources[edge] = "extrapolated"
            sources[rows] = group_sources

        values = np.round(values, 0)
        values[values <= 0] = np.nan
        result[value_col] = values
        result[source_col] = sources

    return result


def clean_acs_values(frame: pd.DataFrame, columns: Iterable[str] = VALUE_COLUMNS) -> pd.DataFrame:
    """Coerce ACS values to numeric; zero / negative (suppression sentinels) become NaN."""
    cleaned = frame.copy()
    for column in columns:
        cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")
        cleaned.loc[cleaned[column] <= 0, column] = np.nan
    return cleaned


def _rename_acs_variables(frame: pd.DataFrame, config: HTIncomeConfig) -> pd.DataFrame:
    rename = {code: name for name, code in config.acs_vars.items()}
    return frame.rename(columns=rename)


def fetch_acs_level(
    level: str,
    year: int,
    config: HTIncomeConfig,
    api_key: str,
    fetcher: Fetcher | None = None,
) -> pd.DataFrame:
    """Fetch one ACS 5-year vintage at ``block group``, ``tract``, ``county`` or ``place`` level.

    Returns ``geoid`` plus median_income, population and households (place level only has
    income, returned as ``median_income``).
    """
    get = fetcher or default_fetcher()
    variables = list(config.acs_vars.values())
    state = config.state_fips

    if level in {"block group", "tract"}:
        frames = [
            get(
                dataset=config.acs_dataset,
                variables=variables,
                year=year,
                params={
                    "for": f"{level}:*",
                    "in": f"state:{state} county:{county}",
                    "key": api_key,
                },
                return_geoid=True,
                guess_dtypes=True,
            )
            for county in config.county_fips
        ]
        raw = pd.concat(frames, ignore_index=True)
    elif level == "county":
        raw = get(
            dataset=config.acs_dataset,
            variables=variables,
            year=year,
            params={"for": "county:*", "in": f"state:{state}", "key": api_key},
            return_geoid=True,
            guess_dtypes=True,
        )
        raw = raw[raw["GEOID"].astype(str).str[2:5].isin(config.county_fips)]
    elif level == "place":
        raw = get(
            dataset=config.acs_dataset,
            variables=[config.acs_vars["median_income"]],
            year=year,
            params={"for": "place:*", "in": f"state:{state}", "key": api_key},
            return_geoid=True,
            guess_dtypes=True,
        )
    else:
        raise ValueError(f"Unsupported ACS level: {level}")

    raw = _rename_acs_variables(raw, config)
    raw["geoid"] = raw["GEOID"].astype(str)
    columns = [c for c in VALUE_COLUMNS if c in raw.columns]
    return clean_acs_values(raw[["geoid", *columns]], columns)


def fetch_acs_panel(
    level: str,
    years: Iterable[int],
    config: HTIncomeConfig,
    api_key: str,
    fetcher: Fetcher | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch every year at one level. Failed years are recorded, not raised.

    Returns the raw long panel (``geoid``, values, ``year``, ``*_source`` = ``acs``) and a
    per-year status table.
    """
    parts: list[pd.DataFrame] = []
    status: list[dict] = []
    for year in years:
        try:
            frame = fetch_acs_level(level, year, config, api_key, fetcher)
        except Exception as exc:  # noqa: BLE001 - the API raises assorted exception types
            status.append({"year": year, "status": "missing", "rows": 0, "message": str(exc)})
            continue
        frame["year"] = year
        for _, source_col in VALUE_SOURCE_PAIRS:
            frame[source_col] = "acs"
        parts.append(frame)
        status.append({"year": year, "status": "fetched", "rows": len(frame), "message": "ok"})

    panel = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    return panel, pd.DataFrame(status)


def expand_to_reference(
    panel: pd.DataFrame, reference_ids: Iterable[str], years: Iterable[int]
) -> pd.DataFrame:
    """Give every reference geography a row for every year (NaN where ACS returned nothing)."""
    grid = pd.MultiIndex.from_product(
        [list(reference_ids), list(years)], names=["geoid", "year"]
    ).to_frame(index=False)
    if panel.empty:
        for column in VALUE_COLUMNS:
            grid[column] = np.nan
        for _, source_col in VALUE_SOURCE_PAIRS:
            grid[source_col] = np.nan
        return grid
    return grid.merge(panel, on=["geoid", "year"], how="left")


def build_filled_panel(
    level: str,
    years: Iterable[int],
    config: HTIncomeConfig,
    api_key: str,
    geoid_col: str,
    reference_ids: Iterable[str] | None = None,
    fetcher: Fetcher | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch, (optionally) expand to a reference geography list, then gap-fill."""
    years = list(years)
    raw, status = fetch_acs_panel(level, years, config, api_key, fetcher)
    if reference_ids is not None:
        raw = expand_to_reference(raw, reference_ids, years)
    if raw.empty:
        raise RuntimeError(
            f"No {level}-level ACS data was fetched. Check CENSUS_API_KEY and years."
        )
    raw = raw.rename(columns={"geoid": geoid_col})
    return fill_acs_panel(raw, geoid_col=geoid_col), status


def build_place_panel(
    years: Iterable[int],
    config: HTIncomeConfig,
    api_key: str,
    places: pd.DataFrame,
    fetcher: Fetcher | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Place-level median income keyed on place name, gap-filled like the other panels."""
    name_by_geoid = place_name_lookup(places)
    years = list(years)
    raw, status = fetch_acs_panel("place", years, config, api_key, fetcher)
    if raw.empty:
        raise RuntimeError("No Place ACS data fetched. Check CENSUS_API_KEY and years.")

    raw[config.place_key] = raw["geoid"].map(name_by_geoid)
    panel = raw.dropna(subset=[config.place_key]).rename(
        columns={"median_income": "place_medinc", "income_source": "medinc_source"}
    )
    panel["medinc_source"] = "acs_place"
    panel = panel[[config.place_key, "year", "place_medinc", "medinc_source"]]
    panel = panel.drop_duplicates(subset=[config.place_key, "year"], keep="first")
    filled = fill_acs_panel(
        panel, geoid_col=config.place_key, value_source_pairs=[("place_medinc", "medinc_source")]
    )
    return filled, status


def place_name_lookup(places: pd.DataFrame) -> dict[str, str]:
    """GEOID -> place name, keeping the incorporated place when a name is shared with a CDP."""
    reference = places[["GEOID", "NAME", "NAMELSAD"]].copy()
    reference["is_incorporated"] = (
        reference["NAMELSAD"].str.lower().str.contains(r"\bcity\b|\btown\b", regex=True)
    )
    deduped = reference.sort_values("is_incorporated", ascending=False, kind="stable")
    deduped = deduped.drop_duplicates(subset="NAME", keep="first")
    return deduped.set_index("GEOID")["NAME"].to_dict()


def apply_income_fallback(
    bg_panel: pd.DataFrame,
    tract_panel: pd.DataFrame,
    county_panel: pd.DataFrame,
    config: HTIncomeConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fill null block-group income from the parent tract, then the parent county.

    Only ``median_income`` / ``income_source`` change; population and households stay as
    fetched. Returns the panel and a per-year summary of how many rows each level filled.
    """
    bg_key, tract_key, county_key = config.bg_key, config.tract_key, config.county_key
    panel = bg_panel.copy()
    panel[bg_key] = panel[bg_key].astype(str)
    panel[tract_key] = panel[bg_key].str[:11]
    panel[county_key] = panel[bg_key].str[:5]

    tract_lookup = (
        tract_panel[[tract_key, "year", "median_income"]]
        .rename(columns={"median_income": "tract_income"})
        .drop_duplicates([tract_key, "year"])
    )
    county_lookup = (
        county_panel[[county_key, "year", "median_income"]]
        .rename(columns={"median_income": "county_income"})
        .drop_duplicates([county_key, "year"])
    )
    tract_lookup[tract_key] = tract_lookup[tract_key].astype(str)
    county_lookup[county_key] = county_lookup[county_key].astype(str)

    panel = panel.merge(tract_lookup, on=[tract_key, "year"], how="left")
    panel = panel.merge(county_lookup, on=[county_key, "year"], how="left")
    panel["null_before"] = panel["median_income"].isna()

    tract_fill = panel["median_income"].isna() & panel["tract_income"].notna()
    panel.loc[tract_fill, "median_income"] = panel.loc[tract_fill, "tract_income"]
    panel.loc[tract_fill, "income_source"] = "tract_fallback"

    county_fill = panel["median_income"].isna() & panel["county_income"].notna()
    panel.loc[county_fill, "median_income"] = panel.loc[county_fill, "county_income"]
    panel.loc[county_fill, "income_source"] = "county_fallback"

    summary = (
        panel.assign(
            filled_by_tract=tract_fill,
            filled_by_county=county_fill,
            still_null=panel["median_income"].isna(),
        )
        .groupby("year")[["null_before", "filled_by_tract", "filled_by_county", "still_null"]]
        .sum()
        .astype(int)
        .reset_index()
    )
    helper_columns = ["tract_income", "county_income", "null_before", tract_key, county_key]
    return panel.drop(columns=helper_columns), summary


def read_panel_cache(path: Path, id_columns: Iterable[str]) -> pd.DataFrame:
    return pd.read_csv(path, dtype={column: str for column in id_columns})


def write_panel_cache(panel: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_csv(path, index=False)
    return path

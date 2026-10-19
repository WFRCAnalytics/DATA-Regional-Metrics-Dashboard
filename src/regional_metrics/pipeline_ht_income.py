from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from .acs_panel import (
    Fetcher,
    apply_income_fallback,
    build_filled_panel,
    build_place_panel,
    read_panel_cache,
    write_panel_cache,
)
from .ht_income import (
    aggregate_city_income,
    assign_block_groups_to_cities,
    build_export_frame,
    build_hybrid_medinc,
    check_city_names,
    compute_ht_income_share,
    derive_years,
    load_ht_costs,
    prepare_city_areas,
)
from .io_spatial import read_spatial, require_geopandas, write_spatial
from .models import HTIncomeConfig
from .validation import ParityReport, compare_to_baseline

logger = logging.getLogger(__name__)

BG_REFERENCE_COLUMNS = ["GEOID", "STATEFP", "COUNTYFP", "TRACTCE", "BLKGRPCE", "geometry"]


@dataclass
class HTIncomeResult:
    export: object  # GeoDataFrame in the legacy output schema
    target_years: list[int]
    acs_years: list[int]
    bg_panel: pd.DataFrame
    fallback_summary: pd.DataFrame
    bg_city_lookup: pd.DataFrame
    city_income: pd.DataFrame
    medinc_sources: pd.DataFrame
    name_mismatches: dict[str, list] = field(default_factory=dict)


def load_block_group_reference(config: HTIncomeConfig, refresh: bool = False):
    """2020 block-group polygons for the configured counties (fetched once via pygris)."""
    path = config.paths["bg_reference"]
    if path.exists() and not refresh:
        return read_spatial(path)

    from pygris import block_groups

    frame = block_groups(state=config.state, year=config.bg_reference_year, cache=True)
    frame = frame[frame["COUNTYFP"].isin(config.county_fips)][BG_REFERENCE_COLUMNS].copy()
    frame[config.bg_key] = frame[config.bg_key].astype(str)
    write_spatial(frame, path)
    return frame


def load_places_reference(config: HTIncomeConfig):
    from pygris import places

    return places(state=config.state, year=config.bg_reference_year, cache=True)


def _covers_years(panel: pd.DataFrame, years: list[int]) -> bool:
    return set(years).issubset(set(panel["year"].unique()))


def load_or_build_panel(
    path: Path,
    years: list[int],
    id_column: str,
    builder,
    refresh: bool = False,
) -> pd.DataFrame:
    """Read a cached panel CSV, or build and cache it (delete the file to refresh)."""
    if path.exists() and not refresh:
        cached = read_panel_cache(path, [id_column])
        if _covers_years(cached, years):
            return cached
        logger.info("Cache %s is missing requested years; rebuilding.", path)
    panel, status = builder()
    logger.info("ACS fetch status for %s:\n%s", path.name, status.to_string(index=False))
    write_panel_cache(panel, path)
    return panel


def build_ht_income(
    config: HTIncomeConfig,
    api_key: str | None = None,
    refresh: bool = False,
    fetcher: Fetcher | None = None,
) -> HTIncomeResult:
    """Run the full city-level H+T income-share workflow and return every stage's output."""
    costs = load_ht_costs(config.paths["ht_costs_csv"], config)
    target_years, acs_years = derive_years(costs, config)
    city_areas = prepare_city_areas(read_spatial(config.paths["city_areas"]), config)
    mismatches = check_city_names(costs, city_areas, config)

    block_groups = load_block_group_reference(config, refresh=refresh)
    bg_ids = block_groups[config.bg_key].astype(str).tolist()

    def _key() -> str:
        if not api_key:
            raise RuntimeError("A Census API key is required to build missing ACS panels.")
        return api_key

    bg_path = config.paths["bg_panel"]
    bg_cached = bg_path.exists() and not refresh
    bg_panel = pd.read_parquet(bg_path) if bg_cached else None
    if bg_panel is None or not _covers_years(bg_panel, acs_years):
        bg_panel, bg_status = build_filled_panel(
            "block group",
            acs_years,
            config,
            _key(),
            config.bg_key,
            reference_ids=bg_ids,
            fetcher=fetcher,
        )
        logger.info("Block-group ACS fetch status:\n%s", bg_status.to_string(index=False))
        bg_path.parent.mkdir(parents=True, exist_ok=True)
        bg_panel.to_parquet(bg_path, index=False)

    tract_panel = load_or_build_panel(
        config.paths["tract_panel"],
        acs_years,
        config.tract_key,
        lambda: build_filled_panel(
            "tract", acs_years, config, _key(), config.tract_key, fetcher=fetcher
        ),
        refresh,
    )
    county_panel = load_or_build_panel(
        config.paths["county_panel"],
        acs_years,
        config.county_key,
        lambda: build_filled_panel(
            "county", acs_years, config, _key(), config.county_key, fetcher=fetcher
        ),
        refresh,
    )
    place_panel = load_or_build_panel(
        config.paths["place_panel"],
        acs_years,
        config.place_key,
        lambda: build_place_panel(
            acs_years, config, _key(), load_places_reference(config), fetcher=fetcher
        ),
        refresh,
    )

    bg_panel, fallback_summary = apply_income_fallback(bg_panel, tract_panel, county_panel, config)
    bg_city_lookup = assign_block_groups_to_cities(block_groups, city_areas, config)
    duplicates = bg_city_lookup[config.bg_key].duplicated().sum()
    if duplicates:
        raise ValueError(f"Duplicate GEOIDs in block-group/city lookup: {duplicates}")

    city_income = aggregate_city_income(bg_panel, bg_city_lookup, acs_years, config)
    hybrid, sources = build_hybrid_medinc(city_income, place_panel, county_panel, acs_years, config)
    ht_income = compute_ht_income_share(costs, hybrid, county_panel, target_years, config)
    export = build_export_frame(city_areas, ht_income, target_years, acs_years, config)

    return HTIncomeResult(
        export=export,
        target_years=target_years,
        acs_years=acs_years,
        bg_panel=bg_panel,
        fallback_summary=fallback_summary,
        bg_city_lookup=bg_city_lookup,
        city_income=city_income,
        medinc_sources=sources,
        name_mismatches=mismatches,
    )


def write_ht_income_outputs(result: HTIncomeResult, config: HTIncomeConfig) -> tuple[Path, Path]:
    """GeoPackage (maintained spatial output) plus a CSV without geometry."""
    require_geopandas()
    gpkg = write_spatial(result.export, config.paths["output_gpkg"])
    csv_path = config.paths["output_csv"]
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    result.export.drop(columns=result.export.geometry.name).to_csv(csv_path, index=False)
    return gpkg, csv_path


def validate_against_baseline(
    export: pd.DataFrame, config: HTIncomeConfig, baseline_path: Path | None = None
) -> ParityReport:
    """Compare a built H+T income frame with the main-branch snapshot."""
    # keep_default_na=False: "NA" is a real SUBAREA value in the city layer, not a null.
    baseline = pd.read_csv(
        baseline_path or config.paths["baseline_csv"], keep_default_na=False, na_values=[""]
    )
    candidate = pd.DataFrame(export.drop(columns=export.geometry.name, errors="ignore"))
    return compare_to_baseline(
        baseline,
        candidate,
        key=config.city_key,
        tolerances=config.parity_tolerances,
    )

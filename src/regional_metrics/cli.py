from __future__ import annotations

import subprocess
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from .io_arcgis import connect_gis, feature_layer_to_frame, get_item_layer
from .io_spatial import read_spatial, write_spatial
from .logging_utils import configure_logging
from .metrics_ht import build_city_cost_layer, load_ht_cost_table, summarize_ht_by_geography
from .pipeline_performance_metrics import build_performance_metrics
from .publish_agol import build_publish_plan, publish_geodataframe
from .settings import (
    load_arcgis_settings,
    load_project_config,
    project_root,
    publish_enabled,
)
from .validation import compare_frames

app = typer.Typer(help="Modernized CLI for the WFRC Regional Metrics Dashboard.")
build_app = typer.Typer(help="Build local outputs.")
publish_app = typer.Typer(help="Publish hosted ArcGIS outputs.")
docs_app = typer.Typer(help="Documentation helpers.")

app.add_typer(build_app, name="build")
app.add_typer(publish_app, name="publish")
app.add_typer(docs_app, name="docs")

console = Console()


@build_app.command("performance-metrics")
def build_performance_metrics_command(
    output: Path | None = typer.Option(None, help="Optional local output path."),
    dry_run: bool = typer.Option(False, help="Validate config without connecting to ArcGIS."),
    metrics: list[str] | None = typer.Option(None, help="Optional subset of metric keys to build."),
):
    config = load_project_config()
    configure_logging(config.logging)

    if dry_run:
        table = Table(title="Configured metrics")
        table.add_column("Key")
        table.add_column("Name")
        table.add_column("Family")
        for key, metric in config.metrics.items():
            if metrics and key not in metrics:
                continue
            table.add_row(key, metric.name, metric.family)
        console.print(table)
        return

    gis = connect_gis(load_arcgis_settings())
    frame = build_performance_metrics(config, gis, selected_metrics=metrics)
    if output:
        destination = write_spatial(frame, output)
        console.print(f"Wrote performance metrics to {destination}")
    else:
        console.print(frame.head().to_string())


@build_app.command("housing-transportation")
def build_housing_transportation_command(
    output: Path = typer.Option(..., help="Output GeoPackage or Parquet path."),
    city_layer: Path | None = typer.Option(
        None,
        help="City polygons for the city-level H+T merge.",
    ),
    cost_table: Path | None = typer.Option(
        None,
        help="Composite H+T CSV for city-level output.",
    ),
    geography: Path | None = typer.Option(
        None,
        help="Geography polygons for block-group summarization.",
    ),
    block_groups: Path | None = typer.Option(None, help="Block-group polygons with H+T measures."),
    value_fields: list[str] = typer.Option(
        ["h_ami", "t_ami", "ht_ami", "t_cost_ami", "h_cost"],
        help="Fields to summarize when building the median H+T geography output.",
    ),
):
    if city_layer and cost_table:
        frame = build_city_cost_layer(read_spatial(city_layer), load_ht_cost_table(cost_table))
        destination = write_spatial(frame, output)
        console.print(f"Wrote housing and transportation city output to {destination}")
        return

    if geography and block_groups:
        frame = summarize_ht_by_geography(
            read_spatial(geography),
            read_spatial(block_groups),
            value_fields,
        )
        destination = write_spatial(frame, output)
        console.print(f"Wrote housing and transportation geography output to {destination}")
        return

    raise typer.BadParameter(
        "Provide either --city-layer and --cost-table, or provide --geography and --block-groups."
    )


@app.command("compare")
def compare_command(
    baseline: str = typer.Option(..., help="Local path or ArcGIS item ID for the baseline frame."),
    candidate: Path = typer.Option(..., help="Local candidate output path."),
    layer_index: int = typer.Option(0, help="Layer index when baseline is an ArcGIS item ID."),
):
    config = load_project_config()
    baseline_frame = _load_comparison_source(baseline, layer_index)
    candidate_frame = read_spatial(candidate)
    join_field = config.publish.boundaries.join_field
    if join_field not in baseline_frame.columns or join_field not in candidate_frame.columns:
        join_field = "geoname"
    summary = compare_frames(baseline_frame, candidate_frame, join_field=join_field)
    console.print(summary)


@publish_app.command("performance-metrics")
def publish_performance_metrics_command(
    input_path: Path = typer.Option(..., "--input", help="Local build output to publish."),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Validate the local publish file and show the target without publishing.",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        help="Actually publish to ArcGIS Online.",
    ),
):
    if dry_run and yes:
        raise typer.BadParameter("Use either --dry-run or --yes, not both.")

    config = load_project_config()
    frame = read_spatial(input_path)
    plan = build_publish_plan(
        frame,
        title=config.publish.output_item.title,
        tags=config.publish.output_item.tags,
        existing_item_id=config.publish.output_item.existing_item_id,
    )

    if dry_run:
        _print_publish_plan(plan, input_path)
        return

    if not yes:
        raise typer.BadParameter(
            "Publishing is disabled by default. "
            "Re-run with --dry-run or --yes after validating the local output."
        )

    if not publish_enabled():
        raise typer.BadParameter(
            "Publishing is blocked because ARCGIS_PUBLISH_ENABLED is not true in the environment."
        )

    gis = connect_gis(load_arcgis_settings())
    result = publish_geodataframe(
        frame,
        gis=gis,
        title=config.publish.output_item.title,
        tags=config.publish.output_item.tags,
        existing_item_id=config.publish.output_item.existing_item_id,
    )
    console.print(result)


@docs_app.command("render")
def render_docs():
    subprocess.run(["quarto", "render"], check=True, cwd=project_root())
    console.print("Rendered Quarto documentation.")


def _print_publish_plan(plan, input_path: Path) -> None:
    table = Table(title="Publish dry run")
    table.add_column("Field")
    table.add_column("Value")
    table.add_row("Input path", str(input_path))
    table.add_row("Target title", plan.title)
    table.add_row("Publish mode", plan.mode)
    table.add_row("Existing item id", plan.existing_item_id or "<create new item>")
    table.add_row("Join field", plan.join_field)
    table.add_row("Geometry column", plan.geometry_column)
    table.add_row("Row count", str(plan.row_count))
    table.add_row("Column count", str(plan.column_count))
    table.add_row("Tags", ", ".join(plan.tags))
    console.print(table)
    console.print("No ArcGIS Online changes were made.")


def _load_comparison_source(source: str, layer_index: int):
    path = Path(source)
    if path.exists():
        return read_spatial(path)
    gis = connect_gis(load_arcgis_settings())
    layer = get_item_layer(gis, source, layer_index)
    return feature_layer_to_frame(layer)


if __name__ == "__main__":
    app()

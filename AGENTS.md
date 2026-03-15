# AGENTS.md

This repository is being modernized on the `modernize-engine` branch.

## Branch Policy
- `main` remains the stable legacy production branch until parity is demonstrated.
- `modernize-engine` is the integration branch for the redesign.
- All modernization PRs target `modernize-engine`, not `main`.

## Repo Map
- `src/regional_metrics/` contains production Python code.
- `config/` contains the YAML-backed metric catalog and publish settings.
- `docs/` contains Quarto maintainer documentation.
- `tests/` contains unit and integration-style tests.
- `legacy/` contains the previous ArcGIS Pro and notebook-centric implementation for parity reference.
- `Inputs/` and `Logs/` remain available as legacy source artifacts and run logs until migration is complete.

## Engineering Rules
- Do not add new `arcpy` dependencies.
- Use `arcgis` only for ArcGIS Online/Enterprise service access and hosted layer publishing.
- Keep analysis and transformation logic in `src/`, not in notebooks.
- Prefer GeoParquet for intermediates and GeoPackage for maintained spatial outputs.
- Any new metric or pipeline change must update code, config, docs, and tests together.

## Migration Expectations
- Preserve existing output schema and geography grain while the dashboard depends on it.
- Validate new pipeline outputs against the legacy hosted layer or exported snapshots.
- Keep legacy assets readable until the replacement workflow is verified.

# DATA-Regional-Metrics-Dashboard

This repository is being modernized into a maintainable Python and Quarto project on the `modernize-engine` branch.

## Current Status
- Legacy workflows now live under [`legacy/`](/d:/GitHub/DATA-Regional-Metrics-Dashboard/legacy).
- The new package lives under [`src/regional_metrics/`](/d:/GitHub/DATA-Regional-Metrics-Dashboard/src/regional_metrics).
- Quarto maintainer documentation lives under [`docs/`](/d:/GitHub/DATA-Regional-Metrics-Dashboard/docs).
- YAML configuration for metrics and publishing lives under [`config/`](/d:/GitHub/DATA-Regional-Metrics-Dashboard/config).

## Goals
- Replace `arcpy`-based analysis with open-source geospatial tooling.
- Keep the `arcgis` Python module only at the service I/O and publishing boundaries.
- Preserve output parity for the `WFRC_PerformanceMetrics` hosted layer.
- Document the project and operational workflows with Quarto.

## Quick Start
```bash
uv sync
uv run regional-metrics --help
quarto render
```

## Legacy Reference
- Legacy metrics script: [`legacy/calc_metrics.py`](/d:/GitHub/DATA-Regional-Metrics-Dashboard/legacy/calc_metrics.py)
- Legacy housing and transportation notebooks: [`legacy/housing-plus-transportation/`](/d:/GitHub/DATA-Regional-Metrics-Dashboard/legacy/housing-plus-transportation)
- Legacy archive notebooks: [`legacy/archive/`](/d:/GitHub/DATA-Regional-Metrics-Dashboard/legacy/archive)
- Knowledge transfer document: [`WFRC Dashboard Integration Knowledge Transfer.docx`](/d:/GitHub/DATA-Regional-Metrics-Dashboard/WFRC%20Dashboard%20Integration%20Knowledge%20Transfer.docx)

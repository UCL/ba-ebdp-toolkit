# ebdp-lightweight

Lightweight modelling workflow for EBDP TWIN2EXPAND project for evidence based approaches to urban design and planning.

## Development

Project configuration is managed using a `pyproject.toml` file. [`pdm`](https://github.com/pdm-project/pdm) is suggested for installation and management of the packages and related upgrades. For example: `pdm install` will install packages listed in the `pyproject.toml` file and creates a self-contained development environment in a `.venv` folder.

## Data Loading

See the [data_loading.md](data_loading.md) markdown file for data loading guidelines.

## Licenses

This repo depends on copy-left open source packages licensed as AGPLv3 and therefore adopts the same license. This is also in keeping with the intention of the TWIN2EXPAND project to create openly reproducible workflows.

The Overture Maps data source is licensed [Community Data License Agreement – Permissive, Version 2.0](https://cdla.dev) with some layers licensed as [Open Data Commons Open Database License](https://opendatacommons.org/licenses/odbl/).

## Data sources

- [Road graph of the streets in Barcelona City](https://opendata-ajuntament.barcelona.cat/data/en/dataset/mapa-graf-viari-carrers-wms)

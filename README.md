# ebdp-lightweight

Lightweight modelling workflow for EBDP TWIN2EXPAND project

## Development

Project configuration is managed using a `pyproject.toml` file. [`pdm`](https://github.com/pdm-project/pdm) is suggested for installation and management of the packages and related upgrades. For example: `pdm install` will install packages listed in the `pyproject.toml` file and creates a self-contained development environment in a `.venv` folder.

## Data

The data source is tentatively [Overture Maps](https://overturemaps.org). This is likely to be preferable over OpenStreetMap due to a degree of data verification and consistency. However, given its newness there may be issues in coverage etc.

## Licenses

This repo depends on copy-left open source packages licensed as AGPLv3 and therefore adopts the same license.

The Overture Maps data source is licensed [Community Data License Agreement – Permissive, Version 2.0](https://cdla.dev) with some layers licensed as [Open Data Commons Open Database License](https://opendatacommons.org/licenses/odbl/).

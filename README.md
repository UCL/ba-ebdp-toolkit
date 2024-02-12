# ebdp-lightweight

Lightweight modelling workflow for EBDP TWIN2EXPAND project for evidence based approaches to urban design and planning.

## Development

Project configuration is managed using a `pyproject.toml` file. [`pdm`](https://github.com/pdm-project/pdm) is suggested for installation and management of the packages and related upgrades. For example: `pdm install` will install packages listed in the `pyproject.toml` file and creates a self-contained development environment in a `.venv` folder.

## Data Loading

See the [data_loading.md](data_loading.md) markdown file for data loading guidelines.

## Licenses

This repo depends on copy-left open source packages licensed as AGPLv3 and therefore adopts the same license. This is also in keeping with the intention of the TWIN2EXPAND project to create openly reproducible workflows.

The Overture Maps data source is licensed [Community Data License Agreement – Permissive, Version 2.0](https://cdla.dev) with some layers licensed as [Open Data Commons Open Database License](https://opendatacommons.org/licenses/odbl/). OpenStreetMap data is [© OpenStreetMap contributors](https://osmfoundation.org/wiki/Licence/Attribution_Guidelines#Attribution_text)

## Data sources

- [Road graph of the streets in Barcelona City](https://opendata-ajuntament.barcelona.cat/data/en/dataset/mapa-graf-viari-carrers-wms)

## Google Cloud DB Connections

- If using a static IP please notify the UCL team, who will then add your IP address to the whitelisted domains. You will then be able to connect to the DB using a specified username and password.
- If you do not have access to a static IP address then you need to connect with the GCP Cloud SQL Proxy. Instructions for installation of the utility are available [here](https://cloud.google.com/sql/docs/postgres/sql-proxy). Please notify the UCL team who will walk you through the steps.

## TODO

- iter bounds and compute building and blocks metrics

## OSM Selection

- [https://handbook.geospatial.psu.edu/sites/default/files/capstone/Stratman_Capstone_Final_Paper_20150503.pdf]()
- [https://www.nature.com/articles/s41467-023-39698-6]()

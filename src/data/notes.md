# Loading Notes

The data source is tentatively [Overture Maps](https://overturemaps.org). This is likely to be preferable over OpenStreetMap due to a degree of data verification and consistency. However, given its newness there may be issues in coverage etc.

The data loading scripts fetch data from Overture Maps' storage services. This is done with the use of DuckDB. Note that DuckDB's functionality is currently limited, so filtering by extents can be slow because, for example, it appears to first retrieve all data and then filter.

Not all parquet data types map nicely to GPKG, so the data tables are first described, and those with lists, maps, or other complex data types are converted to JSON to retain the information.

## Projection

DuckDB doesn't set projection so set manually, so this needs to be set manually with `ogr2ogr`.

For example:

```bash
ogr2ogr -progress -a_srs EPSG:4326 temp/places_4326.gpkg temp/places.gpkg
```

## Clipping

Use `ogr2ogr` to clip the datasets, for example:

```bash
ogr2ogr -progress -spat -12.421470912798974 33.226730183416954 45.535158355759435 71.13547646352613 temp/places_eu.gpkg temp/places_4326.gpkg
```

## Places (POI)

This is a points of interest dataset.

See the `data_load_places.py` script.

The download size for all places data is around 30GB.

## Streets

This is a streets dataset consisting of a nodes and edges layer.

See the `data_load_street.py` script.

The download size is for nodes and for edges.

## Buildings

## Boundaries

- [EU Urban Clusters](https://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/population-distribution-demography/clusters)
- [EU High Density](https://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/population-distribution-demography/clusters)
- [Urban Morphological Zones]()

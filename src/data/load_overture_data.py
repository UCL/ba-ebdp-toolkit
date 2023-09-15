"""
DuckDB doesn't set projection so set manually; so, if wanted, these need to be set manually with `ogr2ogr`, for example:

```bash
ogr2ogr -progress -a_srs EPSG:4326 temp/places_4326.gpkg temp/places.gpkg
```

`ogr2ogr` can likewise be used if clipping the datasets, for example:

```bash
ogr2ogr -progress -spat -12.421470912798974 33.226730183416954 45.535158355759435 71.13547646352613 temp/places_eu.gpkg temp/places_4326.gpkg
```

"""
# iter bounds
# clip downloads
# upload

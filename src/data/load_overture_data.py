"""
DuckDB doesn't set projection so set manually; so, if wanted, these need to be set manually with `ogr2ogr`, for example:

```bash
ogr2ogr -progress -a_srs EPSG:4326 temp/places_4326.gpkg temp/places.gpkg
```

`ogr2ogr` can likewise be used if clipping the datasets, for example:

```bash
ogr2ogr -progress -spat -12.421470912798974 33.226730183416954 45.535158355759435 71.13547646352613 temp/places_eu.gpkg temp/places_4326.gpkg
``` 
ogr2ogr -progress -spat 33.17351357266375 34.998819726131856 33.52249232583958 35.28360861597212 temp/cyprus_places.gpkg temp/eu_places.gpkg
ogr2ogr -progress -spat 33.17351357266375 34.998819726131856 33.52249232583958 35.28360861597212 temp/cyprus_edges.gpkg temp/eu_edges.gpkg
ogr2ogr -progress -spat 33.17351357266375 34.998819726131856 33.52249232583958 35.28360861597212 temp/cyprus_nodes.gpkg temp/eu_nodes.gpkg
ogr2ogr -progress -spat 33.17351357266375 34.998819726131856 33.52249232583958 35.28360861597212 temp/cyprus_buildings.gpkg temp/eu_buildings.gpkg

"""

# iter bounds
# clip downloads
# upload

# Loading Notes

The data source is a combination of EU Copernicus data and [Overture Maps](https://overturemaps.org), which largely resembles [OpenStreetMap](https://www.openstreetmap.org). Overture intends to provide a higher degree of data verification and uses fixed releases.

## PostGIS

Data storage and sharing is done with `postgres` and `postGIS`.

The database adminstrators will enable the `postGIS` and other basic extensions per below.

```sql
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_raster;
SELECT postgis_gdal_version();
-- set output rasters to true if necessary
SHOW postgis.enable_outdb_rasters;
SET postgis.enable_outdb_rasters TO True;
-- enable GDAL drivers if necessary
ALTER DATABASE t2e SET postgis.gdal_enabled_drivers TO 'GTiff';
-- reconnect to refresh
SELECT short_name FROM ST_GDALDrivers();
```

## Create a working schema

```sql
CREATE SCHEMA IF NOT EXISTS eu;
```

## Boundaries

Boundaries are extracted from the [2021 Urban Centres / High Density Clusters](https://ec.europa.eu/eurostat/web/gisco/geodata/population-distribution/clusters) dataset. This is 1x1km raster with high density clusters [described as](https://ec.europa.eu/eurostat/statistics-explained/index.php?title=Territorial_typologies#Typologies) contiguous 1km2 cells with at least 1,500 residents per km2 and consisting of cumulative urban clusters with at least 50,000 people.

- Download the dataset from the above link.
- From the terminal, prepare and upload to PostGIS, substituting the database parameters as required:

```bash
# update the path to your postgres bin directory
/Applications/Postgres.app/Contents/Versions/15/bin/raster2pgsql -d -s 3035 -I -C -M -F -t auto HDENS_CLST_2021.tif eu.hdens_clusters > output.sql
# update the port if necessary
/Applications/Postgres.app/Contents/Versions/15/bin/psql -h localhost -U editor -d t2e -W -p 5435 -f output.sql
```

- Run the `generate_boundary_polys.py` script to generate the vector boundaries from the raster source. Provide the source schema name and the table name of the high density cluster raster per above upload. The output polygons will be generated to the `eu` schema in tables named `bounds`, `unioned_bounds_2000`, and `unioned_bounds_10000`. This script will automatically remove boundaries intersecting the UK.

```bash
python -m src.data.generate_boundary_polys eu hdens_clusters
```

## Census Data (2021)

GeoStat Census data for 2021 is [downloaded from](https://ec.europa.eu/eurostat/web/gisco/geodata/population-distribution/geostat). These census statistics are aggregated to 1km2 cells.

- Download the census ZIP dataset for Version 2021 (16 June 2024).
- From the terminal, prepare and upload to PostGIS, substituting the database parameters as required:

```bash
/Applications/Postgres.app/Contents/Versions/15/bin/ogr2ogr -f "PostgreSQL" PG:"host=localhost dbname=t2e user=editor port=5435 password='<insert>'" "temp/Eurostat_Census-GRID_2021_V2-0/ESTAT_Census_2021_V2.gpkg" -nln eu.stats -progress
```

## Urban Atlas

[urban atlas](https://land.copernicus.eu/local/urban-atlas/urban-atlas-2018) (~37GB vectors)

- Run the `load_urban_atlas_blocks.py` script to upload the data. Provide the path to the input directory with the zipped data files. The blocks will be loaded to the `blocks` table in the `eu` schema.

```bash
python -m src.data.load_urban_atlas_blocks "./temp/urban atlas"
```

## Tree cover

[Tree cover](https://land.copernicus.eu/local/urban-atlas/street-tree-layer-stl-2018) (~36GB vectors).

- Run the `load_urban_atlas_trees.py` script to upload the data. Provide the path to the input directory with the zipped data files. The trees will be loaded to the `trees` table in the `eu` schema.

```bash
python -m src.data.load_urban_atlas_trees "./temp/urban atlas trees"
```

## Ingesting Overture data

Upload overture data. Pass the `--drop` flag to drop and therefore replace existing tables. The loading scripts will otherwise track which boundary extents are loaded and will resume if interrupted. The tables will be uploaded to the `overture` schema.

Places:

```bash
python -m src.data.ingest_overture_places
```

Places:

```bash
python -m src.data.ingest_overture_infrast
```

Buildings:

```bash
python -m src.data.ingest_overture_buildings
```

Network (cleaned) - in this case there is an optional parallel workers argument:

```bash
python -m src.data.ingest_overture_networks all --parallel_workers 4
```

### Building Heights

[Digital Height Model](https://land.copernicus.eu/local/urban-atlas/building-height-2012) (~ 1GB raster).

- Run the `load_bldg_hts_raster.py` script to upload the building heights data. Provide the path to the input directory with the zipped data files. Use the optional argument `--bin_path` to provide a path to the `bin` directory for your `postgres` installation. The raster will be loaded to the `bldg_hts` table in the `eu` schema.

```bash
python -m src.data.load_bldg_hts_raster "./temp/Digital height Model EU" --bin_path /Applications/Postgres.app/Contents/Versions/15/bin/
```

## Metrics

Once the datasets are uploaded, boundaries extracted, and networks prepared, it becomes possible to compute the metrics.

`python -m src.processing.generate_metrics all`

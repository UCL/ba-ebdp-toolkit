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

Network:

```bash
python -m src.data.ingest_overture_networks
```

Places:

```bash
python -m src.data.ingest_overture_places
```

Buildings:

```bash
python -m src.data.ingest_overture_buildings
```

## Network preparation and cleaning

Subsequent steps make use of a cleaned network representation. Run the `generate_networks.py` script to generate the cleaned network, which will be saved to the `overture` schema. Pass the optional `parallel_workers` argument to specify the number of CPU cores to use.

```bash
python -m src.processing.generate_networks all --parallel_workers 2
```

## Metrics

Once the datasets are uploaded, boundaries extracted, and networks prepared, it is possible to start computing the metrics.

Centrality:

`python -m src.processing.metrics_centrality all`

Green space and trees:

`python -m src.processing.metrics_green all`

Landuses:

`python -m src.processing.metrics_landuses all`

Population:

`python -m src.processing.metrics_population all`

Building and block morphologies:

`python -m src.processing.metrics_morphology all`

### Building Heights

[Digital Height Model](https://land.copernicus.eu/local/urban-atlas/building-height-2012) (~ 1GB raster).

- Run the `load_bldg_hts_raster.py` script to upload the building heights data. Provide the path to the input directory with the zipped data files. Use the optional argument `--bin_path` to provide a path to the `bin` directory for your `postgres` installation. The raster will be loaded to the `bldg_hts` table in the `eu` schema.

```bash
python -m src.data.load_bldg_hts_raster "./temp/Digital height Model EU" --bin_path /Applications/Postgres.app/Contents/Versions/15/bin/
```

Set building heights via SQL:

```sql
ALTER TABLE overture.overture_buildings ADD COLUMN rast_ht real;

-- Update rast_ht with average raster value where raster intersects building geometry
UPDATE overture.overture_buildings p
SET rast_ht = (
    -- Average value of raster where it intersects building
    SELECT AVG(ST_Value(r.rast, ST_Intersection(r.rast, p.geom)))
    FROM eu.bldg_hts r
    WHERE ST_Intersects(r.rast, p.geom)
)
WHERE EXISTS (
    SELECT 1 FROM eu.bldg_hts r
    WHERE ST_Intersects(r.rast, p.geom)
);

-- Step 1: Find the nearest building with a non-NULL rast_ht for each building with NULL rast_ht
WITH nearest_building AS (
    SELECT
        p.fid AS target_fid,  -- Target building (with NULL rast_ht)
        nb.fid AS nearest_fid,  -- Nearest adjacent building
        nb.rast_ht AS nearest_rast_ht
    FROM overture.overture_buildings p
    JOIN overture.overture_buildings nb ON nb.rast_ht IS NOT NULL  -- Only consider buildings with valid rast_ht
    WHERE p.rast_ht IS NULL  -- Only update buildings where rast_ht is NULL
    ORDER BY ST_Distance(p.geom, nb.geom)  -- Order by distance to find the nearest building
    LIMIT 1  -- Only pick the closest building
)
-- Step 2: Update rast_ht for the buildings with NULL rast_ht based on the nearest building's rast_ht
UPDATE overture.overture_buildings p
SET rast_ht = nb.nearest_rast_ht
FROM nearest_building nb
WHERE p.fid = nb.target_fid;
```

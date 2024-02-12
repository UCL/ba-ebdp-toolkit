# Loading Notes

The data source is a combination of EU Copernicus data, [OpenStreetMap](https://www.openstreetmap.org), and [Overture Maps](https://overturemaps.org). Overture is a relatively new dataset which intends to provide a higher degree of data verification. However, OpenStreetMap currently remains preferable for land-use information.

## PostGIS

Data storage and sharing is done with `postgres` and `postGIS`.

The database adminstrators will ensure that the `postGIS` and other basic extensions are enabled per below.

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

There are several potential EU boundaries datasets:

- [2018 Urban Clusters](https://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/population-distribution-demography/clusters) - The 2018 urban clusters dataset is 1x1km raster based and broadly reflects the 2006 UMZ vector extents. These are [described as](https://ec.europa.eu/eurostat/statistics-explained/index.php?title=Territorial_typologies#Typologies) consisting of 1km2 grid cells with at least 300 people and a contiguous population of 5,000. The high density clusters are [described as](https://ec.europa.eu/eurostat/statistics-explained/index.php?title=Territorial_typologies#Typologies) contiguous 1km2 cells with at least 1,500 residents per km2 and consisting of cumulative urban clusters with at least 50,000 people.
- [2006 UMZ vector](https://www.eea.europa.eu/en/datahub/datahubitem-view/6e5d9b0d-a448-4c73-b008-bdd98a3cf214) - 2006 UMZ is vector based and broadly reflects the urban clusters dataset but with a greater resolution. It is not being used due to being ~17yrs old.
- [2012 UMZ](https://www.eea.europa.eu/en/datahub/datahubitem-view/bf175d04-8441-4ed2-b089-9636ecf19353) - 2012 UMZ datasets are clipped to administrative regions and therefore not useful for defining urban extents.

Of these, the raster 2018 high density clusters is used

- Download the dataset from the above link.
- From the terminal, prepare and upload to PostGIS, substituting the host, user, and database parameters as required:

```bash
# update the path to your postgres bin directory
/Applications/Postgres.app/Contents/Versions/15/bin/raster2pgsql -d -s 3035 -I -C -M -F -t auto HDENS_CLST_2018.tif eu.hdens_clusters > output.sql
# update the port if necessary
/Applications/Postgres.app/Contents/Versions/15/bin/psql -h localhost -U editor -d t2e -W -p 5435 -f output.sql
```

- Run the `generate_boundary_polys.py` script to generate the vector boundaries from the raster source. Provide the source schema name and the table name of the high density cluster raster per above upload. The output polygons will be generated to the `eu` schema in tables named `bounds`, `unioned_bounds_2000`, and `unioned_bounds_10000`. This script will automatically remove boundaries intersecting the UK.

```bash
python -m src.data.generate_boundary_polys eu hdens_clusters
```

## Population Density

[Eurostat census grid population count](https://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/population-distribution-demography/geostat#geostat11). This is count per km2. (~1.3GB)

- Download the dataset from the above link.
- From the terminal, prepare and upload to PostGIS, substituting the host, user, and database parameters as required:

```bash
# update the path to your postgres bin directory
/Applications/Postgres.app/Contents/Versions/15/bin/raster2pgsql -d -s 3035 -I -C -M -F -t auto ESTAT_OBS-VALUE-T_2021_V1-0.tiff eu.pop_dens > output.sql
# update the port if necessary
/Applications/Postgres.app/Contents/Versions/15/bin/psql -h localhost -U editor -d t2e -W -p 5435 -f output.sql
```

## Census

[census](https://ec.europa.eu/eurostat/web/population-demography/population-housing-censuses/information-data)
[census hub](https://ec.europa.eu/CensusHub2/query.do?step=selectHyperCube&qhc=false)

- The 2021 census adopts the 1km2 grid, but results are so far only released for population counts.
- Other data will be released end March 2024.

## Urban Atlas

[urban atlas](https://land.copernicus.eu/local/urban-atlas/urban-atlas-2018) (~37GB vectors)

- Run the `load_urban_atlas.py` script to upload the data. Provide the path to the input directory with the zipped data files. The blocks will be loaded to the `blocks` table in the `eu` schema.

```bash
python -m src.data.load_urban_atlas "./temp/urban atlas"
```

## Tree cover

[Tree cover](https://land.copernicus.eu/local/urban-atlas/street-tree-layer-stl-2018) (~36GB vectors).

- Run the `load_urban_atlas_trees.py` script to upload the data. Provide the path to the input directory with the zipped data files. The trees will be loaded to the `trees` table in the `eu` schema.

```bash
python -m src.data.load_urban_atlas_trees "./temp/urban atlas trees"
```

## Downloading Overture data

The workflow for ingesting Overture data entails a bulk-download for the extent of the EU using DuckDB. To proceed with a bulk data download, run the `download_overture_bbox.py` script from the project folder (the folder containing `src`). This will concurrently download the Overture places, nodes, edges, and buildings datasets. The script requires an output directory, a file prefix, and a bounding box within which data will be retrieved. Not all parquet data types map nicely to GPKG, so those with lists, maps, or other complex data types are converted to JSON for storage as strings in GPKG fields.

For example, for loading the EU:

```bash
python -m src.data.download_overture_bbox ./temp eu -12.4214 33.2267 45.5351 71.1354
```

The bulk download is currently a slow process (the downloads can take several days). This is because DuckDB's functionality is presently limited and the Overture data source uses parquet instead of geoparquet, which would otherwise afford more efficient spatial queries. These are likely to be improved in due course.

The download sizes for the EU are:

- Places dataset: 5.58GB
- Nodes dataset: 19.27GB
- Edges dataset: 50.82GB
- Buildings dataset: 81.04GB

## Ingesting Overture data

Upload overture network data (nodes and edges) using the `ingest_networks.py` script. Pass the `--drop` flag to drop and therefore replace existing tables. The loading scripts will otherwise track which boundary extents are loaded and will resume if interrupted. The tables will be uploaded to the `overture` schema.

```bash
python -m src.data.ingest_overture_networks 'temp/eu_nodes.gpkg' 'temp/eu_edges.gpkg'
```

Places is similar:

```bash
python -m src.data.ingest_overture_places 'temp/eu_places.gpkg'
```

As is buildings:

```bash
python -m src.data.ingest_overture_buildings 'temp/eu_buildings.gpkg'
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

## Pending

### Buildings and blocks

Extract building and block characteristics.

### Building Heights

[Digital Height Model](https://land.copernicus.eu/local/urban-atlas/building-height-2012) (~ 1GB raster).

> NOTE: This workflow assumes running the model for the entirety of the EU. If running a smaller extent, then only the necessary files need to be downloaded and can be uploaded using a similar process to the Population Density example.

- Run the `load_bldg_hts_raster.py` script to upload the building heights data. Provide the path to the input directory with the zipped data files. Use the optional argument `--bin_path` to provide a path to the `bin` directory for your `postgres` installation. The raster will be loaded to the `bldg_hts` table in the `eu` schema.

```bash
python -m src.data.load_bldg_hts_raster "./temp/Digital height Model EU" --bin_path /Applications/Postgres.app/Contents/Versions/15/bin/
```

Possible SQL strategy:

```sql
-- uses overture buildings table directly
ALTER TABLE {bldg_hts_table_name}
    ADD COLUMN max_rast_ht real;
WITH ClippedRasters AS (
    SELECT
        p.fid AS region_fid,
        ST_Clip(r.rast, ST_Transform(p.geom, 3035)) AS clipped_rast
    FROM
        {input_rast_bldg_hts_table} r
    JOIN {bldg_hts_table_name} p ON ST_Intersects(ST_Transform(p.geom, 3035), r.rast)
), RasterValues AS (
    SELECT
        region_fid,
        unnest((ST_DumpValues(clipped_rast)).valarray) as un
    FROM
        ClippedRasters
), MaxRasterValues AS (
    SELECT
        region_fid,
        MAX(un) as max_val
    FROM RasterValues
    WHERE un IS NOT NULL
    GROUP BY region_fid
)
UPDATE {bldg_hts_table_name} ob
    SET max_rast_ht = mv.max_val
    FROM MaxRasterValues mv
    WHERE mv.region_fid = ob.fid;
```

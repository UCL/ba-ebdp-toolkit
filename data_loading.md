# Loading Notes

The data source is a combination of [Copernicus](), [OpenStreetMap](https://www.openstreetmap.org), and [Overture Maps](https://overturemaps.org). Overture is a relatively new dataset which intends to provide a higher degree of data verification. However, OpenStreetMap currently remains preferable for land-use information.

## PostGIS

Data storage and sharing is done with `postgres` and `postGIS`.

Ensure that the `postGIS` and other basic extensions are enabled.

It may be necessary to configure raster support, for example:

```sql
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_raster;
SELECT postgis_gdal_version();
-- set output rasters to true if necessary
SHOW postgis.enable_outdb_rasters;
SET postgis.enable_outdb_rasters TO True;
-- enable GDAL drivers if necessary
ALTER DATABASE t2e SET postgis.gdal_enabled_drivers TO 'GTiff';
SELECT pg_reload_conf();
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
raster2pgsql -d -s 3035 -I -C -M -F -t auto HDENS_CLST_2018.tif eu.hdens_clusters > output.sql
psql -h <host> -U <user> -d <db> -W -f output.sql
```

- Run the `prepare_boundary_polys.py` script to generate the vector boundaries from the raster source. Provide the schema name, the table name of the high density clusters per above upload, and the output table name for the polygon boundaries. For example:

```bash
python -m src.data.prepare_boundary_polys eu hdens_clusters bounds
```

## Population Density

[Eurostat census grid population count](https://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/population-distribution-demography/geostat#geostat11). This is count per km2. (~1.3GB)

- Download the dataset from the above link.
- From the terminal, prepare and upload to PostGIS, substituting the host, user, and database parameters as required:

```bash
raster2pgsql -d -s 3035 -I -C -M -F -t auto ESTAT_OBS-VALUE-T_2021_V1-0.tiff eu.pop_dens > output.sql
psql -h <host> -U <user> -d <db> -W -f output.sql
```

## Census

[census](https://ec.europa.eu/eurostat/web/population-demography/population-housing-censuses/information-data)
[census hub](https://ec.europa.eu/CensusHub2/query.do?step=selectHyperCube&qhc=false)

- The 2021 census adopts the 1km2 grid, but results are so far only released for population counts.
- Other data will be released end March 2024.

## Urban Atlas

[urban atlas](https://land.copernicus.eu/local/urban-atlas/urban-atlas-2018) (~37GB vectors)

- Run the `load_urban_atlas.py` script to upload the data. Provide the path to the input directory with the zipped data files. Also specify the schema, boundaries table name, and the output table name for the new urban atlas blocks table. For example:

```bash
python -m src.data.load_urban_atlas "./temp/urban atlas" eu bounds blocks
```

## Tree cover

[Tree cover](https://land.copernicus.eu/local/urban-atlas/street-tree-layer-stl-2018) (~36GB vectors).

- Run the `load_urban_atlas_trees.py` script to upload the data. Provide the path to the input directory with the zipped data files. Also specify the schema, boundaries table name, and the output table name for the new trees table. For example:

```bash
python -m src.data.load_urban_atlas_trees "./temp/urban atlas trees" eu bounds trees
```

## Building Heights

[Digital Height Model](https://land.copernicus.eu/local/urban-atlas/building-height-2012) (~ 1GB raster).

> NOTE: This workflow assumes you're running the model for the entirety of the EU. If running a smaller extent, then only the necessary files need to be downloaded and can be uploaded using a similar process to the Population Density example.

- Run the `load_bldg_hts_raster.py` script to upload the data. Provide the path to the input directory with the zipped data files. Also specify the schema, table name, and the optional argument `--bin_path` to provide a path to the `bin` directory for your `postgres` installation. For example:

```bash
python -m src.data.load_bldg_hts_raster "./temp/Digital height Model EU" eu bldg_hts --bin_path /Applications/Postgres.app/Contents/Versions/16/bin/
```

## Downloading Overture data

The workflow for ingesting Overture data entails a bulk-download for the extent of the EU using DuckDB. To proceed with a bulk data download, run the `load_data_bbox.py` script from the project folder (the folder containing `src`). This will concurrently download the Overture places, nodes, edges, and buildings datasets. The script requires an output directory, a file prefix, and a bounding box within which data will be retrieved. Not all parquet data types map nicely to GPKG, so those with lists, maps, or other complex data types are converted to JSON for storage as strings in GPKG fields.

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

Upload overture datasets using the `load_overture_data.py` script. Provide either a target boundary table FID value or else use 'all' to load all FIDs.

```bash
python -m src.data.load_overture_data 783 load_overture_networks eu bounds overture geom_10000 --overture_nodes_path='temp/eu_nodes.gpkg' --overture_edges_path='temp/eu_edges.gpkg' --overwrite=True
python -m src.data.load_overture_data 783 load_overture_places eu bounds overture geom_2000 --overture_places_path='temp/eu_places.gpkg' --overwrite=True
python -m src.data.load_overture_data 783 load_overture_buildings eu bounds overture geom_2000  --overture_buildings_path='temp/eu_buildings.gpkg' --overwrite=True
```

- Automate building heights from raster

```sql
-- uses overture buildings table directly
ALTER TABLE {bldg_hts_table_name}
    ADD COLUMN max_rast_ht real;
WITH ClippedRasters AS (
    SELECT
        p.fid AS region_id,
        ST_Clip(r.rast, ST_Transform(p.geom, 3035)) AS clipped_rast
    FROM
        {input_rast_bldg_hts_table} r
    JOIN {bldg_hts_table_name} p ON ST_Intersects(ST_Transform(p.geom, 3035), r.rast)
), RasterValues AS (
    SELECT
        region_id,
        unnest((ST_DumpValues(clipped_rast)).valarray) as un
    FROM
        ClippedRasters
), MaxRasterValues AS (
    SELECT
        region_id,
        MAX(un) as max_val
    FROM RasterValues
    WHERE un IS NOT NULL
    GROUP BY region_id
)
UPDATE {bldg_hts_table_name} ob
    SET max_rast_ht = mv.max_val
    FROM MaxRasterValues mv
    WHERE mv.region_id = ob.fid;
```

# Loading Notes

The data source is a combination of [OpenStreetMap](https://www.openstreetmap.org) and [Overture Maps](https://overturemaps.org). Overture is a relatively new dataset which intends to provide a higher degree of data verification. However, OpenStreetMap currently remains preferable for land-use information.

## Overture data downloads

The workflow for ingesting Overture data entails a bulk-download for the extent of the EU using DuckDB. To proceed with a bulk data download, run the `load_data_bbox.py` script from the project folder (the folder containing `src`). This will concurrently download the Overture places, nodes, edges, and buildings datasets. The script requires an output directory, a file prefix, and a bounding box within which data will be retrieved. Not all parquet data types map nicely to GPKG, so those with lists, maps, or other complex data types are converted to JSON for storage as strings in GPKG fields.

For example, for loading the EU:

```bash
python -m src.data.load_data_bbox ./temp eu -12.4214 33.2267 45.5351 71.1354
```

The bulk download is currently a slow process (the downloads can take several days). This is because DuckDB's functionality is presently limited and the Overture data source uses parquet instead of geoparquet, which would otherwise afford more efficient spatial queries. These are likely to be improved in due course.

The download sizes for the EU are:

- Places dataset: 5.58GB
- Nodes dataset: 19.27GB
- Edges dataset: 50.82GB
- Buildings dataset: 81.04GB

DuckDB doesn't set projection so set manually; so, if wanted, these need to be set manually with `ogr2ogr`, for example:

```bash
ogr2ogr -progress -a_srs EPSG:4326 temp/places_4326.gpkg temp/places.gpkg
```

`ogr2ogr` can likewise be used if clipping the datasets, for example:

```bash
ogr2ogr -progress -spat -12.421470912798974 33.226730183416954 45.535158355759435 71.13547646352613 temp/places_eu.gpkg temp/places_4326.gpkg
```

## PostGIS

Data storage and sharing is done with `postgres` and `postGIS`.

Ensure that the `postGIS` and other basic extensions are enabled.

It may be necessary to configure raster support, for example:

```sql
SELECT postgis_gdal_version();
-- set output rasters to true if necessary
SHOW postgis.enable_outdb_rasters;
SET postgis.enable_outdb_rasters TO True;
-- enable GDAL drivers if necessary
ALTER DATABASE my_db SET postgis.gdal_enabled_drivers TO 'GTiff';
SELECT pg_reload_conf();
SELECT short_name FROM ST_GDALDrivers();
```

## Boundaries

There are several potential EU boundaries datasets:

- [2018 Urban Clusters](https://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/population-distribution-demography/clusters) - The 2018 urban clusters dataset is 1x1km raster based and broadly reflects the 2006 UMZ vector extents. These are [described as](https://ec.europa.eu/eurostat/statistics-explained/index.php?title=Territorial_typologies#Typologies) consisting of 1km2 grid cells with at least 300 people and a contiguous population of 5,000. The high density clusters are [described as](https://ec.europa.eu/eurostat/statistics-explained/index.php?title=Territorial_typologies#Typologies) contiguous 1km2 cells with at least 1,500 residents per km2 and consisting of clusters with at least 50,000 people.
- [2006 UMZ vector](https://www.eea.europa.eu/en/datahub/datahubitem-view/6e5d9b0d-a448-4c73-b008-bdd98a3cf214) - 2006 UMZ is vector based and broadly reflects the urban clusters dataset but with a greater resolution.
- [2012 UMZ](https://www.eea.europa.eu/en/datahub/datahubitem-view/bf175d04-8441-4ed2-b089-9636ecf19353) - 2012 UMZ datasets are clipped to administrative regions and therefore not useful for defining urban extents.

Of these, the raster 2018 high density clusters is used

- Download the dataset from the above link.
- From the terminal, prepare and upload to PostGIS, substituting the host, user, and database parameters as required:

```bash
raster2pgsql -s 3035 -I -C -M HDENS_CLST_2018.tif -F eu.hdens_clusters > output.sql
psql -h <host> -U <user> -d <db> -W -f output.sql
```

- Run the `prepare_boundaries.py` script to generate the vector boundaries from the raster source.

## Population Density

[Eurostat census grid population count](https://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/population-distribution-demography/geostat#geostat11). This is count per km2. (~1.3GB)

## Building Heights

[Digital Height Model](https://land.copernicus.eu/local/urban-atlas/building-height-2012) (~ 1GB raster).

## Tree cover

[Tree cover](https://land.copernicus.eu/local/urban-atlas/street-tree-layer-stl-2018) (~36GB vectors).

## Urban Atlas

[urban atlas](https://land.copernicus.eu/local/urban-atlas/urban-atlas-2018) (~37GB vectors)

## Census

[census](https://ec.europa.eu/eurostat/web/population-demography/population-housing-censuses/information-data)
[census hub](https://ec.europa.eu/CensusHub2/query.do?step=selectHyperCube&qhc=false)

- The 2021 census adopts the 1km2 grid, but results are so far only released for population counts.
- Other data will be released end March 2024.

## PENDING

- Automate import and iteration of clusters or UMZs.
- Automate import and upsampling of census grid population.
- Automate urban atlas import

```sql
delete from {urban_atlas_name} tn
where not exists (
    select 1
    from {boundary_name} bb
    where ST_Intersects(tn.geom, bb.geom)
);
create table {blocks_table_name}
    as select
        id::text,
        fid,
        country,
        fua_name,
        fua_code,
        code_2018,
        class_2018,
        prod_date,
        identifier,
        perimeter,
        area,
        comment,
        pop2018,
        geom
    from {urban_atlas_name}
    where class_2018 = ANY(ARRAY[
        'Airports',
        'Continuous urban fabric (S.L. : > 80%)',
        'Discontinuous dense urban fabric (S.L. : 50% -  80%)',
        'Discontinuous low density urban fabric (S.L. : 10% - 30%)',
        'Discontinuous medium density urban fabric (S.L. : 30% - 50%)',
        'Discontinuous very low density urban fabric (S.L. : < 10%)',
        'Green urban areas',
        'Industrial, commercial, public, military and private units',
        'Isolated structures',
        'Land without current use',
        'Port areas',
        'Sports and leisure facilities'
]);
ALTER TABLE {blocks_table_name} ADD PRIMARY KEY (id);
CREATE INDEX IF NOT EXISTS {blocks_table_name}_geom_gix
    ON {blocks_table_name}
    USING GIST (geom);
```

- Automate building heights from raster

```sql
-- uses overture buildings table directly
ALTER TABLE {bldg_hts_table_name} ob
    ADD COLUMN max_rast_ht real;
WITH ClippedRasters AS (
    SELECT
        p.id AS region_id,
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
    WHERE mv.region_id = ob.id;
```

- automate tree coverage import

```sql
ALTER TABLE {trees_table_name} ADD PRIMARY KEY (id);
CREATE INDEX IF NOT EXISTS {trees_table_name}_geom_gix
    ON {trees_table_name}
    USING GIST (geom);
delete from {trees_table_name} tn
where not exists (
    select 1
    from {boundary_name} bb
    where ST_Intersects(tn.geom, bb.geom);
);
```

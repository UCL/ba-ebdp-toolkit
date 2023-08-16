# Loading Notes

The data source is tentatively [Overture Maps](https://overturemaps.org). This is likely to be preferable over OpenStreetMap due to a degree of data verification and consistency. However, given its newness there may be issues in coverage etc.

The data loading scripts fetch data from Overture Maps' storage services. This is done with the use of DuckDB. Note that DuckDB's functionality is currently limited, so filtering by extents can be slow because, for example, it appears to first retrieve all data and then filter.

Not all parquet data types map nicely to GPKG, so the data tables are first described, and those with lists, maps, or other complex data types are converted to JSON to retain the information.

## Loading

Run the `load_data_bbox.py` script from the project folder (the folder containing `src`). This will concurrently download the Overture places, nodes, edges, and buildings datasets. The script requires an output directory, a file prefix, and a bounding box within which data will be retrieved.

For example, for loading the EU:

```bash
python -m src.data.load_data_bbox ./temp eu -12.4214 33.2267 45.5351 71.1354
```

The download sizes for the EU are:

- Places dataset: 5.58GB
- Nodes dataset: 19.27GB
- Edges dataset: 50.82GB
- Buildings dataset: 81.04GB

## Projection

DuckDB doesn't set projection so set manually, so this may need to be set manually with `ogr2ogr`, though it seems to work fine without this step...

For example:

```bash
ogr2ogr -progress -a_srs EPSG:4326 temp/places_4326.gpkg temp/places.gpkg
```

## Clipping

Use `ogr2ogr` if clipping the datasets, for example:

```bash
ogr2ogr -progress -spat -12.421470912798974 33.226730183416954 45.535158355759435 71.13547646352613 temp/places_eu.gpkg temp/places_4326.gpkg
```

## PostGIS

Data storage and sharing is done with `postgres` and `postGIS`.

Ensure that the `postgis` and other basic extensions are enabled.

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

- [EU Urban Clusters](https://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/population-distribution-demography/clusters)
- [Urban Morphological Zones](https://www.eea.europa.eu/en/datahub/datahubitem-view/bf175d04-8441-4ed2-b089-9636ecf19353)

## Population Density

[Eurostat census grid population count](https://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/population-distribution-demography/geostat#geostat11). This is count per km2. (~1.3GB)

## Building Heights

[Digital Height Model](https://land.copernicus.eu/local/urban-atlas/building-height-2012) (~ 1GB raster).

## Tree cover

[Tree cover](https://land.copernicus.eu/local/urban-atlas/street-tree-layer-stl-2018) (~36GB vectors).

## Urban Atlas

[urban atlas](https://land.copernicus.eu/local/urban-atlas/urban-atlas-2018) (~37GB vectors)

## PENDING

- Automate import and iteration of clusters or UMZs.
- Automate import and upsampling of census grid population.
- Automate urban atlas import

```sql
ALTER TABLE {blocks_table_name} ADD PRIMARY KEY (id);
CREATE INDEX IF NOT EXISTS {blocks_table_name}_geom_gix
    ON {blocks_table_name}
    USING GIST (geom);
delete from {urban_atlas_name} tn
where not exists (
    select 1
    from {boundary_name} bb
    where ST_Intersects(tn.geom, bb.geom)
);
create table {blocks_table_name}
    as select *
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

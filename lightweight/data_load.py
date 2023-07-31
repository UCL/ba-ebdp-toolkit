"""
Loads the overture places dataset via DuckDB.

Numerous errors which seem to stem from complex nested data structures (e.g. brand) which don't necessarily map to OGR
data types. Columns filtered or cast to JSON to work with GeoPackage.

DuckDB doesn't set projection so set manually, e.g.: ogr2ogr -a_srs EPSG:4326 places-4326.gpkg places.gpkg
"""
from __future__ import annotations

import duckdb

# initialise duck db
duckdb.sql(
    """
    INSTALL spatial;
    INSTALL httpfs;
           """
)
# configure
duckdb.sql(
    """
    LOAD spatial;
    LOAD httpfs;
    SET s3_region='us-west-2';
        """
)
duckdb.sql(
    """
           COPY (
    SELECT
        id,
        updatetime,
        version,
        JSON(names) as names,
        JSON(categories) as categories,
        confidence,
        -- JSON(websites) as websites,
        -- JSON(socials) as socials,
        -- JSON(emails) as emails,
        -- JSON(phones) as phones,
        -- JSON(brand) as brand, -- quite complex
        JSON(addresses) as addresses,
        JSON(sources) as sources,
        -- JSON(bbox) as bbox,
        ST_GeomFromWkb(geometry) AS geometry
    FROM read_parquet('s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=places/type=place/*', filename=true, hive_partitioning=1)
) TO './temp/places.gpkg'
WITH (FORMAT GDAL, DRIVER 'GPKG')
"""
)

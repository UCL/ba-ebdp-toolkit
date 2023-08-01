"""
ogr2ogr -progress -a_srs EPSG:4326 temp/buildings_4326.gpkg temp/buildings.gpkg
ogr2ogr -progress -spat -12.421470912798974 33.226730183416954 45.535158355759435 71.13547646352613 temp/buildings_eu.gpkg temp/buildings_4326.gpkg
"""
from __future__ import annotations

import duckdb

if __name__ == "__main__":
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
    description = duckdb.sql(
        """
        DESCRIBE
        SELECT *
        FROM read_parquet('s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=buildings/type=building/*', filename=true, hive_partitioning=1)
        """
    )
    """
    ┌─────────────┬────────────────────────────────────────────────────────────┬
    │ column_name │                        column_type                         │
    │   varchar   │                          varchar                           │
    ├─────────────┼────────────────────────────────────────────────────────────┼
    │ id          │ VARCHAR                                                    │
    │ updatetime  │ VARCHAR                                                    │
    │ version     │ INTEGER                                                    │
    │ names       │ MAP(VARCHAR, MAP(VARCHAR, VARCHAR)[])                      │
    │ level       │ INTEGER                                                    │
    │ height      │ DOUBLE                                                     │
    │ numfloors   │ INTEGER                                                    │
    │ class       │ VARCHAR                                                    │
    │ sources     │ MAP(VARCHAR, VARCHAR)[]                                    │
    │ bbox        │ STRUCT(minx DOUBLE, maxx DOUBLE, miny DOUBLE, maxy DOUBLE) │
    │ geometry    │ BLOB                                                       │
    │ filename    │ VARCHAR                                                    │
    │ theme       │ VARCHAR                                                    │
    │ type        │ VARCHAR                                                    │
    ├─────────────┴────────────────────────────────────────────────────────────┴
    │ 14 rows                                                                   
    └───────────────────────────────────────────────────────────────────────────
    """
    print(description)

    # fetch POI
    duckdb.sql(
        """
            COPY (
        SELECT
            id,
            updatetime,
            version,
            JSON(names) as names,
            level,
            height,
            numfloors,
            class,
            -- JSON(sources) as sources,
            -- JSON(bbox) as bbox,
            ST_GeomFromWkb(geometry) AS geometry
        FROM read_parquet('s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=buildings/type=building/*', filename=true, hive_partitioning=1)
    ) TO './temp/buildings.gpkg'
    WITH (FORMAT GDAL, DRIVER 'GPKG')
    """
    )

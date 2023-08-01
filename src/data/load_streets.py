"""
ogr2ogr -progress -a_srs EPSG:4326 temp/nodes_4326.gpkg temp/nodes.gpkg
ogr2ogr -progress -spat -12.421470912798974 33.226730183416954 45.535158355759435 71.13547646352613 temp/nodes_eu.gpkg temp/nodes_4326.gpkg

ogr2ogr -progress -a_srs EPSG:4326 temp/edges_4326.gpkg temp/edges.gpkg
ogr2ogr -progress -spat -12.421470912798974 33.226730183416954 45.535158355759435 71.13547646352613 temp/edges_eu.gpkg temp/edges_4326.gpkg
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
    nodes_description = duckdb.sql(
        """
        DESCRIBE
        SELECT *
        FROM read_parquet('s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=transportation/type=connector/*', filename=true, hive_partitioning=1)
        """
    )
    """
    ┌─────────────┬────────────────────────────────────────────────────────────┬
    │ column_name │                        column_type                         │
    │   varchar   │                          varchar                           │
    ├─────────────┼────────────────────────────────────────────────────────────┼
    │ id          │ VARCHAR                                                    │
    │ updatetime  │ TIMESTAMP                                                  │
    │ version     │ INTEGER                                                    │
    │ level       │ INTEGER                                                    │
    │ subtype     │ VARCHAR                                                    │
    │ connectors  │ VARCHAR[]                                                  │
    │ road        │ VARCHAR                                                    │
    │ sources     │ MAP(VARCHAR, VARCHAR)[]                                    │
    │ bbox        │ STRUCT(minx DOUBLE, maxx DOUBLE, miny DOUBLE, maxy DOUBLE) │
    │ geometry    │ BLOB                                                       │
    │ filename    │ VARCHAR                                                    │
    │ theme       │ VARCHAR                                                    │
    │ type        │ VARCHAR                                                    │
    ├─────────────┴────────────────────────────────────────────────────────────┴
    │ 13 rows                                                                   
    └───────────────────────────────────────────────────────────────────────────
    """
    print(nodes_description)
    edges_description = duckdb.sql(
        """
        DESCRIBE
        SELECT *
        FROM read_parquet('s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=transportation/type=segment/*', filename=true, hive_partitioning=1)
        """
    )
    """
    ┌─────────────┬────────────────────────────────────────────────────────────┬
    │ column_name │                        column_type                         │
    │   varchar   │                          varchar                           │
    ├─────────────┼────────────────────────────────────────────────────────────┼
    │ id          │ VARCHAR                                                    │
    │ updatetime  │ TIMESTAMP                                                  │
    │ version     │ INTEGER                                                    │
    │ level       │ INTEGER                                                    │
    │ subtype     │ VARCHAR                                                    │
    │ connectors  │ VARCHAR[]                                                  │
    │ road        │ VARCHAR                                                    │
    │ sources     │ MAP(VARCHAR, VARCHAR)[]                                    │
    │ bbox        │ STRUCT(minx DOUBLE, maxx DOUBLE, miny DOUBLE, maxy DOUBLE) │
    │ geometry    │ BLOB                                                       │
    │ filename    │ VARCHAR                                                    │
    │ theme       │ VARCHAR                                                    │
    │ type        │ VARCHAR                                                    │
    ├─────────────┴────────────────────────────────────────────────────────────┴
    │ 13 rows                                                                   
    └───────────────────────────────────────────────────────────────────────────
    """
    print(edges_description)

    # fetch street nodes
    duckdb.sql(
        """
            COPY (
        SELECT
            id,
            updatetime::varchar as updatetime, -- doesn't seem to handle timestamps
            version,
            level,
            JSON(connectors) as connectors,
            road,
            -- JSON(sources) as sources,
            -- JSON(bbox) as bbox,
            ST_GeomFromWkb(geometry) AS geometry
        FROM read_parquet('s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=transportation/type=connector/*', filename=true, hive_partitioning=1)
    ) TO './temp/nodes.gpkg'
    WITH (FORMAT GDAL, DRIVER 'GPKG')
    """
    )

    # fetch street edges
    duckdb.sql(
        """
            COPY (
        SELECT
            id,
            updatetime::varchar as updatetime, -- doesn't seem to handle timestamps
            version,
            level,
            JSON(connectors) as connectors,
            road,
            -- JSON(sources) as sources,
            -- JSON(bbox) as bbox,
            ST_GeomFromWkb(geometry) AS geometry
        FROM read_parquet('s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=transportation/type=segment/*', filename=true, hive_partitioning=1)
    ) TO './temp/edges.gpkg'
    WITH (FORMAT GDAL, DRIVER 'GPKG')
    """
    )

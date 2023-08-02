"""
ogr2ogr -progress -a_srs EPSG:4326 temp/buildings_4326.gpkg temp/buildings.gpkg
ogr2ogr -progress -spat -12.421470912798974 33.226730183416954 45.535158355759435 71.13547646352613 temp/buildings_eu.gpkg temp/buildings_4326.gpkg
"""
from __future__ import annotations

import duckdb

from src import tools

logger = tools.get_logger(__name__)


def load_buildings(min_x: int, min_y: int, max_x: int, max_y: int):
    """ """
    logger.info("Loading Overture buildings")
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
    logger.info(description)
    # fetch POI
    duckdb.sql(
        f"""
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
        WHERE bbox.minX > {min_x}
            AND bbox.minY > {min_y}
            AND bbox.maxX < {max_x}
            AND bbox.maxY < {max_y}
    ) TO './temp/buildings.gpkg'
    WITH (FORMAT GDAL, DRIVER 'GPKG')
    """
    )


def load_places(min_x: int, min_y: int, max_x: int, max_y: int):
    """ """
    logger.info("Loading Overture places.")
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
        FROM read_parquet('s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=places/type=place/*', filename=true, hive_partitioning=1)
        """
    )
    """
    ┌─────────────┬─────────────────────────────────────┬
    │ column_name │             column_type             │
    │   varchar   │               varchar               │
    ├─────────────┼─────────────────────────────────────┼
    │ id          │ VARCHAR                             │
    │ updatetime  │ VARCHAR                             │
    │ version     │ INTEGER                             │
    │ names       │ MAP(VARCHAR, MAP(VARCHAR, VARCHAR…  │
    │ categories  │ STRUCT(main VARCHAR, alternate VA…  │
    │ confidence  │ DOUBLE                              │
    │ websites    │ VARCHAR[]                           │
    │ socials     │ VARCHAR[]                           │
    │ emails      │ VARCHAR[]                           │
    │ phones      │ VARCHAR[]                           │
    │ brand       │ STRUCT("names" MAP(VARCHAR, MAP(V…  │
    │ addresses   │ MAP(VARCHAR, VARCHAR)[]             │
    │ sources     │ MAP(VARCHAR, VARCHAR)[]             │
    │ bbox        │ STRUCT(minx DOUBLE, maxx DOUBLE, …  │
    │ geometry    │ BLOB                                │
    │ filename    │ VARCHAR                             │
    │ theme       │ VARCHAR                             │
    │ type        │ VARCHAR                             │
    ├─────────────┴─────────────────────────────────────┴
    │ 18 rows                                            
    └────────────────────────────────────────────────────
    """
    logger.info(description)
    # fetch POI
    duckdb.sql(
        f"""
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
            -- JSON(addresses) as addresses,
            -- JSON(sources) as sources,
            -- JSON(bbox) as bbox,
            ST_GeomFromWkb(geometry) AS geometry
        FROM read_parquet('s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=places/type=place/*', filename=true, hive_partitioning=1)
        WHERE bbox.minX > {min_x}
            AND bbox.minY > {min_y}
            AND bbox.maxX < {max_x}
            AND bbox.maxY < {max_y}
    ) TO './temp/places.gpkg'
    WITH (FORMAT GDAL, DRIVER 'GPKG')
    """
    )


def load_nodes(min_x: int, min_y: int, max_x: int, max_y: int):
    """ """
    logger.info("Loading Overture connectors (nodes)")
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
    logger.info(nodes_description)
    # fetch street nodes
    duckdb.sql(
        f"""
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
        WHERE bbox.minX > {min_x}
            AND bbox.minY > {min_y}
            AND bbox.maxX < {max_x}
            AND bbox.maxY < {max_y}
    ) TO './temp/nodes.gpkg'
    WITH (FORMAT GDAL, DRIVER 'GPKG')
    """
    )


def load_edges(min_x: int, min_y: int, max_x: int, max_y: int):
    """ """
    logger.info("Loading Overture segments (edges)")
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
    logger.info(edges_description)
    # fetch street edges
    duckdb.sql(
        f"""
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
        WHERE bbox.minX > {min_x}
            AND bbox.minY > {min_y}
            AND bbox.maxX < {max_x}
            AND bbox.maxY < {max_y}
    ) TO './temp/edges.gpkg'
    WITH (FORMAT GDAL, DRIVER 'GPKG')
    """
    )

"""
ogr2ogr -progress -a_srs EPSG:4326 -t_srs EPSG:3035 -spat 23.5564 37.7753 24.0717 38.1758 temp/eu_nodes_3035.gpkg temp/eu_nodes.gpkg
ogr2ogr -progress -a_srs EPSG:4326 -t_srs EPSG:3035 -spat 23.5564 37.7753 24.0717 38.1758 temp/eu_edges_3035.gpkg temp/eu_edges.gpkg
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from src import tools

logger = tools.get_logger(__name__)


def prepare_db() -> duckdb.DuckDBPyConnection:
    """ """
    con = duckdb.connect()
    con.execute(
        """
        INSTALL spatial;
        INSTALL httpfs;
            """
    )
    # configure
    con.execute(
        """
        LOAD spatial;
        LOAD httpfs;
        SET s3_region='us-west-2';
            """
    )
    return con


def load_buildings(out_path: Path, file_prefix: str, min_x: int, min_y: int, max_x: int, max_y: int):
    """ """
    logger.info("Loading Overture buildings")
    con = prepare_db()
    con.sql(
        """
        DESCRIBE
        SELECT *
        FROM read_parquet('s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=buildings/type=building/*', filename=true, hive_partitioning=1)
        """
    ).show()
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
    # fetch POI
    con.execute(
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
    ) TO '{out_path.resolve()}/{file_prefix}_buildings.gpkg'
    WITH (FORMAT GDAL, DRIVER 'GPKG')
    """
    )
    logger.info("Overture buildings load completed")


def load_places(out_path: Path, file_prefix: str, min_x: int, min_y: int, max_x: int, max_y: int):
    """ """
    logger.info("Loading Overture places.")
    con = prepare_db()
    con.sql(
        """
        DESCRIBE
        SELECT *
        FROM read_parquet('s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=places/type=place/*', filename=true, hive_partitioning=1)
        """
    ).show()
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
    # fetch POI
    con.execute(
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
    ) TO '{out_path.resolve()}/{file_prefix}_places.gpkg'
    WITH (FORMAT GDAL, DRIVER 'GPKG')
    """
    )
    logger.info("Overture places load completed")


def load_nodes(out_path: Path, file_prefix: str, min_x: int, min_y: int, max_x: int, max_y: int):
    """ """
    logger.info("Loading Overture connectors (nodes)")
    con = prepare_db()
    con.sql(
        """
        DESCRIBE
        SELECT *
        FROM read_parquet('s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=transportation/type=connector/*', filename=true, hive_partitioning=1)
        """
    ).show()
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
    # fetch street nodes
    con.execute(
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
    ) TO '{out_path.resolve()}/{file_prefix}_nodes.gpkg'
    WITH (FORMAT GDAL, DRIVER 'GPKG')
    """
    )
    logger.info("Overture street nodes load completed")


def load_edges(out_path: Path, file_prefix: str, min_x: int, min_y: int, max_x: int, max_y: int):
    """ """
    con = prepare_db()
    con.sql(
        """
        DESCRIBE
        SELECT *
        FROM read_parquet('s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=transportation/type=segment/*', filename=true, hive_partitioning=1)
        """
    ).show()
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
    # fetch street edges
    con.execute(
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
    ) TO '{out_path.resolve()}/{file_prefix}_edges.gpkg'
    WITH (FORMAT GDAL, DRIVER 'GPKG')
    """
    )
    logger.info("Overture street edges load completed")

""" """

import geopandas as gpd

from src import tools

engine = tools.get_sqlalchemy_engine()
logger = tools.get_logger(__name__)


def prepare_metrics_takeoffs(city_key: str, bounds_10km: int):
    """ """
    logger.info(f"Processing {city_key}")
    tools.db_execute("CREATE SCHEMA IF NOT EXISTS takeoffs")
    for metrics_table in ["centrality", "green", "population", "landuses"]:
        logger.info("Checking for index")
        tools.db_execute(
            f"CREATE INDEX IF NOT EXISTS idx_{metrics_table}_bounds_fid ON metrics.{metrics_table} (bounds_fid);"
        )
        logger.info("Reading")
        bounds_data = gpd.read_postgis(
            f"""
            SELECT
                mt.*,
                nnc.edge_geom
            FROM
                metrics.{metrics_table} mt
            JOIN
                overture.dual_nodes nnc
            ON
                mt.fid = nnc.fid
            WHERE mt.bounds_fid = {bounds_10km}
                AND mt.live is true;
            """,
            engine,
            index_col="fid",
            geom_col="edge_geom",
        )
        logger.info("Writing")
        bounds_data.to_postgis(  # type: ignore
            f"b_{city_key}_{metrics_table}",
            engine,
            if_exists="replace",
            schema="takeoffs",
            index=True,
            index_label="fid",
        )


def prepare_data_takeoffs(city_key: str, bounds_fid_2km: int, bounds_fid_10km: int):
    """ """
    logger.info(f"Processing {city_key}")
    tools.db_execute("CREATE SCHEMA IF NOT EXISTS takeoffs")
    for schema, table, bounds_table, bounds_fid in [
        ("overture", "dual_edges", "unioned_bounds_10000", bounds_fid_10km),  # uses 10km
        ("overture", "overture_buildings", "unioned_bounds_2000", bounds_fid_2km),  # uses 2km
        ("overture", "overture_infrast", "unioned_bounds_2000", bounds_fid_2km),
        ("overture", "overture_place", "unioned_bounds_2000", bounds_fid_2km),
        ("eu", "blocks", "unioned_bounds_2000", bounds_fid_2km),  # uses 2km
        ("eu", "stats", "unioned_bounds_2000", bounds_fid_2km),  # uses 2km
        ("eu", "trees", "unioned_bounds_2000", bounds_fid_2km),  # uses 2km
        ("eu", "bounds", "unioned_bounds_2000", bounds_fid_2km),  # uses 2km
    ]:
        logger.info(f"Processing {table}")
        logger.info("Reading")
        data = gpd.read_postgis(
            f"""
            WITH bounds AS (
                SELECT geom
                FROM eu.{bounds_table}
                WHERE fid = {bounds_fid}
            )
            SELECT ot.*
            FROM {schema}.{table} ot
            JOIN bounds b ON ST_Intersects(b.geom, ot.geom);
            """,
            engine,
            index_col="fid",
            geom_col="geom",
        )
        logger.info("Writing")
        data.to_postgis(  # type:ignore
            f"{city_key}_{table}",
            engine,
            if_exists="replace",
            schema="takeoffs",
            index=True,
            index_label="fid",
        )
        # GPKG expects int index
        data.reset_index(drop=True, inplace=True)  # type:ignore
        data.to_file(f"temp/{city_key}_{table}.gpkg")  # type:ignore


if __name__ == "__main__":
    """ """
    prepare_data_takeoffs("nicosia", 105, 119)
    # prepare_data_takeoffs("madrid", 6, 7)
    # prepare_metrics_takeoffs(673)

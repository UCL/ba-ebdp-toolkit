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
                overture.network_nodes_clean nnc
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


def prepare_overture_data_takeoffs(city_key: str, bounds_2km: int, bounds_10km: int):
    """ """
    logger.info(f"Processing {city_key}")
    tools.db_execute("CREATE SCHEMA IF NOT EXISTS takeoffs")
    for overture_table, bounds_fid in [
        ("overture_edge", bounds_10km),  # uses 10km
        ("overture_node", bounds_10km),
        ("overture_buildings", bounds_2km),  # uses 2km
        ("overture_infrast", bounds_2km),
        ("overture_place", bounds_2km),
    ]:
        logger.info(f"Processing {overture_table}")
        logger.info("Checking for index")
        tools.db_execute(
            f"CREATE INDEX IF NOT EXISTS idx_{overture_table}_bounds_fid ON overture.{overture_table} (bounds_fid);"
        )
        logger.info("Reading")
        overture_data = gpd.read_postgis(
            f"""
            SELECT
                ot.*
            FROM
                overture.{overture_table} ot
            WHERE ot.bounds_fid = {bounds_fid};
            """,
            engine,
            index_col="fid",
            geom_col="geom",
        )
        logger.info("Writing")
        overture_data.to_postgis(  # type:ignore
            f"b_{city_key}_{overture_table}",
            engine,
            if_exists="replace",
            schema="takeoffs",
            index=True,
            index_label="fid",
        )


if __name__ == "__main__":
    """ """
    prepare_overture_data_takeoffs("nicosia", 105, 119)
    # prepare_metrics_takeoffs(673)

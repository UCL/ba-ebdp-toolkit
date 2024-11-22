""" """

import geopandas as gpd

from src import tools

engine = tools.get_sqlalchemy_engine()
logger = tools.get_logger(__name__)


def prepare_takeoff_tables(bounds_fid: int):
    """ """
    logger.info(f"Processing bounds {bounds_fid}")
    tools.db_execute("CREATE SCHEMA IF NOT EXISTS takeoffs")
    for metrics_table in ["centrality", "green", "population", "landuses"]:
        logger.info(f"Preparing {metrics_table}")
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
            WHERE mt.bounds_fid = {bounds_fid}
                AND mt.live is true;
            """,
            engine,
            index_col="fid",
            geom_col="edge_geom",
        )
        bounds_data.to_postgis(
            f"b_{bounds_fid}_{metrics_table}",
            engine,
            if_exists="replace",
            schema="takeoffs",
            index=True,
            index_label="fid",
        )


if __name__ == "__main__":
    """ """
    # prepare_takeoff_tables(168)
    prepare_takeoff_tables(27)

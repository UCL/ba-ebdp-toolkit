""" """

import argparse
import concurrent.futures
import os
import traceback

import geopandas as gpd
from cityseer.tools import io
from shapely import geometry

from src import tools

logger = tools.get_logger(__name__)


def generate_clean_network(
    bounds_fid: int | str,
    bound_geom: geometry.Polygon,
    bounds_schema: str,
    bounds_table: str,
    target_schema: str,
    target_table: str,
):
    """ """
    engine = tools.get_sqlalchemy_engine()
    nodes_gdf = gpd.read_postgis(
        f"""
        WITH bounds AS (
            SELECT geom
            FROM {bounds_schema}.{bounds_table}
            WHERE fid = {bounds_fid}
        )
        SELECT DISTINCT n.fid, n.geom
        FROM overture.overture_node n
        JOIN bounds b ON ST_Intersects(b.geom, n.geom);
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    # skip any empty - where overture data doesn't cover extents e.g. peripheral remote towns
    # these are mostly dealt with (e.g. madeira) when running prepare_boundary_polys
    if len(nodes_gdf) == 0:  # type: ignore
        return
    edges_gdf = gpd.read_postgis(
        f"""
        WITH bounds AS (
            SELECT geom
            FROM {bounds_schema}.{bounds_table}
            WHERE fid = {bounds_fid}
        )
        SELECT DISTINCT ON (e.fid)
            e.fid, 
            e.connectors, 
            e.class,
            e.names,
            e.routes,
            e.level_rules,
            e.road_flags,
            e.subtype,
            e.geom
        FROM overture.overture_edge e
        JOIN bounds b ON ST_Intersects(b.geom, e.geom);
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    edges_gdf = edges_gdf[edges_gdf["subtype"] == "road"]  # type: ignore
    multigraph = tools.generate_graph(
        nodes_gdf=nodes_gdf,  # type: ignore
        edges_gdf=edges_gdf,  # type: ignore
        # not dropping "parking_aisle" because this sometimes removes important links
    )
    multigraph = io._auto_clean_network(
        multigraph,
        geom_wgs=bound_geom,
        to_crs_code=3035,
        final_clean_distances=(6, 12),
        remove_disconnected=100,
        green_footways=True,
        green_service_roads=False,
    )
    edges_gdf = io.geopandas_from_nx(multigraph, crs=3035)
    edges_gdf["bounds_key"] = bounds_table
    edges_gdf["bounds_fid"] = bounds_fid
    # write
    edges_gdf.to_postgis(target_table, engine, if_exists="append", schema=target_schema, index=True, index_label="fid")


def process_network(
    target_bounds_fids: list[int] | str,
    drop: bool = False,
    parallel_workers: int = 1,
):
    """ """
    if not (
        tools.check_table_exists("overture", "overture_node") and tools.check_table_exists("overture", "overture_edge")
    ):
        raise OSError("The overture nodes and edges tables need to be created prior to proceeding.")
    logger.info("Preparing cleaned networks")
    load_key = "network_edges_clean"
    bounds_schema = "eu"
    bounds_table = "unioned_bounds_10000"
    bounds_geom_col = "geom"
    bounds_fid_col = "fid"
    target_schema = "overture"
    target_table = "network_edges_clean"
    bounds_fids_geoms = tools.iter_boundaries(bounds_schema, bounds_table, bounds_fid_col, bounds_geom_col, wgs84=True)
    # target fids
    if target_bounds_fids == "all":
        target_fids = [int(big[0]) for big in bounds_fids_geoms]
    else:
        target_fids = [int(fid) for fid in target_bounds_fids]

    # set to quiet mode
    os.environ["CITYSEER_QUIET_MODE"] = "true"

    futures = {}
    with concurrent.futures.ProcessPoolExecutor(max_workers=parallel_workers) as executor:
        try:
            for bound_fid, bound_geom in bounds_fids_geoms:
                if bound_fid not in target_fids:
                    continue
                args = (
                    bound_fid,
                    load_key,
                    generate_clean_network,
                    [
                        bound_fid,
                        bound_geom,
                        bounds_schema,
                        bounds_table,
                        target_schema,
                        target_table,
                    ],
                    target_schema,
                    [target_table],
                    bounds_schema,
                    bounds_table,
                    bounds_geom_col,
                    bounds_fid_col,
                    drop,
                )
                futures[executor.submit(tools.process_func_with_bound_tracking, *args)] = args
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    logger.error(traceback.format_exc())
                    raise RuntimeError("An error occurred in the background task") from exc
        except KeyboardInterrupt:
            executor.shutdown(wait=True, cancel_futures=True)
            raise


if __name__ == "__main__":
    """
    Examples are run from the project folder (the folder containing src)
    python -m src.processing.generate_networks all --parallel_workers 6
    """

    if True:
        parser = argparse.ArgumentParser(description="Convert raw Overture nodes and edges to network.")
        parser.add_argument(
            "bounds_fid",
            type=tools.bounds_fid_type,
            help=("A bounds fid as int to load a specific bounds. Use 'all' to load all bounds."),
        )
        parser.add_argument(
            "--parallel_workers",
            type=int,
            default=2,  # Set your desired default value here
            help="The number of CPU cores to use for processing bounds in parallel. Defaults to 2.",
        )
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        process_network(
            args.bounds_fid,
            args.drop,
            args.parallel_workers,
        )
    else:
        bounds_fids = [269]
        process_network(
            bounds_fids,
            drop=True,
            parallel_workers=1,
        )

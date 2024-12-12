""" """

import argparse
import concurrent.futures
import os
import traceback

from cityseer.tools import graphs, io
from shapely import geometry
from sqlalchemy.dialects.postgresql import JSON

from src import tools
from src.data import loaders

logger = tools.get_logger(__name__)


def process_extent_network(
    bounds_fid: int | str,
    bounds_geom: geometry.Polygon,
    bounds_table: str,
    target_schema: str,
    target_nodes_table: str,
    target_edges_table: str,
    target_clean_nodes_table: str,
    target_clean_edges_table: str,
):
    """ """
    engine = tools.get_sqlalchemy_engine()
    nodes_gdf, edges_gdf, clean_edges_gdf = loaders.load_network(bounds_geom, 3035)
    # NODES
    nodes_gdf["bounds_key"] = bounds_table
    nodes_gdf["bounds_fid"] = bounds_fid
    nodes_gdf.to_postgis(  # type: ignore
        target_nodes_table,
        engine,
        if_exists="append",
        schema=target_schema,
        index=True,
        index_label="fid",
        dtype={
            "sources": JSON,
        },
    )
    # EDGES
    edges_gdf["bounds_key"] = bounds_table
    edges_gdf["bounds_fid"] = bounds_fid
    edges_gdf.to_postgis(  # type: ignore
        target_edges_table,
        engine,
        if_exists="append",
        schema=target_schema,
        index=True,
        index_label="fid",
        dtype={
            "sources": JSON,
            "names": JSON,
            "connectors": JSON,
            "routes": JSON,
            "subclass_rules": JSON,
            "access_restrictions": JSON,
            "level_rules": JSON,
            "destinations": JSON,
            "prohibited_transitions": JSON,
            "road_surface": JSON,
            "road_flags": JSON,
            "speed_limits": JSON,
            "width_rules": JSON,
        },
    )
    # DUAL CLEAN NETWORK
    nx_clean = io.nx_from_generic_geopandas(clean_edges_gdf)
    # cast to dual
    nx_dual = graphs.nx_to_dual(nx_clean)
    # back to GDF
    nodes_dual_gdf, edges_dual_gdf, _network_structure = io.network_structure_from_nx(nx_dual, crs=3035)
    # write
    nodes_dual_gdf["bounds_key"] = bounds_table
    nodes_dual_gdf["bounds_fid"] = bounds_fid
    nodes_dual_gdf.to_postgis(
        target_clean_nodes_table,
        engine,
        if_exists="append",
        schema=target_schema,
        index=True,
        index_label="fid",
    )
    edges_dual_gdf["bounds_key"] = bounds_table
    edges_dual_gdf["bounds_fid"] = bounds_fid
    edges_dual_gdf.to_postgis(
        target_clean_edges_table,
        engine,
        if_exists="append",
        schema=target_schema,
        index=True,
        index_label="fid",
    )


def process_network(
    target_bounds_fids: list[int] | str,
    drop: bool = False,
    parallel_workers: int = 1,
):
    """ """
    logger.info("Preparing cleaned networks")
    load_key = "network_edges_clean"
    bounds_schema = "eu"
    bounds_table = "unioned_bounds_10000"
    bounds_geom_col = "geom"
    bounds_fid_col = "fid"
    target_schema = "overture"
    target_nodes_table = "overture_node"
    target_edges_table = "overture_edge"
    target_clean_nodes_table = "dual_nodes"
    target_clean_edges_table = "dual_edges"

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
                    process_extent_network,
                    [
                        bound_fid,
                        bound_geom,
                        bounds_table,
                        target_schema,
                        target_nodes_table,
                        target_edges_table,
                        target_clean_nodes_table,
                        target_clean_edges_table,
                    ],
                    target_schema,
                    [target_nodes_table, target_edges_table, target_clean_nodes_table, target_clean_edges_table],
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
    python -m src.data.ingest_overture_networks all --parallel_workers 4
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

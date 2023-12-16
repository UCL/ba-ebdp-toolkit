""" """
from __future__ import annotations

import argparse
import concurrent.futures
import os
import traceback

import geopandas as gpd
from cityseer.tools import graphs, io
from geoalchemy2 import Geometry

from src import tools

logger = tools.get_logger(__name__)


def generate_clean_network(
    bounds_fid: int | str,
    bounds_schema: str,
    bounds_table: str,
    target_schema: str,
    target_nodes_table: str,
    target_edges_table: str,
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
        FROM overture.overture_nodes n, bounds b
        WHERE ST_Intersects(b.geom, n.geom)
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    # skip any empty - where overture data doesn't cover extents e.g. peripheral remote towns
    # these are mostly dealt with (e.g. madeira) when running prepare_boundary_polys
    if len(nodes_gdf) == 0:
        return
    edges_gdf = gpd.read_postgis(
        f"""
        WITH bounds AS (
            SELECT geom
            FROM {bounds_schema}.{bounds_table}
            WHERE fid = {bounds_fid}
        )
        SELECT DISTINCT e.fid, connectors, road_class, surface, level, e.geom
        FROM overture.overture_edges e, bounds b
        WHERE ST_Intersects(b.geom, e.geom)
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    multigraph = tools.generate_graph(
        nodes_gdf=nodes_gdf,  # type: ignore
        edges_gdf=edges_gdf,  # type: ignore
        road_class_col="road_class",
        drop_road_classes=["motorway", "parkingAisle"],
    )
    # clean
    crawl_dist = 12
    contains_buffer_dist = 50
    parallel_dist = 15
    G = graphs.nx_remove_filler_nodes(multigraph)
    G = graphs.nx_remove_dangling_nodes(G)
    G = graphs.nx_consolidate_nodes(G, buffer_dist=crawl_dist, crawl=True, contains_buffer_dist=contains_buffer_dist)
    G = graphs.nx_split_opposing_geoms(G, buffer_dist=parallel_dist, contains_buffer_dist=contains_buffer_dist)
    G = graphs.nx_consolidate_nodes(G, buffer_dist=parallel_dist, contains_buffer_dist=contains_buffer_dist)
    G = graphs.nx_remove_filler_nodes(G)
    G = graphs.nx_iron_edges(G)
    G = graphs.nx_split_opposing_geoms(G, buffer_dist=parallel_dist, contains_buffer_dist=contains_buffer_dist)
    G = graphs.nx_consolidate_nodes(G, buffer_dist=parallel_dist, contains_buffer_dist=contains_buffer_dist)
    G = graphs.nx_remove_filler_nodes(G)
    G = graphs.nx_iron_edges(G)
    nodes_gdf, edges_gdf, _network_structure = io.network_structure_from_nx(G, crs=3035)

    G = graphs.nx_generate_vis_lines(G)

    def generate_vis_lines(node_row):
        return G.nodes[node_row.name]["line_geom"]

    nodes_gdf["edge_geom"] = nodes_gdf.apply(generate_vis_lines, axis=1)
    nodes_gdf.to_postgis(
        target_nodes_table,
        engine,
        if_exists="append",
        schema=target_schema,
        index=True,
        index_label="fid",
        dtype={
            "geom": Geometry(geometry_type="POINT", srid=3035),
            "edge_geom": Geometry(geometry_type="MULTILINESTRING", srid=3035),
        },
    )
    edges_gdf.to_postgis(
        target_edges_table, engine, if_exists="append", schema=target_schema, index=True, index_label="fid"
    )


def process_network(
    target_bounds_fids: list[int] | str,
    drop: bool = False,
    parallel_workers: int = 2,
):
    """ """
    if not (
        tools.check_table_exists("overture", "overture_nodes")
        and tools.check_table_exists("overture", "overture_edges")
    ):
        raise IOError("The overture nodes and edges tables need to be created prior to proceeding.")
    logger.info("Preparing cleaned networks")
    load_key = "clean_network"
    bounds_schema = "eu"
    bounds_table = "unioned_bounds_10000"
    bounds_geom_col = "geom"
    bounds_fid_col = "fid"
    target_schema = "overture"
    target_nodes_table = "network_nodes_clean"
    target_edges_table = "network_edges_clean"
    bounds_fids_geoms = tools.iter_boundaries(bounds_schema, bounds_table, bounds_fid_col, bounds_geom_col, wgs84=True)
    # check fids
    bounds_fids = [big[0] for big in bounds_fids_geoms]
    if isinstance(target_bounds_fids, str) and target_bounds_fids == "all":
        process_fids = bounds_fids
    elif set(target_bounds_fids).issubset(set(bounds_fids)):
        process_fids = target_bounds_fids
    else:
        raise ValueError(
            'target_bounds_fids must either be "all" to load all boundaries '
            f"or should otherwise be a list with a subset of ids found in {bounds_schema}.{bounds_table} table."
        )
    # set to quiet mode
    os.environ["CITYSEER_QUIET_MODE"] = "true"

    with concurrent.futures.ProcessPoolExecutor(max_workers=parallel_workers) as executor:
        futures = {}
        for bound_fid in process_fids:
            args = (
                bound_fid,
                load_key,
                generate_clean_network,
                [
                    bound_fid,
                    bounds_schema,
                    bounds_table,
                    target_schema,
                    target_nodes_table,
                    target_edges_table,
                ],
                target_schema,
                [target_nodes_table, target_edges_table],
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


if __name__ == "__main__":
    """
    Examples are run from the project folder (the folder containing src)
    python -m src.processing.generate_networks all --parallel_workers 2
    """

    def bounds_fid_type(value):
        if value == "all":
            return value
        try:
            return int(value)
        except ValueError:
            raise argparse.ArgumentTypeError(f"{value} is not a valid bounds_fid. It must be an integer or 'all'.")

    if True:
        parser = argparse.ArgumentParser(description="Convert raw Overture nodes and edges to network.")
        parser.add_argument(
            "bounds_fid",
            type=bounds_fid_type,
            help=(
                "A bounds fid to load corresponding to input bounds table. "
                "Use 'all' to load all bounds or provide an integer."
            ),
        )
        parser.add_argument(
            "--parallel_workers",
            type=int,
            default=2,  # Set your desired default value here
            help="The number of CPU cores to use for processing bounds in parallel. Defaults to 5.",
        )
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        process_network(
            args.bounds_fid,
            args.overwrite,
            args.parallel_workers,
        )
    else:
        bounds_fids = [436, 439]
        process_network(
            bounds_fids,
            drop=False,
            parallel_workers=2,
        )

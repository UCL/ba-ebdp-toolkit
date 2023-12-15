""" """
from __future__ import annotations

import argparse

import geopandas as gpd
from cityseer.tools import graphs, io
from geoalchemy2 import Geometry
from shapely import geometry
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)

engine = tools.get_sqlalchemy_engine()


def generate_clean_network(
    bounds_fid: int | str,
):
    """ """
    nodes_gdf = gpd.read_postgis(
        f"""
        WITH bounds AS (
            SELECT geom
            FROM eu.unioned_bounds_10000
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
            FROM eu.unioned_bounds_10000
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
    # cast to 3035
    G = io.nx_epsg_conversion(multigraph, 4326, 3035)
    G = graphs.nx_remove_filler_nodes(G)
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
    nodes_gdf, edges_gdf, network_structure = io.network_structure_from_nx(G, crs=3035)

    def generate_vis_lines(node_row):
        return G.nodes[node_row.name]["line_geom"]

    nodes_gdf["edge_geom"] = nodes_gdf.apply(generate_vis_lines, axis=1)
    nodes_gdf.rename(columns={"geom": "node_geom"}, inplace=True)
    nodes_gdf.set_geometry("node_geom", inplace=True)
    nodes_gdf.to_postgis(
        "nodes_network",
        engine,
        if_exists="append",
        schema="overture",
        index=True,
        index_label="fid",
        dtype={
            "point_geom": Geometry(geometry_type="POINT", srid=3035),
            "line_geom": Geometry(geometry_type="LINESTRING", srid=3035),
        },
    )
    edges_gdf.to_postgis("clean_network", engine, if_exists="append", schema="overture", index=True, index_label="fid")


def process_network(
    target_bounds_fids: list[int] | str,
    drop: bool = False,
):
    """ """
    if not (
        tools.check_table_exists("overture", "overture_nodes")
        and tools.check_table_exists("overture", "overture_edges")
    ):
        raise IOError("The overture nodes and edges tables need to be created prior to proceeding.")
    logger.info("Preparing cleaned networks")
    # setup load tracking
    load_key = "clean_network"
    tools.init_tracking_table(load_key, "eu", "unioned_bounds_10000", "fid", "geom")
    # process by boundary clusters to avoid duplication of elements
    bounds_fids_geoms = tools.iter_boundaries("eu", "unioned_bounds_10000", "fid", "geom", wgs84=True)
    bounds_fids = [big[0] for big in bounds_fids_geoms]
    if isinstance(target_bounds_fids, str):
        if target_bounds_fids == "all":
            process_fids = bounds_fids
        else:
            raise ValueError('target_bounds_fids parameter must be a list of int else pass "all" to process all.')
    elif isinstance(target_bounds_fids, list):
        if set(target_bounds_fids).issubset(set(bounds_fids)):
            process_fids = target_bounds_fids
        else:
            raise ValueError("target_bounds_fids must be a subset of ids found in eu.unioned_bounds_10000 table.")
    else:
        raise ValueError('target_bounds_fids parameter must be a list of int else pass "all" to process all.')
    for bound_fid in tqdm(process_fids):
        if drop is True:
            tools.drop_content(
                "overture",
                "clean_network",
                "eu",
                "unioned_bounds_10000",
                "geom",
                "fid",
                bound_fid,
            )
            tools.tracking_state_reset_loaded(load_key, bound_fid)
        loaded = tools.tracking_state_check_loaded(load_key, bound_fid)
        if loaded is True:
            continue
        logger.info(f"Processing eu.unioned_bounds_10000 bounds fid {bound_fid}")
        generate_clean_network(bound_fid)


if __name__ == "__main__":
    """
    Examples are run from the project folder (the folder containing src)
    python -m src.processing.generate_networks all
    """
    if False:
        parser = argparse.ArgumentParser(description="Convert raw Overture nodes and edges to network.")
        parser.add_argument(
            "bounds_fid",
            type=str,
            help="A bounds fid to load corresponding to input bounds table. Use 'all' to load all bounds.",
        )
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        bounds_fid = args.bounds_fid
        if bounds_fid == "all":
            bounds_fids = tools.db_fetch(
                """
                WITH ids AS (SELECT fid FROM eu.unioned_bounds_10000 ORDER BY fid ASC)
                SELECT array_agg(fid) FROM ids
                """
            )[0][0]
        else:
            bounds_fids = [int(bounds_fid)]
        process_network(
            bounds_fids,
            args.overwrite,
        )
    else:
        bounds_fids = [197]  # 197 - Rennes
        # bounds_fids = tools.db_fetch(
        #     f"""
        # WITH ids AS (SELECT fid FROM eu.unioned_bounds_10000 ORDER BY fid ASC)
        # SELECT array_agg(fid) FROM ids
        # """
        # )[0][0]
        process_network(
            bounds_fids,
            drop=False,
        )

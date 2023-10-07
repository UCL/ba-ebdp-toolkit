# pyright: basic
""" """
import argparse

import geopandas as gpd
from cityseer.tools import graphs
from geoalchemy2 import Geometry
from shapely import geometry
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)

engine = tools.get_sqlalchemy_engine()


def fetch_unioned_bound_fids(
    buffer_col: str, bounds_schema: str, bounds_table: str, bounds_fids: list[int]
) -> list[list[int]]:
    """ """
    # use union because of overlaps
    # return clusters of FIDs corresponding to unioned geoms
    bounds_fid_clusters = tools.db_fetch(
        f"""
        WITH union_geoms AS (
            SELECT (ST_Dump(ST_Union({buffer_col}))).geom
            FROM {bounds_schema}.{bounds_table}
        ),
        union_fids AS(
            SELECT array_agg(b.fid) as fids
            FROM {bounds_schema}.{bounds_table} b, union_geoms ug
            WHERE ST_Intersects(b.{buffer_col}, ug.geom)
            GROUP BY ug.geom
        )
        SELECT uf.fids
        FROM union_fids uf
        WHERE uf.fids && %s::bigint[]
        """,
        (bounds_fids,),
    )
    return [b[0] for b in bounds_fid_clusters]


def process_bounds(
    bounds_fids: list[int],
    bounds_schema: str,
    bounds_table: str,
    bounds_buffer_col: str,
    overture_schema: str,
    output_schema: str,
):
    """ """
    logger.info(f"Processing FID cluster: {bounds_fids}")
    # the fids correspond to overlapping clusters per the prior step
    # this avoids duplication
    nodes_gdf = gpd.read_postgis(
        f"""
        WITH bounds AS (
            SELECT ST_Union({bounds_buffer_col}) as geom
            FROM {bounds_schema}.{bounds_table}
            WHERE fid = ANY(%s)
        )
        SELECT DISTINCT n.fid, n.geom
        FROM {overture_schema}.overture_nodes n, bounds b
        WHERE ST_Intersects(b.geom, n.geom)
        """,
        engine,
        index_col="fid",
        geom_col="geom",
        params=(bounds_fids,),
    )
    # skip any empty - where overture data doesn't cover extents e.g. peripheral remote towns
    # these are mostly dealt with (e.g. madeira) when running prepare_boundary_polys
    if len(nodes_gdf) == 0:
        return
    edges_gdf = gpd.read_postgis(
        f"""
        WITH bounds AS (
            SELECT ST_Union({bounds_buffer_col}) as geom
            FROM {bounds_schema}.{bounds_table}
            WHERE fid = ANY(%s)
        )
        SELECT DISTINCT e.fid, connectors, road_class, surface, level, e.geom
        FROM {overture_schema}.overture_edges e, bounds b
        WHERE ST_Intersects(b.geom, e.geom)
        """,
        engine,
        index_col="fid",
        geom_col="geom",
        params=(bounds_fids,),
    )
    multigraph = tools.generate_graph(
        nodes_gdf=nodes_gdf,  # type: ignore
        edges_gdf=edges_gdf,  # type: ignore
        road_class_col="road_class",
        drop_road_classes=["motorway", "parkingAisle", "cycleway"],
    )
    G = graphs.nx_remove_filler_nodes(multigraph)
    G = graphs.nx_remove_dangling_nodes(G, despine=10)
    G = graphs.merge_parallel_edges(G, merge_edges_by_midline=True, contains_buffer_dist=1)
    G_decomp = graphs.nx_decompose(G, 100)
    G_dual = graphs.nx_to_dual(G_decomp)
    nodes_gdf, edges_gdf, network_structure = graphs.network_structure_from_nx(G_dual, crs=3035)

    # extract linestring from primal for visualisation purposes
    def merge_primal_edges(node_row):
        edge = G_decomp[node_row["primal_edge_node_a"]][node_row["primal_edge_node_b"]][node_row["primal_edge_idx"]]
        return edge["geom"]

    nodes_gdf["line_geom"] = nodes_gdf.apply(merge_primal_edges, axis=1)
    nodes_gdf.rename(columns={"geom": "point_geom"}, inplace=True)
    nodes_gdf.set_geometry("point_geom", inplace=True)
    nodes_gdf.to_postgis(
        "nodes_network",
        engine,
        if_exists="append",
        schema=output_schema,
        index=True,
        index_label="fid",
        dtype={
            "point_geom": Geometry(geometry_type="POINT", srid=3035),
            "line_geom": Geometry(geometry_type="LINESTRING", srid=3035),
        },
    )
    edges_gdf.to_postgis(
        "edges_network", engine, if_exists="append", schema=output_schema, index=True, index_label="fid"
    )


def process_network(
    bounds_fids: list[int],
    bounds_schema: str,
    bounds_table: str,
    bounds_buffer_col: str,
    overture_schema: str,
    output_schema: str,
    overwrite: bool = False,
):
    """ """
    logger.info("Preparing networks")
    tools.check_exists(overwrite, output_schema, "nodes_network")
    tools.check_exists(overwrite, output_schema, "edges_network")
    tools.prepare_schema(output_schema)
    # process by boundary clusters to avoid duplication of elements
    bounds_fid_clusters = fetch_unioned_bound_fids(bounds_buffer_col, bounds_schema, bounds_table, bounds_fids)
    for bounds_fid_cluster in tqdm(bounds_fid_clusters):
        process_bounds(
            bounds_fid_cluster,
            bounds_schema,
            bounds_table,
            bounds_buffer_col,
            overture_schema,
            output_schema,
        )


if __name__ == "__main__":
    """
    Examples are run from the project folder (the folder containing src)
    python -m src.processing.generate_networks all eu bounds geom_10000 overture base --overwrite=True
    """
    if True:
        parser = argparse.ArgumentParser(description="Convert raw Overture nodes and edges to network.")
        parser.add_argument(
            "bounds_fid",
            type=str,
            help="A bounds fid to load corresponding to input bounds table. Use 'all' to load all bounds.",
        )
        parser.add_argument("bounds_schema", type=str, help="Schema name for input boundary polygons.")
        parser.add_argument("bounds_table", type=str, help="Table name for input boundary polygons.")
        parser.add_argument("bounds_buffer_column", type=str, help="Column name for buffered boundary.")
        parser.add_argument("overture_schema", type=str, help="Schema name for overture schema.")
        parser.add_argument("output_schema", type=str, help="Schema name for output tables.")
        parser.add_argument("--overwrite", type=bool, default=False, help="Whether to overwrite existing tables.")
        args = parser.parse_args()
        bounds_fid = args.bounds_fid
        if bounds_fid == "all":
            bounds_fids = tools.db_fetch(
                f"""
                WITH fids AS (SELECT fid FROM {args.bounds_schema}.{args.bounds_table} ORDER BY fid ASC)
                SELECT array_agg(fid) FROM fids
                """
            )[0][0]
        else:
            bounds_fids = [int(bounds_fid)]
        process_network(
            bounds_fids,
            args.bounds_schema,
            args.bounds_table,
            args.bounds_buffer_column,
            args.overture_schema,
            args.output_schema,
            args.overwrite,
        )
    else:
        bounds_fids = [811]  # 739
        # bounds_fids = tools.db_fetch(
        #     f"""
        # WITH fids AS (SELECT fid FROM eu.bounds ORDER BY fid ASC)
        # SELECT array_agg(fid) FROM fids
        # """
        # )[0][0]
        process_network(
            bounds_fids,
            "eu",
            "bounds",
            "geom_10000",
            "overture",
            "base",
            overwrite=True,
        )

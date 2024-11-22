"""
This step extracts a basic network - no cleaning at this stage.
"""

from __future__ import annotations

import argparse

import geopandas as gpd
from cityseer.tools import io
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)


def generate_raw_network(
    bounds_fid: int | str,
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
        JOIN bounds b ON ST_Intersects(b.geom, n.geom)

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
            e.connector_ids, 
            e.class,
            e.names,
            e.routes,
            e.level_rules,
            e.geom
        FROM overture.overture_edge e
        JOIN bounds b ON ST_Intersects(b.geom, e.geom)
        -- don't select rail or water
        WHERE subtype = 'road'
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    multigraph = tools.generate_graph(
        nodes_gdf=nodes_gdf,  # type: ignore
        edges_gdf=edges_gdf,  # type: ignore
        # not dropping "parking_aisle" because this sometimes removes important links
    )
    edges_gdf = io.geopandas_from_nx(multigraph, crs=3035)
    edges_gdf["bounds_key"] = bounds_table
    edges_gdf["bounds_fid"] = bounds_fid
    # write
    edges_gdf.to_postgis(target_table, engine, if_exists="append", schema=target_schema, index=True, index_label="fid")


def process_network(
    target_bounds_fids: list[int] | str,
    drop: bool = False,
):
    """ """
    if not (
        tools.check_table_exists("overture", "overture_node") and tools.check_table_exists("overture", "overture_edge")
    ):
        raise OSError("The overture nodes and edges tables need to be created prior to proceeding.")
    logger.info("Preparing raw networks")
    load_key = "raw_network"
    bounds_schema = "eu"
    bounds_table = "unioned_bounds_10000"
    bounds_geom_col = "geom"
    bounds_fid_col = "fid"
    target_schema = "overture"
    target_table = "network_edges_raw"
    bounds_fids_geoms = tools.iter_boundaries(bounds_schema, bounds_table, bounds_fid_col, bounds_geom_col, wgs84=False)
    # check fids
    bounds_fids = reversed([big[0] for big in bounds_fids_geoms])
    if isinstance(target_bounds_fids, str) and target_bounds_fids == "all":
        process_fids = bounds_fids
    elif set(target_bounds_fids).issubset(set(bounds_fids)):
        process_fids = target_bounds_fids
    else:
        raise ValueError(
            'target_bounds_fids must either be "all" to load all boundaries '
            f"or should correspond to an fid found in {bounds_schema}.{bounds_table} table."
        )
    for bound_fid in tqdm(process_fids):
        tools.process_func_with_bound_tracking(
            bound_fid=bound_fid,
            load_key=load_key,
            core_function=generate_raw_network,
            func_args=[
                bound_fid,
                bounds_schema,
                bounds_table,
                target_schema,
                target_table,
            ],
            content_schema=target_schema,
            content_tables=[target_table],
            bounds_schema=bounds_schema,
            bounds_table=bounds_table,
            bounds_geom_col=bounds_geom_col,
            bounds_fid_col=bounds_fid_col,
            drop=drop,
        )


if __name__ == "__main__":
    """
    Examples are run from the project folder (the folder containing src)
    python -m src.processing.generate_networks all
    """

    if True:
        parser = argparse.ArgumentParser(description="Convert raw Overture nodes and edges to network.")
        parser.add_argument(
            "bounds_fid",
            type=tools.bounds_fid_type,
            help=("A bounds fid as int to load a specific bounds. Use 'all' to load all bounds."),
        )
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        process_network(
            args.bounds_fid,
            args.drop,
        )
    else:
        bounds_fids = "all"  # [447]
        process_network(
            bounds_fids,
            drop=False,
        )

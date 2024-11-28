""" """

import argparse

from overturemaps import core
from shapely import geometry
from sqlalchemy.dialects.postgresql import JSON
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)


def process_extent_network(
    bounds_fid: int | str,
    bounds_geom: geometry.Polygon,
    bounds_schema: str,
    bounds_table: str,
    target_schema: str,
    target_nodes_table: str,
    target_edges_table: str,
):
    """ """
    engine = tools.get_sqlalchemy_engine()
    # NODES
    nodes_gdf = core.geodataframe("connector", bounds_geom.bounds)  # type:ignore
    nodes_gdf.set_crs(4326, inplace=True)
    nodes_gdf.to_crs(3035, inplace=True)
    nodes_gdf.set_index("id", inplace=True)
    nodes_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    nodes_gdf.set_geometry("geom", inplace=True)
    nodes_gdf.drop(columns=["bbox"], inplace=True)
    nodes_gdf["bounds_key"] = bounds_table
    nodes_gdf["bounds_fid"] = bounds_fid
    nodes_gdf["sources"] = nodes_gdf["sources"].apply(tools.col_to_json)  # type: ignore
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
    # cleanup nodes
    tools.db_execute(
        f"""
        DELETE FROM {target_schema}.{target_nodes_table} n
        WHERE bounds_fid = {bounds_fid} AND NOT EXISTS (
            SELECT 1 
            FROM {bounds_schema}.{bounds_table} b
            WHERE ST_Contains(b.geom, n.geom)
        );
                """
    )
    # EDGES
    edges_gdf = core.geodataframe("segment", bounds_geom.bounds)  # type:ignore
    edges_gdf.set_crs(4326, inplace=True)
    edges_gdf.to_crs(3035, inplace=True)
    edges_gdf.set_index("id", inplace=True)
    edges_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    edges_gdf.set_geometry("geom", inplace=True)
    edges_gdf.drop(columns=["bbox"], inplace=True)
    edges_gdf["bounds_key"] = bounds_table
    edges_gdf["bounds_fid"] = bounds_fid
    # convert other data columns to JSON
    for col in [
        "sources",
        "names",
        "connectors",
        "routes",
        "subclass_rules",
        "access_restrictions",
        "level_rules",
        "destinations",
        "prohibited_transitions",
        "road_surface",
        "road_flags",
        "speed_limits",
        "width_rules",
    ]:
        edges_gdf[col] = edges_gdf[col].apply(tools.col_to_json).astype("str")  # type: ignore
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
    # cleanup edges
    tools.db_execute(
        f"""
        DELETE FROM {target_schema}.{target_edges_table} e
        WHERE bounds_fid = {bounds_fid} AND NOT EXISTS (
            SELECT 1 
            FROM {bounds_schema}.{bounds_table} b
            WHERE ST_Contains(b.geom, e.geom)
        );
                """
    )


def load_overture_networks(drop: bool = False) -> None:
    """ """
    logger.info("Loading overture networks")
    tools.prepare_schema("overture")
    load_key = "overture_network"
    bounds_schema = "eu"
    bounds_table = "unioned_bounds_10000"
    bounds_geom_col = "geom"
    bounds_fid_col = "fid"
    target_schema = "overture"
    target_nodes_table = "overture_node"
    target_edges_table = "overture_edge"
    bounds_fids_geoms = tools.iter_boundaries(bounds_schema, bounds_table, bounds_fid_col, bounds_geom_col, wgs84=True)
    # iter
    for bound_fid, bound_geom in tqdm(bounds_fids_geoms):
        tools.process_func_with_bound_tracking(
            bound_fid=bound_fid,
            load_key=load_key,
            core_function=process_extent_network,
            func_args=[
                bound_fid,
                bound_geom,
                bounds_schema,
                bounds_table,
                target_schema,
                target_nodes_table,
                target_edges_table,
            ],
            content_schema=target_schema,
            content_tables=[target_nodes_table, target_edges_table],
            bounds_schema=bounds_schema,
            bounds_table=bounds_table,
            bounds_geom_col=bounds_geom_col,
            bounds_fid_col=bounds_fid_col,
            drop=drop,
        )


if __name__ == "__main__":
    """
    Examples are run from the project folder (the folder containing src)
    python -m src.data.ingest_overture_networks
    """
    if True:
        parser = argparse.ArgumentParser(description="Load overture nodes and edges to DB.")
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        load_overture_networks(
            drop=args.drop,
        )
    else:
        load_overture_networks(
            drop=False,
        )

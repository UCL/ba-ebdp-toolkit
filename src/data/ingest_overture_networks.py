""" """
from __future__ import annotations

import argparse
import json
from pathlib import Path

from shapely import geometry
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)


def process_extent_network(
    bounds_fid: int | str,
    bounds_geom: geometry.Polygon,
    bounds_schema: str,
    bounds_table: str,
    overture_nodes_path: str | Path,
    overture_edges_path: str | Path,
    target_schema: str,
    target_nodes_table: str,
    target_edges_table: str,
    bin_path: str | None = None,
):
    """ """
    engine = tools.get_sqlalchemy_engine()
    # NODES
    nodes_gdf = tools.snip_overture_by_extents(overture_nodes_path, bounds_geom, "nodes", bin_path)
    nodes_gdf.set_index("id", inplace=True)
    nodes_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    nodes_gdf.set_geometry("geom", inplace=True)
    nodes_gdf.drop(columns=["connectors", "road", "version", "level"], inplace=True)
    nodes_gdf["bounds_key"] = bounds_table
    nodes_gdf["bounds_fid"] = bounds_fid
    nodes_gdf.to_crs(3035).to_postgis(
        target_nodes_table, engine, if_exists="append", schema=target_schema, index=True, index_label="fid"
    )
    # cleanup edges
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
    edges_gdf = tools.snip_overture_by_extents(overture_edges_path, bounds_geom, "edges", bin_path)
    edges_gdf.set_index("id", inplace=True)
    edges_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    edges_gdf.set_geometry("geom", inplace=True)

    # extract connectors as list of str
    def extract_connectors(json_string):
        connectors = json.loads(json_string)
        connectors = [c for c in connectors]
        return json.dumps(connectors)

    edges_gdf = edges_gdf.rename(columns={"connectors": "_connectors"})
    edges_gdf["connectors"] = edges_gdf["_connectors"].apply(extract_connectors)
    edges_gdf = edges_gdf.drop(columns=["_connectors"])

    # extract class
    def extract_class(json_string):
        road_info = json.loads(json_string)
        if "class" in road_info:
            return road_info["class"]
        return None

    edges_gdf["road_class"] = edges_gdf["road"].apply(extract_class)

    # extract surface
    def extract_surface(json_string):
        road_info = json.loads(json_string)
        if "surface" in road_info:
            return road_info["surface"]
        return None

    edges_gdf["surface"] = edges_gdf["road"].apply(extract_surface)
    edges_gdf["bounds_key"] = bounds_table
    edges_gdf["bounds_fid"] = bounds_fid
    edges_gdf.to_crs(3035).to_postgis(
        target_edges_table, engine, if_exists="append", schema=target_schema, index=True, index_label="fid"
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


def load_overture_networks(
    overture_nodes_path: str | Path,
    overture_edges_path: str | Path,
    bin_path: str | None = None,
    drop: bool = False,
) -> None:
    """ """
    logger.info("Loading overture networks")
    tools.prepare_schema("overture")
    load_key = "overture_networks"
    bounds_schema = "eu"
    bounds_table = "unioned_bounds_10000"
    bounds_geom_col = "geom"
    bounds_fid_col = "fid"
    target_schema = "overture"
    target_nodes_table = "overture_nodes"
    target_edges_table = "overture_edges"
    bounds_fids_geoms = tools.iter_boundaries(bounds_schema, bounds_table, bounds_fid_col, bounds_geom_col, wgs84=True)
    # generate indices on input GPKG
    tools.create_gpkg_spatial_index(overture_nodes_path)
    tools.create_gpkg_spatial_index(overture_edges_path)
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
                overture_nodes_path,
                overture_edges_path,
                target_schema,
                target_nodes_table,
                target_edges_table,
                bin_path,
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
    python -m src.data.ingest_overture_networks 'temp/eu_nodes.gpkg' 'temp/eu_edges.gpkg'
    """
    if True:
        parser = argparse.ArgumentParser(description="Load overture nodes and edges GPKG to DB.")
        parser.add_argument(
            "overture_nodes_path",
            type=str,
            help="Path to overture nodes dataset.",
        )
        parser.add_argument(
            "overture_edges_path",
            type=str,
            help="Path to overture edges dataset.",
        )
        parser.add_argument("--bin_path", type=str, default=None, help="Optional bin path for ogr2ogr.")
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        load_overture_networks(
            args.overture_nodes_path,
            args.overture_edges_path,
            bin_path=args.bin_path,
            drop=args.drop,
        )
    else:
        load_overture_networks(
            "temp/eu_nodes.gpkg",
            "temp/eu_edges.gpkg",
            drop=False,
        )

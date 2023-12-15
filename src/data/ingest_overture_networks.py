""" """
from __future__ import annotations

import argparse
import json
from pathlib import Path

from shapely import geometry
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)
engine = tools.get_sqlalchemy_engine()


def process_extent_network(
    bounds_id: str,
    bounds_geom: geometry.Polygon,
    overture_nodes_path: str | Path,
    overture_edges_path: str | Path,
    bin_path: str | None = None,
):
    """ """
    # NODES
    nodes_gdf = tools.snip_overture_by_extents(overture_nodes_path, bounds_geom, "nodes", bin_path)
    nodes_gdf.set_index("id", inplace=True)
    nodes_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    nodes_gdf.set_geometry("geom", inplace=True)
    nodes_gdf.drop(columns=["connectors", "road", "version", "level"], inplace=True)
    nodes_gdf["bounds_key"] = "unioned_bounds_10000"
    nodes_gdf["bounds_fid"] = bounds_id
    nodes_gdf.to_crs(3035).to_postgis(
        "overture_nodes", engine, if_exists="append", schema="overture", index=True, index_label="fid"
    )
    # cleanup edges
    tools.db_execute(
        f"""
        DELETE FROM overture.overture_nodes n
        WHERE bounds_fid = {bounds_id} AND NOT EXISTS (
            SELECT 1 
            FROM eu.unioned_bounds_10000 b
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
    edges_gdf["bounds_key"] = "unioned_bounds_10000"
    edges_gdf["bounds_fid"] = bounds_id
    edges_gdf.to_crs(3035).to_postgis(
        "overture_edges", engine, if_exists="append", schema="eu", index=True, index_label="fid"
    )
    # cleanup edges
    tools.db_execute(
        f"""
        DELETE FROM overture.overture_edges e
        WHERE bounds_fid = {bounds_id} AND NOT EXISTS (
            SELECT 1 
            FROM eu.unioned_bounds_10000 b
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
    # check that the bounds table exists
    if not tools.check_table_exists("eu", "bounds"):
        raise IOError("The eu.bounds table does not exist; this needs to be created prior to proceeding.")
    logger.info("Loading overture networks")
    # create schema if necessary
    tools.prepare_schema("overture")
    # setup load tracking
    load_key = "overture_networks"
    tools.init_tracking_table(load_key, "eu", "unioned_bounds_10000", "id", "geom")
    # get bounds
    bounds_ids_geoms = tools.iter_boundaries("eu", "unioned_bounds_10000", "id", "geom", wgs84=True)
    # generate indices on input GPKG
    tools.create_gpkg_spatial_index(overture_nodes_path)
    tools.create_gpkg_spatial_index(overture_edges_path)
    # iter
    for bound_id, bound_geom in tqdm(bounds_ids_geoms):
        if drop is True:
            tools.drop_content(
                "overture",
                "overture_nodes",
                "eu",
                "unioned_bounds_10000",
                "geom",
                "id",
                bound_id,
            )
            tools.drop_content(
                "overture",
                "overture_edges",
                "eu",
                "unioned_bounds_10000",
                "geom",
                "id",
                bound_id,
            )
            tools.tracking_state_reset_loaded(load_key, bound_id)
        loaded = tools.tracking_state_check_loaded(load_key, bound_id)
        if loaded is True:
            continue
        logger.info(f"Processing eu.unioned_bounds_10000 bounds id {bound_id}")
        process_extent_network(
            bound_id,
            bound_geom,
            overture_nodes_path,
            overture_edges_path,
            bin_path,
        )
        tools.tracking_state_set_loaded(load_key, bound_id)


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

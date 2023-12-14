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
    bounds_schema: str,
    bounds_table: str,
    bounds_geom_col: str,
    overture_schema_name: str,
    bin_path: str | None = None,
):
    """ """
    # NODES
    nodes_gdf = tools.snip_overture_by_extents(overture_nodes_path, bounds_geom, "nodes", bin_path)
    nodes_gdf.set_index("id", inplace=True)
    nodes_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    nodes_gdf.set_geometry("geom", inplace=True)
    nodes_gdf.drop(columns=["connectors", "road", "version", "level"], inplace=True)
    nodes_gdf["bounds_key"] = bounds_table
    nodes_gdf["bounds_fid"] = bounds_id
    nodes_gdf.to_crs(3035).to_postgis(
        "overture_nodes", engine, if_exists="append", schema=overture_schema_name, index=True, index_label="fid"
    )
    # cleanup edges
    tools.db_execute(
        f"""
        DELETE FROM {overture_schema_name}.overture_nodes n
        WHERE bounds_fid = {bounds_id} AND NOT EXISTS (
            SELECT 1 
            FROM {bounds_schema}.{bounds_table} b
            WHERE ST_Contains(b.{bounds_geom_col}, n.geom)
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
    edges_gdf["bounds_fid"] = bounds_id
    edges_gdf.to_crs(3035).to_postgis(
        "overture_edges", engine, if_exists="append", schema=overture_schema_name, index=True, index_label="fid"
    )
    # cleanup edges
    tools.db_execute(
        f"""
        DELETE FROM {overture_schema_name}.overture_edges e
        WHERE bounds_fid = {bounds_id} AND NOT EXISTS (
            SELECT 1 
            FROM {bounds_schema}.{bounds_table} b
            WHERE ST_Contains(b.{bounds_geom_col}, e.geom)
        );
                """
    )


def load_overture_networks(
    overture_nodes_path: str | Path,
    overture_edges_path: str | Path,
    bounds_schema: str,
    bounds_table: str,
    bounds_id_col: str,
    bounds_geom_col: str,
    overture_schema_name: str,
    bin_path: str | None = None,
    drop: bool = False,
) -> None:
    """ """
    logger.info("Loading overture networks")
    # create schema if necessary
    tools.prepare_schema(overture_schema_name)
    # setup load tracking
    load_key = "overture_networks"
    tools.init_tracking_table(load_key, bounds_schema, bounds_table, bounds_id_col, bounds_geom_col)
    # get bounds
    bounds_ids_geoms = tools.iter_boundaries(bounds_schema, bounds_table, bounds_id_col, bounds_geom_col, wgs84=True)
    # generate indices on input GPKG
    tools.create_gpkg_spatial_index(overture_nodes_path)
    tools.create_gpkg_spatial_index(overture_edges_path)
    # iter
    for bound_id, bound_geom in tqdm(bounds_ids_geoms):
        if drop is True:
            tools.drop_content(
                overture_schema_name,
                "overture_nodes",
                bounds_schema,
                bounds_table,
                bounds_geom_col,
                bounds_id_col,
                bound_id,
            )
            tools.drop_content(
                overture_schema_name,
                "overture_edges",
                bounds_schema,
                bounds_table,
                bounds_geom_col,
                bounds_id_col,
                bound_id,
            )
            tools.tracking_state_reset_loaded(load_key, bound_id)
        loaded = tools.tracking_state_check_loaded(load_key, bound_id)
        if loaded is True:
            continue
        logger.info(f"Processing {bounds_schema}.{bounds_table} bounds id {bound_id}")
        process_extent_network(
            bound_id,
            bound_geom,
            overture_nodes_path,
            overture_edges_path,
            bounds_schema,
            bounds_table,
            bounds_geom_col,
            overture_schema_name,
            bin_path,
        )
        tools.tracking_state_set_loaded(load_key, bound_id)


if __name__ == "__main__":
    """
    Examples are run from the project folder (the folder containing src)
    python -m src.data.ingest_overture_networks 'temp/eu_nodes.gpkg' 'temp/eu_edges.gpkg' eu unioned_bounds_10000 id geom overture
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
        parser.add_argument("bounds_schema", type=str, help="Schema name for boundary polygons.")
        parser.add_argument("bounds_table", type=str, help="Table name for boundary polygons.")
        parser.add_argument("bounds_id_col", type=str, help="id column name for boundary polygons.")
        parser.add_argument("bounds_geom_col", type=str, help="geometry column name for boundary polygons.")
        parser.add_argument("overture_schema", type=str, help="Schema name for overture schema.")
        parser.add_argument("--bin_path", type=str, default=None, help="Optional bin path for ogr2ogr.")
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        load_overture_networks(
            args.overture_nodes_path,
            args.overture_edges_path,
            args.bounds_schema,
            args.bounds_table,
            args.bounds_id_col,
            args.bounds_geom_col,
            args.overture_schema,
            bin_path=args.bin_path,
            drop=args.drop,
        )
    else:
        load_overture_networks(
            "temp/eu_nodes.gpkg",
            "temp/eu_edges.gpkg",
            "eu",
            "unioned_bounds_10000",
            "id",
            "geom",
            "overture",
            drop=False,
        )

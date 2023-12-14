from __future__ import annotations

import argparse
import json
from pathlib import Path

from shapely import geometry
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)
engine = tools.get_sqlalchemy_engine()


def process_extent_buildings(
    bounds_id: str,
    bounds_geom: geometry.Polygon,
    overture_buildings_path: str | Path,
    bounds_schema: str,
    bounds_table: str,
    bounds_geom_col: str,
    overture_schema_name: str,
    bin_path: str | None = None,
):
    """ """
    buildings_gdf = tools.snip_overture_by_extents(overture_buildings_path, bounds_geom, "buildings", bin_path)
    buildings_gdf.set_index("id", inplace=True)
    buildings_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    buildings_gdf.set_geometry("geom", inplace=True)
    buildings_gdf["bounds_key"] = bounds_table
    buildings_gdf["bounds_fid"] = bounds_id
    buildings_gdf.to_crs(3035).to_postgis(
        "overture_buildings", engine, if_exists="append", schema=overture_schema_name, index=True, index_label="fid"
    )
    tools.db_execute(
        f"""
        DELETE FROM {overture_schema_name}.overture_buildings bldgs
        WHERE bounds_fid = {bounds_id} AND NOT EXISTS (
            SELECT 1 
            FROM {bounds_schema}.{bounds_table} b
            WHERE ST_Contains(b.{bounds_geom_col}, bldgs.geom)
        );
        """
    )


def load_overture_buildings(
    overture_buildings_path: str | Path,
    bounds_schema: str,
    bounds_table: str,
    bounds_id_col: str,
    bounds_geom_col: str,
    overture_schema_name: str,
    bin_path: str | None = None,
    drop: bool = False,
) -> None:
    """ """
    logger.info("Loading overture buildings")
    # create schema if necessary
    tools.prepare_schema(overture_schema_name)
    # setup load tracking
    load_key = "overture_buildings"
    tools.init_tracking_table(load_key, bounds_schema, bounds_table, bounds_id_col, bounds_geom_col)
    # get bounds
    bounds_ids_geoms = tools.iter_boundaries(bounds_schema, bounds_table, bounds_id_col, bounds_geom_col, wgs84=True)
    # generate indices on input GPKG
    tools.create_gpkg_spatial_index(overture_buildings_path)
    # iter
    for bound_id, bound_geom in tqdm(bounds_ids_geoms):
        if drop is True:
            tools.drop_content(
                overture_schema_name,
                "overture_buildings",
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
        process_extent_buildings(
            bound_id,
            bound_geom,
            overture_buildings_path,
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
    python -m src.data.ingest_overture_buildings 'temp/eu_buildings.gpkg' eu unioned_bounds_2000 id geom overture
    """
    if True:
        parser = argparse.ArgumentParser(description="Load overture buildings GPKG to DB.")
        parser.add_argument(
            "overture_buildings_path",
            type=str,
            help="Path to overture buildings dataset.",
        )
        parser.add_argument("bounds_schema", type=str, help="Schema name for boundary polygons.")
        parser.add_argument("bounds_table", type=str, help="Table name for boundary polygons.")
        parser.add_argument("bounds_id_col", type=str, help="id column name for boundary polygons.")
        parser.add_argument("bounds_geom_col", type=str, help="geometry column name for boundary polygons.")
        parser.add_argument("overture_schema", type=str, help="Schema name for overture schema.")
        parser.add_argument("--bin_path", type=str, default=None, help="Optional bin path for ogr2ogr.")
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        load_overture_buildings(
            args.overture_buildings_path,
            args.bounds_schema,
            args.bounds_table,
            args.bounds_id_col,
            args.bounds_geom_col,
            args.overture_schema,
            bin_path=args.bin_path,
            drop=args.drop,
        )
    else:
        load_overture_buildings(
            "temp/eu_buildings.gpkg",
            "eu",
            "unioned_bounds_2000",
            "id",
            "geom",
            "overture",
            drop=False,
        )

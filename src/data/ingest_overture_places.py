""" """
from __future__ import annotations

import argparse
import json
from pathlib import Path

from shapely import geometry
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)

OVERTURE_SCHEMA = tools.generate_overture_schema()


def process_extent_places(
    bounds_fid: int | str,
    bounds_geom: geometry.Polygon,
    bounds_schema: str,
    bounds_table: str,
    overture_places_path: str | Path,
    target_schema: str,
    target_table: str,
    bin_path: str | None = None,
):
    """ """
    engine = tools.get_sqlalchemy_engine()
    places_gdf = tools.snip_overture_by_extents(overture_places_path, bounds_geom, "places", bin_path)
    places_gdf.set_index("id", inplace=True)
    places_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    places_gdf.set_geometry("geom", inplace=True)

    def extract_main_cat(json_string: str):
        categories = json.loads(json_string)
        return categories["main"]

    def extract_alt_cat(json_string: str):
        categories = json.loads(json_string)
        return json.dumps(categories["alternate"])

    def extract_name(json_string: str):
        name_info = json.loads(json_string)
        return name_info["common"][0]["value"]

    def assign_major_cat(desc: str):
        for major_cat, major_cat_vals in OVERTURE_SCHEMA.items():
            # same parent categories have further sub categories, others not
            if isinstance(major_cat_vals, list):
                if desc in major_cat_vals:
                    return major_cat
        return None

    places_gdf["main_cat"] = places_gdf["categories"].apply(extract_main_cat)
    places_gdf["alt_cat"] = places_gdf["categories"].apply(extract_alt_cat)
    places_gdf["common_name"] = places_gdf["names"].apply(extract_name)
    places_gdf["major_cat"] = places_gdf["main_cat"].apply(assign_major_cat)
    places_gdf["bounds_key"] = bounds_table
    places_gdf["bounds_fid"] = bounds_fid
    places_gdf.to_crs(3035).to_postgis(
        target_table, engine, if_exists="append", schema=target_schema, index=True, index_label="fid"
    )
    tools.db_execute(
        f"""
        DELETE FROM {target_schema}.{target_table} p
        WHERE bounds_fid = {bounds_fid} AND NOT EXISTS (
            SELECT 1 
            FROM {bounds_schema}.{bounds_table} b
            WHERE ST_Contains(b.geom, p.geom)
        );
        """
    )


def load_overture_places(
    overture_places_path: str | Path,
    bin_path: str | None = None,
    drop: bool = False,
) -> None:
    """ """
    logger.info("Loading overture places")
    tools.prepare_schema("overture")
    load_key = "overture_places"
    bounds_schema = "eu"
    bounds_table = "unioned_bounds_2000"
    bounds_geom_col = "geom"
    bounds_fid_col = "fid"
    target_schema = "overture"
    target_table = "overture_places"
    bounds_fids_geoms = tools.iter_boundaries(bounds_schema, bounds_table, bounds_fid_col, bounds_geom_col, wgs84=True)
    # generate indices on input GPKG
    tools.create_gpkg_spatial_index(overture_places_path)
    # iter
    for bound_fid, bound_geom in tqdm(bounds_fids_geoms):
        tools.process_func_with_bound_tracking(
            bound_fid=bound_fid,
            load_key=load_key,
            core_function=process_extent_places,
            func_args=[
                bound_fid,
                bound_geom,
                bounds_schema,
                bounds_table,
                overture_places_path,
                target_schema,
                target_table,
                bin_path,
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
    python -m src.data.ingest_overture_places 'temp/eu_places.gpkg'
    """
    if True:
        parser = argparse.ArgumentParser(description="Load overture places GPKG to DB.")
        parser.add_argument(
            "overture_places_path",
            type=str,
            help="Path to overture places dataset.",
        )
        parser.add_argument("--bin_path", type=str, default=None, help="Optional bin path for ogr2ogr.")
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        load_overture_places(
            args.overture_places_path,
            bin_path=args.bin_path,
            drop=args.drop,
        )
    else:
        load_overture_places(
            "temp/eu_places.gpkg",
            drop=False,
        )

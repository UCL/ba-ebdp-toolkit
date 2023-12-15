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

OVERTURE_SCHEMA = tools.generate_overture_schema()


def process_extent_places(
    bounds_fid: str,
    bounds_geom: geometry.Polygon,
    overture_places_path: str | Path,
    bin_path: str | None = None,
):
    """ """
    places_gdf = tools.snip_overture_by_extents(overture_places_path, bounds_geom, "places", bin_path)
    places_gdf.set_index("fid", inplace=True)
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
    places_gdf["bounds_key"] = "unioned_bounds_2000"
    places_gdf["bounds_fid"] = bounds_fid
    places_gdf.to_crs(3035).to_postgis(
        "overture_places", engine, if_exists="append", schema="overture", index=True, index_label="fid"
    )
    tools.db_execute(
        f"""
        DELETE FROM overture.overture_places p
        WHERE bounds_fid = {bounds_fid} AND NOT EXISTS (
            SELECT 1 
            FROM bounds.unioned_bounds_2000 b
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
    # check that the bounds table exists
    if not tools.check_table_exists("eu", "bounds"):
        raise IOError("The eu.bounds table does not exist; this needs to be created prior to proceeding.")
    logger.info("Loading overture places")
    # create schema if necessary
    tools.prepare_schema("overture")
    # setup load tracking
    load_key = "overture_places"
    tools.init_tracking_table(load_key, "eu", "unioned_bounds_2000", "fid", "geom")
    # get bounds
    bounds_fids_geoms = tools.iter_boundaries("eu", "unioned_bounds_2000", "fid", "geom", wgs84=True)
    # generate indices on input GPKG
    tools.create_gpkg_spatial_index(overture_places_path)
    # iter
    for bound_fid, bound_geom in tqdm(bounds_fids_geoms):
        if drop is True:
            tools.drop_content(
                "overture",
                "overture_places",
                "eu",
                "unioned_bounds_2000",
                "geom",
                "fid",
                bound_fid,
            )
            tools.tracking_state_reset_loaded(load_key, bound_fid)
        loaded = tools.tracking_state_check_loaded(load_key, bound_fid)
        if loaded is True:
            continue
        logger.info(f"Processing eu.unioned_bounds_2000 bounds fid {bound_fid}")
        process_extent_places(
            bound_fid,
            bound_geom,
            overture_places_path,
            bin_path,
        )
        tools.tracking_state_set_loaded(load_key, bound_fid)


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

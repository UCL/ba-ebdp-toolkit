""" """
from __future__ import annotations

import argparse
from pathlib import Path

from shapely import geometry
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)
engine = tools.get_sqlalchemy_engine()


def process_extent_buildings(
    bounds_fid: str,
    bounds_geom: geometry.Polygon,
    overture_buildings_path: str | Path,
    bin_path: str | None = None,
):
    """ """
    buildings_gdf = tools.snip_overture_by_extents(overture_buildings_path, bounds_geom, "buildings", bin_path)
    buildings_gdf.set_index("fid", inplace=True)
    buildings_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    buildings_gdf.set_geometry("geom", inplace=True)
    buildings_gdf["bounds_key"] = "unioned_bounds_2000"
    buildings_gdf["bounds_fid"] = bounds_fid
    buildings_gdf.to_crs(3035).to_postgis(
        "overture_buildings", engine, if_exists="append", schema="overture", index=True, index_label="fid"
    )
    tools.db_execute(
        f"""
        DELETE FROM overture.overture_buildings bldgs
        WHERE bounds_fid = {bounds_fid} AND NOT EXISTS (
            SELECT 1 
            FROM bounds.unioned_bounds_2000 b
            WHERE ST_Contains(b.geom, p.geom)
        );
        """
    )


def load_overture_buildings(
    overture_buildings_path: str | Path,
    bin_path: str | None = None,
    drop: bool = False,
) -> None:
    """ """
    # check that the bounds table exists
    if not tools.check_table_exists("eu", "bounds"):
        raise IOError("The eu.bounds table does not exist; this needs to be created prior to proceeding.")
    logger.info("Loading overture buildings")
    # create schema if necessary
    tools.prepare_schema("overture")
    # setup load tracking
    load_key = "overture_buildings"
    tools.init_tracking_table(load_key, "eu", "unioned_bounds_2000", "fid", "geom")
    # get bounds
    bounds_fids_geoms = tools.iter_boundaries("eu", "unioned_bounds_2000", "fid", "geom", wgs84=True)
    # generate indices on input GPKG
    tools.create_gpkg_spatial_index(overture_buildings_path)
    # iter
    for bound_fid, bound_geom in tqdm(bounds_fids_geoms):
        if drop is True:
            tools.drop_content(
                "overture",
                "overture_buildings",
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
        process_extent_buildings(
            bound_fid,
            bound_geom,
            overture_buildings_path,
            bin_path,
        )
        tools.tracking_state_set_loaded(load_key, bound_fid)


if __name__ == "__main__":
    """
    Examples are run from the project folder (the folder containing src)
    python -m src.data.ingest_overture_buildings 'temp/eu_buildings.gpkg'
    """
    if True:
        parser = argparse.ArgumentParser(description="Load overture buildings GPKG to DB.")
        parser.add_argument(
            "overture_buildings_path",
            type=str,
            help="Path to overture buildings dataset.",
        )
        parser.add_argument("--bin_path", type=str, default=None, help="Optional bin path for ogr2ogr.")
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        load_overture_buildings(
            args.overture_buildings_path,
            bin_path=args.bin_path,
            drop=args.drop,
        )
    else:
        load_overture_buildings(
            "temp/eu_buildings.gpkg",
            drop=False,
        )

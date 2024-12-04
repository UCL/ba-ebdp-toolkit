"""
See src/raw_landuse_schema.csv
Taken from https://github.com/OvertureMaps/schema/blob/0f9fdbcd88e7c0fc08e9c8c68d32cb334dd1d450/docs/schema/concepts/by-theme/places/overture_categories.csv#L16
"""

import argparse

from shapely import geometry
from sqlalchemy.dialects.postgresql import JSON
from tqdm import tqdm

from src import tools
from src.data import loaders

logger = tools.get_logger(__name__)

OVERTURE_SCHEMA = tools.generate_overture_schema()


def process_extent_places(
    bounds_fid: int | str,
    bounds_geom: geometry.Polygon,
    bounds_table: str,
    target_schema: str,
    target_table: str,
):
    """ """
    engine = tools.get_sqlalchemy_engine()
    places_gdf = loaders.load_places(bounds_geom, 3035)
    places_gdf["bounds_key"] = bounds_table
    places_gdf["bounds_fid"] = bounds_fid
    places_gdf.to_postgis(  # type: ignore
        target_table,
        engine,
        if_exists="append",
        schema=target_schema,
        index=True,
        index_label="fid",
        dtype={
            "sources": JSON,
            "names": JSON,
            "categories": JSON,
            "brand": JSON,
            "addresses": JSON,
            "websites": JSON,
            "socials": JSON,
            "emails": JSON,
            "phones": JSON,
        },
    )


def load_overture_places(drop: bool = False) -> None:
    """ """
    logger.info("Loading overture places")
    tools.prepare_schema("overture")
    load_key = "overture_place"
    bounds_schema = "eu"
    bounds_table = "unioned_bounds_2000"
    bounds_geom_col = "geom"
    bounds_fid_col = "fid"
    target_schema = "overture"
    target_table = "overture_place"
    bounds_fids_geoms = tools.iter_boundaries(bounds_schema, bounds_table, bounds_fid_col, bounds_geom_col, wgs84=True)
    # iter
    for bound_fid, bound_geom in tqdm(bounds_fids_geoms):
        tools.process_func_with_bound_tracking(
            bound_fid=bound_fid,
            load_key=load_key,
            core_function=process_extent_places,
            func_args=[
                bound_fid,
                bound_geom,
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
    python -m src.data.ingest_overture_places
    """
    if True:
        parser = argparse.ArgumentParser(description="Load overture places to DB.")
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        load_overture_places(drop=args.drop)
    else:
        load_overture_places(drop=False)

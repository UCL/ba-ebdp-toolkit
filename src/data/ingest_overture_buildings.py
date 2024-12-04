""" """

import argparse

from shapely import geometry
from sqlalchemy.dialects.postgresql import JSON
from tqdm import tqdm

from src import tools
from src.data import loaders

logger = tools.get_logger(__name__)


def process_extent_buildings(
    bounds_fid: int | str,
    bounds_geom: geometry.Polygon,
    bounds_table: str,
    target_schema: str,
    target_table: str,
):
    """ """
    engine = tools.get_sqlalchemy_engine()
    buildings_gdf = loaders.load_buildings(bounds_geom, 3035)  # type:ignore
    buildings_gdf["bounds_key"] = bounds_table
    buildings_gdf["bounds_fid"] = bounds_fid
    buildings_gdf.to_postgis(  # type: ignore
        target_table,
        engine,
        if_exists="append",
        schema=target_schema,
        index=True,
        index_label="fid",
        dtype={
            "sources": JSON,
            "names": JSON,
        },
    )


def load_overture_buildings(drop: bool = False) -> None:
    """ """
    logger.info("Loading overture buildings")
    tools.prepare_schema("overture")
    load_key = "overture_buildings"
    bounds_schema = "eu"
    bounds_table = "unioned_bounds_2000"
    bounds_geom_col = "geom"
    bounds_fid_col = "fid"
    target_schema = "overture"
    target_table = "overture_buildings"
    bounds_fids_geoms = tools.iter_boundaries(bounds_schema, bounds_table, bounds_fid_col, bounds_geom_col, wgs84=True)
    # iter
    for bound_fid, bound_geom in tqdm(bounds_fids_geoms):
        tools.process_func_with_bound_tracking(
            bound_fid=bound_fid,
            load_key=load_key,
            core_function=process_extent_buildings,
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
    python -m src.data.ingest_overture_buildings
    """
    if True:
        parser = argparse.ArgumentParser(description="Load overture buildings to DB.")
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        load_overture_buildings(drop=args.drop)
    else:
        load_overture_buildings(drop=False)

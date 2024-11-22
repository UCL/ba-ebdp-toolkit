""" """

import argparse
from pathlib import Path

from shapely import geometry
from sqlalchemy.dialects.postgresql import JSON
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)


def process_extent_buildings(
    bounds_fid: int | str,
    bounds_geom: geometry.Polygon,
    bounds_schema: str,
    bounds_table: str,
    overture_buildings_path: str | Path,
    target_schema: str,
    target_table: str,
):
    """ """
    engine = tools.get_sqlalchemy_engine()
    buildings_gdf = tools.snip_overture_by_extents(overture_buildings_path, bounds_geom)
    buildings_gdf.set_index("id", inplace=True)
    buildings_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    buildings_gdf.set_geometry("geom", inplace=True)
    buildings_gdf["bounds_key"] = bounds_table
    buildings_gdf["bounds_fid"] = bounds_fid
    for col in ["sources", "names"]:
        buildings_gdf[col] = buildings_gdf[col].apply(tools.col_to_json).astype("str")  # type: ignore
    buildings_gdf.to_crs(3035).to_postgis(  # type: ignore
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
    tools.db_execute(
        f"""
        DELETE FROM {target_schema}.{target_table} bldgs
        WHERE bounds_fid = {bounds_fid} AND NOT EXISTS (
            SELECT 1 
            FROM {bounds_schema}.{bounds_table} b
            WHERE ST_Contains(b.geom, bldgs.geom)
        );
        """
    )


def load_overture_buildings(
    overture_buildings_path: str | Path,
    drop: bool = False,
) -> None:
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
                bounds_schema,
                bounds_table,
                overture_buildings_path,
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
    python -m src.data.ingest_overture_buildings 'temp/eu_buildings.geoparquet'
    """
    if True:
        parser = argparse.ArgumentParser(description="Load overture buildings geoparquet file to DB.")
        parser.add_argument(
            "overture_buildings_path",
            type=str,
            help="Path to overture buildings dataset.",
        )
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        load_overture_buildings(
            args.overture_buildings_path,
            drop=args.drop,
        )
    else:
        load_overture_buildings(
            "temp/eu-building.geoparquet",
            drop=False,
        )

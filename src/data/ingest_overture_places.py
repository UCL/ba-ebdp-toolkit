"""
See src/raw_landuse_schema.csv
Taken from https://github.com/OvertureMaps/schema/blob/0f9fdbcd88e7c0fc08e9c8c68d32cb334dd1d450/docs/schema/concepts/by-theme/places/overture_categories.csv#L16
"""

import argparse

from overturemaps import core
from shapely import geometry
from sqlalchemy.dialects.postgresql import JSON
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)

OVERTURE_SCHEMA = tools.generate_overture_schema()


def process_extent_places(
    bounds_fid: int | str,
    bounds_geom: geometry.Polygon,
    bounds_schema: str,
    bounds_table: str,
    target_schema: str,
    target_table: str,
):
    """ """
    engine = tools.get_sqlalchemy_engine()
    places_gdf = core.geodataframe("place", bounds_geom.bounds)  # type:ignore
    places_gdf.set_crs(4326, inplace=True)
    places_gdf.to_crs(3035, inplace=True)
    places_gdf.set_index("id", inplace=True)
    places_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    places_gdf.set_geometry("geom", inplace=True)

    def extract_main_cat(lu_classes: dict | None) -> str | None:
        if lu_classes is None:
            return None
        return lu_classes["primary"]

    def extract_alt_cats(lu_classes: dict | None):
        if lu_classes is None:
            return None
        return tools.col_to_json(lu_classes["alternate"])

    def extract_name(names: dict | None) -> str | None:
        if names is None:
            return None
        if names["common"] is not None:
            return names["common"]
        if names["primary"] is not None:
            return names["primary"]
        return None

    def assign_major_cat(lu_cat_desc: str) -> str | None:
        for major_cat, major_cat_vals in OVERTURE_SCHEMA.items():
            if lu_cat_desc in major_cat_vals:
                return major_cat
        if lu_cat_desc is not None:
            logger.info(f"Category not found in landuse schema: {lu_cat_desc}")
        return None

    places_gdf["main_cat"] = places_gdf["categories"].apply(extract_main_cat)  # type: ignore
    places_gdf["alt_cats"] = places_gdf["categories"].apply(extract_alt_cats)  # type: ignore
    places_gdf["common_name"] = places_gdf["names"].apply(extract_name)  # type: ignore
    places_gdf["major_lu_schema_class"] = places_gdf["main_cat"].apply(assign_major_cat)  # type: ignore
    for col in [
        "sources",
        "names",
        "categories",
        "brand",
        "addresses",
        "websites",
        "socials",
        "emails",
        "phones",
    ]:
        places_gdf[col] = places_gdf[col].apply(tools.col_to_json).astype(str)  # type: ignore
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
                bounds_schema,
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
    if False:
        parser = argparse.ArgumentParser(description="Load overture places geoparquet file to DB.")
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        load_overture_places(drop=args.drop)
    else:
        load_overture_places(drop=False)

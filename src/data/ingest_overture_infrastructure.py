""" """

import argparse

from overturemaps import core
from shapely import geometry
from sqlalchemy.dialects.postgresql import JSON
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)


def process_extent_infrast(
    bounds_fid: int | str,
    bounds_geom: geometry.Polygon,
    bounds_schema: str,
    bounds_table: str,
    target_schema: str,
    target_table: str,
):
    """ """
    engine = tools.get_sqlalchemy_engine()
    infrast_gdf = core.geodataframe("infrastructure", bounds_geom.bounds)  # type:ignore
    infrast_gdf.set_crs(4326, inplace=True)
    infrast_gdf.to_crs(3035, inplace=True)
    infrast_gdf.set_index("id", inplace=True)
    infrast_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    infrast_gdf.set_geometry("geom", inplace=True)
    infrast_gdf = infrast_gdf[infrast_gdf.geom.geom_type == "Point"]  # returns line and polygons as well
    infrast_gdf.drop(columns=["bbox"], inplace=True)

    def extract_name(names: dict | None) -> str | None:
        if names is None:
            return None
        if names["common"] is not None:
            return names["common"]
        if names["primary"] is not None:
            return names["primary"]
        return None

    infrast_gdf["common_name"] = infrast_gdf["names"].apply(extract_name)  # type: ignore
    for col in [
        "sources",
        "names",
        "source_tags",
    ]:
        infrast_gdf[col] = infrast_gdf[col].apply(tools.col_to_json).astype(str)  # type: ignore
    infrast_gdf["bounds_key"] = bounds_table
    infrast_gdf["bounds_fid"] = bounds_fid
    infrast_gdf.to_postgis(  # type: ignore
        target_table,
        engine,
        if_exists="append",
        schema=target_schema,
        index=True,
        index_label="fid",
        dtype={
            "sources": JSON,
            "names": JSON,
            "source_tags": JSON,
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


def load_overture_infrast(drop: bool = False) -> None:
    """ """
    logger.info("Loading overture infrastructure")
    tools.prepare_schema("overture")
    load_key = "overture_infrast"
    bounds_schema = "eu"
    bounds_table = "unioned_bounds_2000"
    bounds_geom_col = "geom"
    bounds_fid_col = "fid"
    target_schema = "overture"
    target_table = "overture_infrast"
    bounds_fids_geoms = tools.iter_boundaries(bounds_schema, bounds_table, bounds_fid_col, bounds_geom_col, wgs84=True)
    # iter
    for bound_fid, bound_geom in tqdm(bounds_fids_geoms):
        tools.process_func_with_bound_tracking(
            bound_fid=bound_fid,
            load_key=load_key,
            core_function=process_extent_infrast,
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
    python -m src.data.ingest_overture_infrast
    """
    if False:
        parser = argparse.ArgumentParser(description="Load overture infrastructure to DB.")
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        load_overture_infrast(drop=args.drop)
    else:
        load_overture_infrast(drop=False)

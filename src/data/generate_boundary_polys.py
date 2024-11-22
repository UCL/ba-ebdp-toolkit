""" """

from __future__ import annotations

import argparse

import geopandas as gpd
import osmnx as ox
from geoalchemy2 import Geometry
from rasterio.features import shapes
from rasterio.io import MemoryFile
from shapely import geometry

from src import tools

logger = tools.get_logger(__name__)


def extract_boundary_polys(src_schema_name: str, bounds_raster_table_name: str) -> None:
    """ """
    # may need raster support to be enabled on DB
    engine = tools.get_sqlalchemy_engine()
    # fetch eu high density clusters
    raster_result = tools.db_fetch(
        f"""
        SELECT ST_AsTiff(ST_Union(r.rast))
            FROM {src_schema_name}.{bounds_raster_table_name} r;
        """
    )[0][0]
    # fetch UK boundary to filter out
    uk_boundary = ox.geocode_to_gdf("United Kingdom").to_crs("3035").iloc[0].geometry
    # and EU boundary to filter out remote islands (including Madeira)
    eu_boundary = geometry.box(1431795, 2500000, 6637004, 4772012)
    # extract polygons from raster
    polys: list[geometry.Polygon] = []
    with MemoryFile(raster_result) as memfile:
        with memfile.open() as dataset:
            rast_array = dataset.read(1)
            for geom, value in shapes(rast_array, transform=dataset.transform):
                # -21474836... represents no value
                if value < 0:
                    continue
                # extract polygon features from raster blobs
                poly = geometry.shape(geom)
                # log if anything problematic found
                if not isinstance(poly, geometry.Polygon):
                    logger.warning(f"Discarding extracted geom of type {poly.type}")
                    continue
                # don't load if intersecting UK
                if uk_boundary.contains(poly):
                    continue
                # don't load if outside EU
                if not eu_boundary.contains(poly):
                    continue
                # buffer and reverse buffer to smooth edges
                poly = poly.buffer(2000).buffer(-1000)
                # agg
                polys.append(poly)

    # generate the gdf
    data = {"geom": polys}
    bounds_gdf = gpd.GeoDataFrame(data, geometry="geom", crs=dataset.crs)
    bounds_gdf["geom_2000"] = bounds_gdf["geom"].buffer(2000)
    bounds_gdf["geom_10000"] = bounds_gdf["geom"].buffer(10000)
    # write to DB
    tools.prepare_schema("eu")
    bounds_gdf.to_postgis(
        "bounds",
        engine,
        if_exists="replace",
        schema="eu",
        index=True,
        index_label="fid",
        dtype={
            "geom_2000": Geometry(geometry_type="POLYGON", srid=3035),
            "geom_10000": Geometry(geometry_type="POLYGON", srid=3035),
        },
    )
    # add indices
    tools.db_execute(
        """
        CREATE INDEX bounds_2000_geom_idx
            ON eu.bounds USING GIST (geom_2000);
                     """
    )
    tools.db_execute(
        """
        CREATE INDEX bounds_10000_geom_idx
            ON eu.bounds USING GIST (geom_10000);
                     """
    )
    # create unioned boundaries
    tools.db_execute(
        """
        DROP TABLE IF EXISTS eu.unioned_bounds_2000;
        CREATE TABLE eu.unioned_bounds_2000 AS
        WITH unioned_geoms AS (
            SELECT 
                ST_MakePolygon(
                    ST_ExteriorRing(
                        (ST_Dump(ST_Union(geom_2000))).geom
                    )
                )::geometry(POLYGON, 3035) AS geom
            FROM 
                eu.bounds
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY ST_Area(geom) DESC) AS fid, 
            geom
        FROM 
            unioned_geoms;
        """
    )
    tools.db_execute(
        """
        CREATE INDEX unioned_bounds_geom_2000_idx
            ON eu.unioned_bounds_2000 USING GIST (geom);
        """
    )
    tools.db_execute(
        """
        DROP TABLE IF EXISTS eu.unioned_bounds_10000;
        CREATE TABLE eu.unioned_bounds_10000 AS
        WITH unioned_geoms AS (
            SELECT 
                ST_MakePolygon(
                    ST_ExteriorRing(
                        (ST_Dump(ST_Union(geom_10000))).geom
                    )
                )::geometry(POLYGON, 3035) AS geom
            FROM 
                eu.bounds
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY ST_Area(geom) DESC) AS fid, 
            geom
        FROM 
            unioned_geoms;
        """
    )
    tools.db_execute(
        """
        CREATE INDEX unioned_bounds_geom_10000_idx
            ON eu.unioned_bounds_10000 USING GIST (geom);
        """
    )


if __name__ == "__main__":
    """
    Examples are run from the project folder (the folder containing src)
    python -m src.data.generate_boundary_polys eu hdens_clusters bounds
    """
    logger.info("Converting raster boundaries to polygons.")
    if True:
        parser = argparse.ArgumentParser(description="Load building heights raster data.")
        parser.add_argument("src_schema_name", type=str, help="Schema name.")
        parser.add_argument("bounds_raster_table_name", type=str, help="Table name for boundaries raster.")
        args = parser.parse_args()
        extract_boundary_polys(args.src_schema_name, args.bounds_raster_table_name)
    else:
        extract_boundary_polys("eu", "hdens_clusters")

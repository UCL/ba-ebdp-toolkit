import logging

import geopandas as gpd
import momepy
import numpy as np
from rasterio.io import MemoryFile
from rasterio.mask import mask
from shapely import geometry
from tqdm import tqdm

from src import tools

engine = tools.get_sqlalchemy_engine()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def buildings(
    bounds_geom_wgs: geometry.Polygon,
    crs: int,
):
    bldgs_gdf = gpd.read_postgis(
        f"""
        WITH bounds AS (
            SELECT ST_Transform(
                    ST_SetSRID(ST_GeomFromText('{bounds_geom_wgs.wkt}'), 4326), 
                    3035
                ) AS geom
        )
        SELECT bldgs.*
        FROM overture.overture_buildings AS bldgs
        JOIN bounds AS b ON ST_Intersects(b.geom, bldgs.geom);
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    # load raster
    raster_bytes = tools.db_fetch(f"""
        WITH bounds AS (
            SELECT ST_Transform(
                    ST_SetSRID(ST_GeomFromText('{bounds_geom_wgs.wkt}'), 4326), 
                    3035
                ) AS geom
        )
        SELECT ST_AsGDALRaster(ST_Union(ST_Clip(rast, b.geom)), 'GTiff') AS rast
        FROM eu.bldg_hts, bounds b
        WHERE ST_Intersects(b.geom, rast);
        """)[0][0]
    # explode
    bldgs_gdf = bldgs_gdf.explode(index_parts=False)  # type: ignore
    bldgs_gdf.reset_index(drop=True, inplace=True)
    bldgs_gdf.index = bldgs_gdf.index.astype(str)
    # sample heights
    logger.info("Sampling building heights")
    heights = []
    with MemoryFile(raster_bytes) as memfile, memfile.open() as rast_data:
        for _idx, bldg_row in tqdm(bldgs_gdf.iterrows(), total=len(bldgs_gdf)):
            try:
                # raster values within building polygon
                out_image, _ = mask(rast_data, [bldg_row.geom.buffer(5)], crop=True)
                # mean height, excluding nodata values
                valid_pixels = out_image[0][out_image[0] != rast_data.nodata]
                mean_height = np.mean(valid_pixels) if len(valid_pixels) > 0 else np.nan
                heights.append(mean_height)
            except ValueError:
                heights.append(np.nan)
    bldgs_gdf["mean_height"] = heights
    # bldg metrics
    area = bldgs_gdf.area
    ht = bldgs_gdf.loc[:, "mean_height"]
    bldgs_gdf["area"] = area
    bldgs_gdf["perimeter"] = bldgs_gdf.length
    bldgs_gdf["compactness"] = momepy.circular_compactness(bldgs_gdf)
    bldgs_gdf["orientation"] = momepy.orientation(bldgs_gdf)
    # height-based metrics
    bldgs_gdf["volume"] = momepy.volume(area, ht)
    bldgs_gdf["floor_area_ratio"] = momepy.floor_area(area, ht, 3)
    bldgs_gdf["form_factor"] = momepy.form_factor(bldgs_gdf, ht)
    # complexity metrics
    bldgs_gdf["corners"] = momepy.corners(bldgs_gdf)
    bldgs_gdf["shape_index"] = momepy.shape_index(bldgs_gdf)
    bldgs_gdf["fractal_dimension"] = momepy.fractal_dimension(bldgs_gdf)
    # save
    bldgs_gdf = bldgs_gdf.to_crs(crs)
    bldgs_gdf.to_file("temp/madrid_bldgs.gpkg")


def blocks(
    bounds_geom_wgs: geometry.Polygon,
    crs: int,
):
    blocks_gdf = gpd.read_postgis(
        f"""
        WITH bounds AS (
            SELECT ST_Transform(
                    ST_SetSRID(ST_GeomFromText('{bounds_geom_wgs.wkt}'), 4326), 
                    3035
                ) AS geom
        )
        SELECT bl.*
        FROM eu.blocks AS bl
        JOIN bounds AS b ON ST_Intersects(b.geom, bl.geom);
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    blocks_gdf["block_area"] = blocks_gdf.area
    blocks_gdf["block_perimeter"] = blocks_gdf.length
    blocks_gdf["block_compactness"] = momepy.circular_compactness(blocks_gdf)
    blocks_gdf["block_orientation"] = momepy.orientation(blocks_gdf)
    # save
    blocks_gdf = blocks_gdf.to_crs(crs)
    blocks_gdf.reset_index(drop=True, inplace=True)
    blocks_gdf.to_file("temp/madrid_blocks.gpkg")


if __name__ == "__main__":
    """ """
    crs = 25830
    location_key = "madrid"
    bounds_path = "temp/madrid_buffered_bounds.gpkg"
    out_path = f"temp/{location_key}"
    bounds = gpd.read_file(bounds_path)
    bounds_geom_wgs = bounds.to_crs(4326).union_all()
    buildings(bounds_geom_wgs, 25830)
    # blocks(bounds_geom_wgs, 25830)

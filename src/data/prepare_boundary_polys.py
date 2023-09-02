from __future__ import annotations

import argparse
import json
import os

import geopandas as gpd
from dotenv import load_dotenv
from rasterio.features import shapes
from rasterio.io import MemoryFile
from shapely import geometry
from sqlalchemy import create_engine, text

from src import tools

load_dotenv()

logger = tools.get_logger(__name__)

db_config_json = os.getenv("DB_CONFIG")
db_config = json.loads(db_config_json)
connection_string = (
    f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
)
engine = create_engine(connection_string)


def bound_polys(schema_name: str, bounds_raster_table_name: str, bounds_table_name: str) -> None:
    # may need raster support to be enabled on DB
    # fetch eu high density clusters
    with engine.connect() as connection:
        raster_result = connection.execute(  # type: ignore
            text(
                f"""
            SELECT ST_AsTiff(ST_Union(r.rast))
                FROM {schema_name}.{bounds_raster_table_name} r;
            """
            )
        ).fetchone()[0]
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
                # agg
                polys.append(poly)

    # generate the gdf
    data = {"geom": polys}
    bounds_gdf = gpd.GeoDataFrame(data, geometry="geom", crs=dataset.crs)
    # write to DB
    bounds_gdf.to_postgis(
        bounds_table_name, engine, if_exists="replace", schema=schema_name, index=True, index_label="fid"
    )


if __name__ == "__main__":
    """
    Examples are run from the project folder (the folder containing src)
    python -m src.data.prepare_boundary_polys eu hdens_clusters bounds
    """
    parser = argparse.ArgumentParser(description="Load building heights raster data.")
    parser.add_argument("schema_name", type=str, help="Schema name.")
    parser.add_argument("bounds_raster_table_name", type=str, help="Table name for boundaries raster.")
    parser.add_argument("bounds_table_name", type=str, help="Table name for output boundary polygons.")
    args = parser.parse_args()
    logger.info(f"Converting raster boundaries to polygons.")
    bound_polys(args.schema_name, args.bounds_raster_table_name, args.bounds_table_name)

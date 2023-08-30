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
# may need raster support to be enabled on DB
# fetch eu high density clusters
with engine.connect() as connection:
    raster_result = connection.execute(
        text(
            f"""
        WITH rasters AS (
            SELECT rast 
            FROM eu.hdens_clusters
        )
        SELECT ST_AsTiff(ST_Union(r.rast))
            FROM rasters r
            LIMIT 1;
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
data = {"geometry": polys}
bounds_gdf = gpd.GeoDataFrame(data, crs=dataset.crs)
# write to DB
bounds_gdf.to_postgis(
    "bounds",
    engine,
    if_exists="replace",
    schema="eu",
    index=True,
    index_label="id",
)

import argparse

import geopandas as gpd
from geoalchemy2 import Geometry
from rasterio.features import shapes
from rasterio.io import MemoryFile
from shapely import geometry

from src import tools

logger = tools.get_logger(__name__)
engine = tools.get_sqlalchemy_engine()


def bound_polys(schema_name: str, bounds_raster_table_name: str, bounds_table_name: str) -> None:
    # may need raster support to be enabled on DB
    # fetch eu high density clusters
    raster_result = tools.db_fetch(
        f"""
        SELECT ST_AsTiff(ST_Union(r.rast))
            FROM {schema_name}.{bounds_raster_table_name} r;
        """
    )[0][0]
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
    bounds_gdf.to_postgis(
        bounds_table_name,
        engine,
        if_exists="replace",
        schema=schema_name,
        index=True,
        index_label="fid",
        dtype={
            "geom_2000": Geometry(geometry_type="POLYGON", srid=3035),
            "geom_10000": Geometry(geometry_type="POLYGON", srid=3035),
        },
    )
    # add indices
    tools.db_execute(
        f"""
        CREATE INDEX {bounds_table_name}_2000_geom_idx
            ON {schema_name}.{bounds_table_name} USING GIST (geom_2000);
                     """
    )
    tools.db_execute(
        f"""
        CREATE INDEX {bounds_table_name}_10000_geom_idx
            ON {schema_name}.{bounds_table_name} USING GIST (geom_10000);
                     """
    )
    # drop remote boundaries, e.g. south west islands such as madeira
    tools.db_execute(
        f"""
        WITH extent AS (
            SELECT ST_MakeEnvelope(2576047, 1389198, 5883853, 4772012, 3035) AS geom
        )
        DELETE FROM {schema_name}.{bounds_table_name} b
        WHERE NOT EXISTS (
            SELECT 1 FROM extent e
            WHERE ST_Intersects(e.geom, b.geom)
        );
        """
    )


if __name__ == "__main__":
    """
    Examples are run from the project folder (the folder containing src)
    python -m src.data.prepare_boundary_polys eu hdens_clusters bounds
    """
    logger.info(f"Converting raster boundaries to polygons.")
    if True:
        parser = argparse.ArgumentParser(description="Load building heights raster data.")
        parser.add_argument("schema_name", type=str, help="Schema name.")
        parser.add_argument("bounds_raster_table_name", type=str, help="Table name for boundaries raster.")
        parser.add_argument("bounds_table_name", type=str, help="Table name for output boundary polygons.")
        args = parser.parse_args()
        bound_polys(args.schema_name, args.bounds_raster_table_name, args.bounds_table_name)
    else:
        bound_polys("eu", "hdens_clusters", "bounds")

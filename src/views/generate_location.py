""" """

import logging
from pathlib import Path

import geopandas as gpd
from shapely import geometry

from src import tools
from src.data import loaders

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OVERTURE_SCHEMA = tools.generate_overture_schema()


def download_location(
    bounds_geom_wgs: geometry.Polygon,
    location_key: str,
    out_path: Path | str,
    crs: int,
):
    """ """
    out_path = Path(out_path)
    logger.info(f"Downloading overture data to {out_path}")
    out_path.mkdir(exist_ok=True, parents=True)

    # NETWORK
    _nodes_gdf, _edges_gdf, clean_edges_gdf = loaders.load_network(bounds_geom_wgs, crs)
    clean_edges_gdf.to_file(out_path / f"{location_key}_network.gpkg")

    # BUILDINGS
    buildings_gdf = loaders.load_buildings(bounds_geom_wgs, crs)
    buildings_gdf.to_file(out_path / f"{location_key}_buildings.gpkg")

    # INFRASTRUCTURE
    infrast_gdf = loaders.load_infrastructure(bounds_geom_wgs, crs)
    infrast_gdf.to_file(out_path / f"{location_key}_infrastructure.gpkg")

    # PLACES
    places_gdf = loaders.load_places(bounds_geom_wgs, crs)
    places_gdf.to_file(out_path / f"{location_key}_places.gpkg")


if __name__ == "__main__":
    """ """
    crs = 3035
    for location_key, bounds_path in [
        ("nicosia", "temp/nicosia_buffered_bounds.gpkg"),
        # ("madrid", "temp/madrid_buffered_bounds.gpkg"),
    ]:
        out_path = f"temp/{location_key}"
        bounds = gpd.read_file(bounds_path)
        bounds_geom_wgs = bounds.to_crs(4326).union_all()

        download_location(
            bounds_geom_wgs=bounds_geom_wgs,
            location_key=location_key,
            out_path=out_path,
            crs=crs,
        )

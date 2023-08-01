""" """
from __future__ import annotations

import geopandas as gpd
from shapely import geometry

from src.tools import get_logger

logger = get_logger(__name__)


def generate_graph(
    boundary: geometry.Polygon, nodes_gpkg_path: str, edges_gpkg_path: str
):
    """ """
    nodes_gdf = gpd.read_file(nodes_gpkg_path)
    edges_gdf = gpd.read_file(edges_gpkg_path)
    # filter by boundary and build nx
    for node in nodes_gdf.iterrows():
        print(node)


if __name__ == "__main__":
    """ """
    # TODO: workflow to process boundaries from DB
    generate_graph(
        boundary="",
        nodes_gpkg_path="temp/nodes_small.gpkg",
        edges_gpkg_path="temp/edges_small.gpkg",
    )

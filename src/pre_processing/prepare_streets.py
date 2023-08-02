""" """
from __future__ import annotations

import json

import geopandas as gpd
import networkx as nx
from shapely import geometry

from src.tools import get_logger

logger = get_logger(__name__)


def generate_graph(boundary: geometry.Polygon, nodes_gpkg_path: str, edges_gpkg_path: str):
    """ """
    nodes_gdf = gpd.read_file(nodes_gpkg_path)
    edges_gdf = gpd.read_file(edges_gpkg_path)
    multigraph = nx.MultiGraph()
    # filter by boundary and build nx
    for node_idx, node_data in nodes_gdf.iterrows():
        print(node_data)
        multigraph.add_node(
            node_data.id,
            x=node_data.geometry.x,
            y=node_data.geometry.y,
            live=True,
            # boundary.contains(
            #     geometry.Point(node_data.geometry.x, node_data.geometry.y)
            # ),
        )
    for edge_idx, edges_data in edges_gdf.iterrows():
        print(edges_data)
        connectors = json.loads(edges_data.connectors)
        # where there are multiple connectors
        if len(connectors) != 2:
            print("here")
        level = edges_data.level
        data = edges_data.road
        geom = edges_data.geometry


if __name__ == "__main__":
    """ """
    # TODO: workflow to process boundaries from DB
    generate_graph(
        boundary="",
        nodes_gpkg_path="temp/nodes_small.gpkg",
        edges_gpkg_path="temp/edges_small.gpkg",
    )

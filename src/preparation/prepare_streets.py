""" """
from __future__ import annotations

import json

import geopandas as gpd
import networkx as nx
from shapely import geometry

from src import tools

logger = tools.get_logger(__name__)


def generate_graph(
    nodes_gpkg_path: str,
    edges_gpkg_path: str,
    target_crs_epsg: int | str | None = None,
    boundary: geometry.Polygon | None = None,
    buffer: int | None = None,
):
    """ """
    # prepare nodes
    nodes_gdf = gpd.read_file(nodes_gpkg_path)
    edges_gdf = gpd.read_file(edges_gpkg_path)
    if target_crs_epsg is not None:
        nodes_gdf = nodes_gdf.to_crs(target_crs_epsg)
        edges_gdf = edges_gdf.to_crs(target_crs_epsg)
    if boundary is not None:
        # if buffer parameter, then buffer first
        buff_boundary = boundary if buffer is None else boundary.buffer(buffer)
        nodes_gdf = nodes_gdf[nodes_gdf.geometry.within(buff_boundary)]
        edges_gdf = edges_gdf[edges_gdf.geometry.within(buff_boundary)]
    # set live nodes
    nodes_gdf["live"] = True
    # if boundary provided, then set False where outside the boundary
    if boundary is not None:
        nodes_gdf[~nodes_gdf.geometry.within(boundary)]["live"] = False
    # create graph
    multigraph = nx.MultiGraph()
    # filter by boundary and build nx
    for node_data in nodes_gdf.itertuples(index=False):
        # print(node_data)
        multigraph.add_node(
            node_data.id.split(".")[-1],
            x=node_data.geometry.x,
            y=node_data.geometry.y,
            live=node_data.live,
            level=node_data.level,
            data=node_data.road,
        )
    for edges_data in edges_gdf.itertuples(index=False):
        # print(edges_data)
        # TODO
        connectors: list[str] = json.loads(edges_data.connectors)
        connectors = [c.split(".")[-1] for c in connectors]
        connector_pairs: list[tuple[str, geometry.Point]] = []
        for connector in connectors:
            if connector in multigraph.nodes():
                print("here")
        seg_pairs = tools.split_street_segment(edges_data.geometry, connector_pairs)
        for seg_pair in seg_pairs:
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

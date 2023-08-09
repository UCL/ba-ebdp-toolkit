""" """
from __future__ import annotations

import json
from functools import partial

import geopandas as gpd
import networkx as nx
from shapely import geometry
from shapely.ops import transform
from pyproj import Transformer
from tqdm import tqdm
import matplotlib.pyplot as plt
from src import tools

logger = tools.get_logger(__name__)


def generate_graph(
    nodes_gdf: gpd.GeoDataFrame,
    edges_gdf: gpd.GeoDataFrame,
    road_class_col: str | None = None,
    drop_road_classes: list[str] = ["motorway", "parkingAisle"],
) -> nx.MultiGraph:
    """ """
    logger.info("Preparing GeoDataFrames")
    # create graph
    multigraph = nx.MultiGraph()
    # filter by boundary and build nx
    logger.info("Adding nodes to graph")
    for node_row in tqdm(nodes_gdf.itertuples()):
        multigraph.add_node(
            node_row.Index,
            x=node_row.geom.x,
            y=node_row.geom.y,
        )
    logger.info("Adding edges to graph")
    kept_road_types: set[str] = set()
    for edge_idx, edges_data in tqdm(edges_gdf.iterrows()):
        if road_class_col is not None:
            if edges_data[road_class_col] in drop_road_classes:
                continue
            kept_road_types.add(edges_data[road_class_col])
        connector_ids: list[str] = json.loads(edges_data.connectors)
        connector_infos: list[tuple[str, geometry.Point]] = []
        missing_connectors = False
        for connector_id in connector_ids:
            # skip malformed edges - this happens at boundary thresholds with missing nodes in relation to edges
            if connector_id not in multigraph:
                missing_connectors = True
                break
            connector_point = geometry.Point(multigraph.nodes[connector_id]["x"], multigraph.nodes[connector_id]["y"])
            connector_infos.append((connector_id, connector_point))
        if missing_connectors is True:
            continue
        if len(connector_infos) < 2:
            # logger.warning("Only one connector pair for edge")
            continue
        street_segs = tools.split_street_segment(edges_data.geom, connector_infos)
        for seg_geom, node_info_a, node_info_b in street_segs:
            if not node_info_a[1].touches(seg_geom) or not node_info_b[1].touches(seg_geom):
                raise ValueError(
                    "Edge and endpoint connector are not touching. "
                    f"See connectors: {node_info_a[0]} and {node_info_b[0]}"
                )
            multigraph.add_edge(
                node_info_a[0],
                node_info_b[0],
                edge_idx=edge_idx,
                level=edges_data.level,
                geom=seg_geom,
            )
    logger.info(f'Dropped road types: {", ".join(drop_road_classes)}')
    logger.info(f'Kept road types: {", ".join(kept_road_types)}')

    return multigraph


if __name__ == "__main__":
    """ """
    pass

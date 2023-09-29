""" """
import json

import geopandas as gpd
import networkx as nx
from shapely import geometry
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)
Connector = tuple[str, geometry.Point]


def split_street_segment(
    line_string: geometry.LineString, connector_infos: list[Connector]
) -> list[tuple[str, str, geometry.LineString]]:
    """ """
    # overture segments can span multiple intersections
    # sort through and split until pairings are ready for insertion to the graph
    node_segment_pairs: list[tuple[geometry.LineString, Connector, Connector]] = []
    node_segment_lots: list[tuple[geometry.LineString, list[Connector]]] = [(line_string, connector_infos)]
    # start iterating
    while node_segment_lots:
        old_line_string, old_connectors = node_segment_lots.pop()
        # filter down connectors
        new_connectors: list[tuple[str, geometry.Point]] = []
        # if the point doesn't touch the line, discard
        for _id, _point in old_connectors:
            if _point.distance(old_line_string) > 0:
                continue
            new_connectors.append((_id, _point))
        # if only two connectors, check that these are endpoints and continue
        if len(new_connectors) == 2:
            node_segment_pairs.append((old_line_string, *new_connectors))
            continue
        # look for splits
        for _id, _point in new_connectors:
            splits = ops.split(old_line_string, _point)
            # continue if an endpoint
            if len(splits.geoms) == 1:
                continue
            # otherwise unpack
            line_string_a, line_string_b = splits.geoms
            # otherwise split into two bundles and reset
            node_segment_lots.append((line_string_a, new_connectors))
            node_segment_lots.append((line_string_b, new_connectors))
            break
    return node_segment_pairs


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

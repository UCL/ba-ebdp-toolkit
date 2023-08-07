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

from src import tools

logger = tools.get_logger(__name__)


def generate_graph(
    nodes_gdf: gpd.GeoDataFrame,
    edges_gdf: gpd.GeoDataFrame,
    target_crs_epsg: int | str | None = None,
    wgs_clip_bbox: geometry.Polygon | None = None,
    neg_buffer_dist: int | None = None,
    road_class_col: str | None = None,
    drop_road_classes: list[str] = ["motorway", "parkingAisle"],
) -> nx.MultiGraph:
    """ """
    logger.info("Preparing GeoDataFrames")
    # convert to projected CRS specified
    if target_crs_epsg is not None:
        nodes_gdf = nodes_gdf.to_crs(target_crs_epsg)
        edges_gdf = edges_gdf.to_crs(target_crs_epsg)
    # clip if bbox
    if wgs_clip_bbox is not None:
        transformer = Transformer.from_crs("EPSG:4326", f"EPSG:{target_crs_epsg}", always_xy=True)
        projector = partial(transformer.transform)
        projected_bbox = transform(projector, wgs_clip_bbox)
        nodes_gdf = nodes_gdf[nodes_gdf.geometry.intersects(projected_bbox)]
        edges_gdf = edges_gdf[edges_gdf.geometry.intersects(projected_bbox)]
    nodes_gdf["live"] = True
    if neg_buffer_dist is not None:
        bound_geom = nodes_gdf.unary_union.convex_hull
        buff_geom = bound_geom.buffer(-abs(neg_buffer_dist))
        nodes_gdf.loc[~nodes_gdf.geometry.within(buff_geom), "live"] = False
    # create graph
    multigraph = nx.MultiGraph()
    # filter by boundary and build nx
    logger.info("Adding nodes to graph")
    for node_row in tqdm(nodes_gdf.itertuples()):
        multigraph.add_node(
            node_row.Index,
            x=node_row.geometry.x,
            y=node_row.geometry.y,
            live=node_row.live,
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
        street_segs = tools.split_street_segment(edges_data.geometry, connector_infos)
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
    multigraph = generate_graph(
        nodes_gpkg_path="temp/_nodes_athens.gpkg",
        edges_gpkg_path="temp/_edges_athens.gpkg",
        target_crs_epsg=3035,
    )

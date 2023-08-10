""" """
from __future__ import annotations

from cityseer.tools import graphs
import geopandas as gpd

from src import tools

logger = tools.get_logger(__name__)


def reassemble_network(engine):
    logger.info("Loading nodes GDF")
    # load nodes
    nodes_gdf = gpd.read_postgis(
        """
        SELECT
            network.id,
            ns_node_idx,
            x,
            y,
            ST_Contains(bounds.geom, network.geom) as live,
            ST_Transform(network.geom, 3035) as geom
        FROM athens.nodes_network as network, athens.boundary as bounds;
        """,
        engine,
        index_col="id",
        geom_col="geom",
    )
    logger.info("Loading edges GDF")
    # load edges
    edges_gdf = gpd.read_postgis(
        """
        SELECT
            id,
            ns_edge_idx,
            start_ns_node_idx,
            end_ns_node_idx,
            edge_idx,
            nx_start_node_key,
            nx_end_node_key,
            length,
            angle_sum,
            imp_factor,
            in_bearing,
            out_bearing,
            total_bearing,
            ST_Transform(geom, 3035) as geom
        FROM athens.edges_network;
        """,
        engine,
        index_col="id",
        geom_col="geom",
    )
    logger.info("Generating networkx graph")
    multigraph = graphs.nx_from_geopandas(nodes_gdf, edges_gdf)

    return multigraph

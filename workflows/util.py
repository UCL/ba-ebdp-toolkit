""" """
import geopandas as gpd
import networkx as nx
from cityseer.tools import graphs
from sqlalchemy import Engine, text

from src import tools

logger = tools.get_logger(__name__)


def reassemble_network(engine: Engine, bounds_table: str, nodes_table: str, edges_table: str) -> nx.MultiGraph:
    """ """
    logger.info("Loading nodes GDF")
    # load nodes
    nodes_gdf: gpd.GeoDataFrame = gpd.read_postgis(  # type: ignore
        f"""
        SELECT
            network.fid,
            ns_node_idx,
            x,
            y,
            ST_Contains(bounds.geom, network.point_geom) as live,
            ST_Transform(network.point_geom, 3035) as geom
        FROM {nodes_table} as network, {bounds_table} as bounds;
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    logger.info("Loading edges GDF")
    # load edges
    edges_gdf: gpd.GeoDataFrame = gpd.read_postgis(  # type: ignore
        f"""
        SELECT
            fid,
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
        FROM {edges_table};
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    logger.info("Generating networkx graph")
    multigraph = graphs.nx_from_geopandas(nodes_gdf, edges_gdf)

    return multigraph

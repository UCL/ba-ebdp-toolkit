""" """
from __future__ import annotations

from cityseer.tools import graphs
import geopandas as gpd
from sqlalchemy import text, Engine
import networkx as nx
from src import tools

logger = tools.get_logger(__name__)


def reassemble_network(engine: Engine, bounds_table: str, nodes_table: str, edges_table: str) -> nx.MultiGraph:
    """ """
    logger.info("Loading nodes GDF")
    # load nodes
    nodes_gdf: gpd.GeoDataFrame = gpd.read_postgis(  # type: ignore
        f"""
        SELECT
            network.id,
            ns_node_idx,
            x,
            y,
            ST_Contains(bounds.geom, network.point_geom) as live,
            ST_Transform(network.point_geom, 3035) as geom
        FROM {nodes_table} as network, {bounds_table} as bounds;
        """,
        engine,
        index_col="id",
        geom_col="geom",
    )
    logger.info("Loading edges GDF")
    # load edges
    edges_gdf: gpd.GeoDataFrame = gpd.read_postgis(  # type: ignore
        f"""
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
        FROM {edges_table};
        """,
        engine,
        index_col="id",
        geom_col="geom",
    )
    logger.info("Generating networkx graph")
    multigraph = graphs.nx_from_geopandas(nodes_gdf, edges_gdf)

    return multigraph


# TODO: untested
def assign_nodes_to_edges(
    columns: list[str], engine: Engine, template_edges_table: str, nodes_table: str, edges_table: str
) -> None:
    """Use with caution as it blurs the results - e.g. steer clear for betweenness metrics."""
    with engine.connect() as connection:
        # create table
        connection.execute(
            text(
                f"""
            CREATE TABLE IF NOT EXISTS {edges_table}
                AS SELECT id, geom
                FROM {template_edges_table};
            CREATE INDEX IF NOT EXISTS idx_{edges_table.split('.')[-1]}_geom
                ON {edges_table} USING gist (geom);
                """
            )
        )
        connection.commit()
        # iterate columns and add
        for column in columns:
            if column.startswith("cc_metric"):
                logger.info(f"Adding column {column}")
                connection.execute(
                    text(
                        f"""
                    -- create column
                    ALTER TABLE {edges_table}
                        ADD COLUMN IF NOT EXISTS {column} float;
                    -- assign based on nearest road nodes
                    WITH line_ends AS (
                        SELECT 
                            id,
                            ST_StartPoint(geom) as start_point,
                            ST_EndPoint(geom) as end_point
                        FROM 
                            {edges_table}
                    )
                    UPDATE {edges_table} as et
                        SET {column} = least(start_val + end_val)
                    FROM 
                        line_ends le
                        -- Get the closest start point
                        LEFT JOIN LATERAL (
                            SELECT nt.{column} as start_val
                            FROM {nodes_table} nt
                            -- degrees not metres
                            WHERE ST_DWithin(le.start_point, nt.geom, 0.001)
                            ORDER BY ST_Distance(le.start_point, nt.geom) LIMIT 1
                        ) sp ON TRUE
                        -- Get the closest end point
                        LEFT JOIN LATERAL (
                            SELECT nt.{column} as end_val
                            FROM {nodes_table} nt
                            -- degrees not metres
                            WHERE ST_DWithin(le.end_point, nt.geom, 0.001)
                            ORDER BY ST_Distance(le.end_point, nt.geom) LIMIT 1
                        ) ep ON true
                    WHERE et.id = le.id;
                    """
                    )
                )
                connection.commit()

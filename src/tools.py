""" """
from __future__ import annotations

import asyncio
import json
import logging
import os
import warnings
from pathlib import Path
from typing import Any, Generator

import asyncpg
import geopandas as gpd
import networkx as nx
import pandas as pd
from dotenv import load_dotenv  # type: ignore
from shapely import geometry, wkb, ops
from sqlalchemy import create_engine  # type: ignore
from tqdm import tqdm

from src import structures

warnings.simplefilter(action="ignore", category=pd.errors.PerformanceWarning)


def get_logger(name: str, log_level: int = logging.INFO) -> logging.Logger:
    logging.basicConfig(level=log_level)
    return logging.getLogger(name)


logger = get_logger(__name__)


def get_db_config() -> structures.DBConfig:
    """ """
    load_dotenv()
    db_config_json = os.getenv("DB_CONFIG")
    if db_config_json is None:
        raise ValueError("Unable to retrieve DB_CONFIG environment variable.")
    db_config: structures.DBConfig = json.loads(db_config_json)
    for key in ["host", "port", "user", "database", "password"]:
        if key not in db_config:
            raise ValueError("Unable to find expected key: {key} in DB_CONFIG")
    return db_config


def get_db_con_str() -> str:
    """ """
    db_config = get_db_config()
    con_str = (
        f"postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}?"
        f"user={db_config['user']}&password={db_config['password']}"
    )
    return con_str


async def get_db_con() -> asyncpg.Connection:
    """ """
    db_config = get_db_config()
    return await asyncpg.connect(**db_config)  # type: ignore


async def postgis_to_nx(
    nodes_db_schema: str,
    nodes_db_table: str,
    edges_db_schema: str,
    edges_db_table: str,
    boundary_id: str,
):
    """ """
    logger = get_logger(__name__)
    logger.info(f"Generating graph from database for city uid: {city_pop_id}")
    multigraph = nx.MultiGraph()
    logger.info("Loading nodes data")
    db_con = await get_db_con()
    node_records: Any = await db_con.fetch(
        f"""
        SELECT uid, within, geom
        FROM {nodes_db_schema}.{nodes_db_table}
        WHERE boundary_id = {boundary_id}"""
    )
    for node_data in tqdm(node_records):
        geom: geometry.Point = wkb.loads(node_data["geom"], hex=True)  # type: ignore
        multigraph.add_node(node_data["uid"], x=geom.x, y=geom.y, live=node_data["within"])  # pylint: disable=no-member
    logger.info("Loading edges data")
    edge_records: Any = await db_con.fetch(
        f"""
        SELECT uid, node_a, node_b, geom
        FROM {edges_db_schema}.{edges_db_table}
        WHERE boundary_id = {boundary_id}"""
    )
    await db_con.close()
    # handle (literal) edge cases
    for edge_data in tqdm(edge_records):
        if edge_data["node_a"] in multigraph and edge_data["node_b"] in multigraph:
            multigraph.add_edge(
                edge_data["node_a"],
                edge_data["node_b"],
                geom=wkb.loads(edge_data["geom"], hex=True),
            )

    return multigraph


async def _iter_boundaries(
    boundaries_schema: str, boundaries_table: str, start: int, end: int, buffer: int
) -> list[tuple[int, geometry.Polygon, geometry.Polygon]]:
    """ """
    db_con = await get_db_con()
    response: list[Any] = await db_con.fetch(
        f"""
    SELECT pop_id, geom as bound_geom, ST_Simplify(ST_ConvexHull(ST_Buffer(geom, $1)), 20) as convex_buff_geom
    FROM {boundaries_schema}.{boundaries_table}
    WHERE pop_id >= {start} AND pop_id <= {end}
    ORDER BY pop_id;
    """,
        buffer,
    )
    rows = [
        (
            row["pop_id"],
            wkb.loads(row["bound_geom"], hex=True),
            wkb.loads(row["convex_buff_geom"], hex=True),
        )
        for row in response
    ]
    return rows  # type: ignore


def iter_boundaries(
    boundaries_schema: str,
    boundaries_table: str,
    start: int = 1,
    end: int = 2,
    buffer: int = 2000,
) -> list[tuple[int, geometry.Polygon, geometry.Polygon]]:
    """ """
    rows: list[tuple[int, geometry.Polygon]] = asyncio.run(
        _iter_boundaries(boundaries_schema, boundaries_table, start, end, buffer)  # type: ignore
    )
    return rows  # type: ignore


Connector = tuple[str, geometry.Point]


def split_street_segment(
    line_string: geometry.LineString, connectors: list[Connector]
) -> list[tuple[str, str, geometry.LineString]]:
    """ """
    # overture segments can span multiple intersections
    # sort through and split until pairings are ready for insertion to the graph
    node_segment_pairs: list[tuple[geometry.LineString, Connector, Connector]] = []
    node_segment_lots: list[tuple[geometry.LineString, list[Connector]]] = [(line_string, connectors)]
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
